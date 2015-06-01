use std::{fmt, thread};
use std::collections::{hash_map, HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::rc::Rc;

use mio::tcp::{TcpListener, TcpStream};
use mio::util::Slab;
use mio::{
    EventLoop,
    Handler,
    Interest,
    PollOpt,
    ReadHint,
    Token,
};
use rand::{self, Rng};
use capnp::{
    MallocMessageBuilder,
    OwnedSpaceMessageReader,
    ReaderOptions,
};
use capnp::serialize::{
    read_message_async,
    write_message_async,
    AsyncValue,
    ReadContinuation,
    WriteContinuation,
};

use replica::{Replica, Actions, Timeout};
use state_machine::StateMachine;
use store::Store;
use ClientId;
use Result;
use ServerId;

const LISTENER: Token = Token(0);

const ELECTION_MIN: u64 = 1500;
const ELECTION_MAX: u64 = 3000;
const HEARTBEAT_DURATION: u64 = 500;

/// The Raft Distributed Consensus Algorithm requires two RPC calls to be available:
///
///   * `append_entries` which is used as both a heartbeat (with no payload) and the primary
///     interface for requests.
///   * `request_vote` which is used by candidates during campaigns to obtain a vote.
///
/// A `Server` acts as a replicated state machine. The server's role in the cluster depends on it's
/// own status. It will maintain both volatile state (which can be safely lost) and persistent
/// state (which must be carefully stored and kept safe).
///
/// Currently, the `Server` API is not well defined. **We are looking for feedback and suggestions.**
pub struct Server<S, M> where S: Store, M: StateMachine {
    id: ServerId,
    peers: HashMap<ServerId, SocketAddr>,
    replica: Replica<S, M>,
    listener: TcpListener,
    connections: Slab<Connection>,
    peer_tokens: HashMap<ServerId, Token>,
    client_tokens: HashMap<ClientId, Token>,
}

/// The implementation of the Server.
impl<S, M> Server<S, M> where S: Store, M: StateMachine {

    /// Creates a new Raft node with the cluster members specified.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the new node.
    /// * `addr` - The address of the new node.
    /// * `peers` - The ID and address of all peers in the Raft cluster.
    /// * `store` - The persistent log store.
    /// * `state_machine` - The client state machine to which client commands will be applied.
    pub fn spawn(id: ServerId,
                 addr: SocketAddr,
                 peers: HashMap<ServerId, SocketAddr>,
                 store: S,
                 state_machine: M) {
        assert!(!peers.contains_key(&id));
        debug!("Spawning Server({})", id);
        let replica = Replica::new(id, peers.keys().cloned().collect(), store, state_machine);
        let mut event_loop = EventLoop::<Server<S, M>>::new().unwrap();
        let listener = TcpListener::bind(&addr).unwrap();
        event_loop.register(&listener, LISTENER).unwrap();

        thread::Builder::new().name(format!("raft::Server({})", id)).spawn(move || {
            let mut raft_node = Server {
                id: id,
                peers: peers,
                replica: replica,
                listener: listener,
                connections: Slab::new_starting_at(Token(1), 129),
                peer_tokens: HashMap::new(),
                client_tokens: HashMap::new(),
            };
            event_loop.run(&mut raft_node).unwrap();
        }).unwrap();
    }

    /// Finds an existing connection to a peer, or opens a new one if necessary.
    fn peer_connection(&mut self,
                       event_loop: &mut EventLoop<Server<S, M>>,
                       peer_id: ServerId)
                       -> Result<&mut Connection> {
        let token: Token = match self.peer_tokens.entry(peer_id) {
            hash_map::Entry::Occupied(entry) => *entry.get(),
            hash_map::Entry::Vacant(entry) => {
                let peer_addr = self.peers[&peer_id];
                let socket: TcpStream = try!(TcpStream::connect(&peer_addr));
                let token: Token = self.connections.insert(Connection::with_server_id(peer_id, socket)).unwrap();
                self.connections[token].token = token;
                try!(event_loop.register_opt(&self.connections[token].stream,
                                        token,
                                        Interest::readable(),
                                        poll_opt()));
                entry.insert(token);
                token
            },
        };
        Ok(&mut self.connections[token])
    }

    /// Finds an existing connection to a client.
    fn client_connection<'a>(&'a mut self, client_id: ClientId) -> Option<&'a mut Connection> {
        match self.client_tokens.get(&client_id) {
            Some(&token) => self.connections.get_mut(token),
            None => None
        }
    }

    fn execute_actions(&mut self,
                       event_loop: &mut EventLoop<Server<S, M>>,
                       actions: Actions) {
        unimplemented!()
    }
}

impl<S, M> Handler for Server<S, M> where S: Store, M: StateMachine {

    type Message = ();
    type Timeout = Timeout;

    fn writable(&mut self, reactor: &mut EventLoop<Server<S, M>>, token: Token) {
        debug!("{:?}: Writeable {:?}", self, token);
        match token {
            LISTENER => unreachable!(),
            tok => {
                self.connections[tok].writable(reactor).unwrap();
            }
        }
    }

    fn readable(&mut self, reactor: &mut EventLoop<Server<S, M>>, token: Token, _hint: ReadHint) {
        match token {
            LISTENER => {
                let stream = self.listener.accept().unwrap().unwrap();
                let addr = stream.peer_addr().unwrap();
                debug!("{:?}: new connection received from {}", self, &addr);
                let conn = Connection::new(stream);
                let token = match self.connections.insert(conn) {
                    Ok(token) => token,
                    Err(conn) => {
                        warn!("Unable to accept connection from {}: connection slab is full.",
                              conn.stream.peer_addr().unwrap());
                        return;
                    },
                };

                // Register the connection
                self.connections[token].token = token;
                reactor.register_opt(&self.connections[token].stream, token, Interest::readable(), poll_opt())
                       .unwrap();
            },
            token => {
                // Read messages from the socket until there are no more.
                while let Some(message) = self.connections[token].readable(reactor).unwrap() {
                    match self.connections[token].id {
                        ConnectionId::Server(id) => {
                            let actions = self.replica.apply_peer_message(id, &message);
                            self.execute_actions(reactor, actions);
                        },
                        ConnectionId::Client(id) => {
                            let actions = self.replica.apply_client_message(id, &message);
                            self.execute_actions(reactor, actions);
                        },
                        ConnectionId::Unknown => {
                            // TODO: parse preamble message and setup connection appropriately
                        }
                    }
                    let from = self.connections[token].id;
                }
            }
        }
    }

    fn timeout(&mut self, reactor: &mut EventLoop<Server<S, M>>, timeout: Timeout) {
        debug!("{:?}: Timeout", self);
        let actions = self.replica.apply_timeout(timeout);
        self.execute_actions(reactor, actions);
    }
}

impl <S, M> fmt::Debug for Server<S, M> where S: Store, M: StateMachine {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Server({})", self.listener.local_addr().unwrap())
    }
}

fn poll_opt() -> PollOpt {
    PollOpt::edge() | PollOpt::oneshot()
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
enum ConnectionId {
    Server(ServerId),
    Client(ClientId),
    Unknown,
}

struct Connection {
    stream: TcpStream,
    id: ConnectionId,
    token: Token,
    interest: Interest,
    read_continuation: Option<ReadContinuation>,
    write_continuation: Option<WriteContinuation>,
    write_queue: VecDeque<Rc<MallocMessageBuilder>>,
}

impl Connection {

    /// Creates a new `Connection` wrapping the provided socket.
    ///
    /// Note: the caller must manually call `set_token` after inserting the
    /// connection into a slab.
    fn new(socket: TcpStream) -> Connection {
        Connection {
            id: ConnectionId::Unknown,
            stream: socket,
            token: Token(0),
            interest: Interest::hup(),
            read_continuation: None,
            write_continuation: None,
            write_queue: VecDeque::new(),
        }
    }

    fn with_server_id(id: ServerId, socket: TcpStream) -> Connection {
        Connection {
            id: ConnectionId::Server(id),
            stream: socket,
            token: Token(0),
            interest: Interest::hup(),
            read_continuation: None,
            write_continuation: None,
            write_queue: VecDeque::new(),
        }
    }

    /// Writes queued messages to the socket.
    fn writable<S, M>(&mut self,
                      event_loop: &mut EventLoop<Server<S, M>>)
                      -> Result<()>
    where S: Store, M: StateMachine {
        debug!("{:?}: writable", self);

        while let Some(message) = self.write_queue.pop_front() {
            match try!(write_message_async(&mut self.stream, &*message, self.write_continuation.take())) {
                AsyncValue::Complete(()) => (),
                AsyncValue::Continue(continuation) =>  {
                    // the write only partially completed. Save the continuation and add the
                    // message back to the front of the queue.
                    self.write_continuation = Some(continuation);
                    self.write_queue.push_front(message);
                    break;
                }
            }
        }

        if self.write_queue.is_empty() {
            self.interest.remove(Interest::writable());
        }

        event_loop.reregister(&self.stream, self.token, self.interest, poll_opt())
                  .map_err(From::from)
    }

    /// Reads a message from the socket, or if a full message is not available,
    /// returns `None`.
    ///
    /// Because connections are registered as edge-triggered, the handler must
    /// continue calling this until no more messages are returned.
    fn readable<S, M>(&mut self,
                      event_loop: &mut EventLoop<Server<S, M>>)
                      -> Result<Option<OwnedSpaceMessageReader>>
    where S: Store, M: StateMachine {
        debug!("{:?}: readable", self);
        match try!(read_message_async(&mut self.stream, ReaderOptions::new(), self.read_continuation.take())) {
            AsyncValue::Complete(message) => {
                Ok(Some(message))
            },
            AsyncValue::Continue(continuation) => {
                // the read only partially completed. Save the continuation and return.
                self.read_continuation = Some(continuation);
                self.interest.insert(Interest::readable());
                try!(event_loop.reregister(&self.stream, self.token, self.interest, poll_opt()));
                Ok(None)
            },
        }
    }

    /// Queues a message to be sent to this connection.
    fn send_message<S, M>(&mut self,
                          event_loop: &mut EventLoop<Server<S, M>>,
                          message: Rc<MallocMessageBuilder>)
                          -> Result<()>
    where S: Store, M: StateMachine {
        debug!("{:?}: send_message", self);
        self.write_queue.push_back(message);
        if self.write_queue.is_empty() {
            self.interest.insert(Interest::writable());
            try!(event_loop.reregister(&self.stream, self.token, self.interest, poll_opt()));
        }
        Ok(())
    }

}

impl fmt::Debug for Connection {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Connection({})", self.stream.peer_addr().unwrap())
    }
}
