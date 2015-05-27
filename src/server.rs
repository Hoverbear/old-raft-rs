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
    MessageBuilder,
    MessageReader,
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

use messages_capnp::message;
use replica::{Replica, EmitType};
use state_machine::StateMachine;
use store::Store;
use super::Result;

const LISTENER: Token = Token(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeoutType {
    Election,
    Heartbeat,
}

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
    replica: Replica<S, M>,
    listener: TcpListener,
    peer_tokens: HashMap<SocketAddr, Token>,
    connections: Slab<Connection>,
}

/// The implementation of the Server. In most use cases, creating a `Server` should just be
/// done via `::new()`.
impl<S, M> Server<S, M> where S: Store, M: StateMachine {

    /// Creates a new Raft node with the cluster members specified.
    ///
    /// # Arguments
    ///
    /// * `addr` - The address of the new node.
    /// * `peers` - The address of all peers in the Raft cluster.
    /// * `store` - The persitent log store.
    /// * `state_machine` - The client state machine to which client commands will be applied.
    pub fn spawn(addr: SocketAddr,
                 peers: HashSet<SocketAddr>,
                 store: S,
                 state_machine: M) {
        debug!("Spawning Server");
        // Create an event loop
        let mut event_loop = EventLoop::<Server<S, M>>::new().unwrap();
        // Setup the socket, make it not block.
        let listener = TcpListener::bind(&addr).unwrap();
        event_loop.register(&listener, LISTENER).unwrap();
        let timeout = rand::thread_rng().gen_range::<u64>(ELECTION_MIN, ELECTION_MAX);
        event_loop.timeout_ms(TimeoutType::Election, timeout).unwrap();
        event_loop.timeout_ms(TimeoutType::Heartbeat, HEARTBEAT_DURATION).unwrap();
        let replica = Replica::new(addr, peers, store, state_machine);

        // Fire up the thread.
        thread::Builder::new().name(format!("Server {}", addr)).spawn(move || {
            let mut raft_node = Server {
                listener: listener,
                replica: replica,
                connections: Slab::new_starting_at(Token(3), 128),
                peer_tokens: HashMap::new(),
            };
            event_loop.run(&mut raft_node).unwrap();
        }).unwrap();
    }

    fn peer_connection<'a>(&'a mut self,
                           event_loop: &mut EventLoop<Server<S, M>>,
                           peer: SocketAddr)
                           -> Result<&'a mut Connection> {
        let token: Token = match self.peer_tokens.entry(peer) {
            hash_map::Entry::Occupied(entry) => *entry.get(),
            hash_map::Entry::Vacant(entry) => {
                let socket: TcpStream = TcpStream::connect(&peer).unwrap();
                let token: Token = self.connections.insert(Connection::new(socket)).unwrap();
                self.connections[token].token = token;
                event_loop.register_opt(&self.connections[token].stream,
                                        token,
                                        Interest::readable(),
                                        poll_opt()).unwrap();
                entry.insert(token);
                token
            },
        };
        Ok(&mut self.connections[token])
    }

    fn broadcast(&mut self,
                 event_loop: &mut EventLoop<Server<S, M>>,
                 message: MallocMessageBuilder)
                 -> Result<()> {
        let rc = Rc::new(message);
        let peers = self.replica.peers().clone();
        for peer in peers {
            let connection = try!(self.peer_connection(event_loop, peer));
            try!(connection.send_message(event_loop, rc.clone()));
        }
        Ok(())
    }
}

impl<S, M> Handler for Server<S, M> where S: Store, M: StateMachine {

    type Message = ();
    type Timeout = TimeoutType;

    /// A registered IoHandle has available writing space.
    fn writable(&mut self, reactor: &mut EventLoop<Server<S, M>>, token: Token) {
        debug!("{:?}: Writeable {:?}", self, token);
        match token {
            LISTENER => unreachable!(),
            tok => {
                self.connections[tok].writable(reactor).unwrap();
            }
        }
    }

    /// A registered IoHandle has available data to read
    fn readable(&mut self, reactor: &mut EventLoop<Server<S, M>>, token: Token, _hint: ReadHint) {
        debug!("{:?}: Readable {:?}", self, token);
        match token {
            LISTENER => {
                let stream = self.listener.accept().unwrap().unwrap();
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
                // Read messages from the socket until there are no more
                while let Some(incoming_message) = self.connections[token].readable(reactor).unwrap() {
                    let from = self.connections[token].stream.peer_addr().unwrap();
                    let mut outgoing_message_builder = MallocMessageBuilder::new_default();
                    let emit_type = {
                        let outgoing_message = outgoing_message_builder.init_root::<message::Builder>();
                        match incoming_message.get_root::<message::Reader>().unwrap().which().unwrap() {
                            message::Which::AppendEntriesRequest(Ok(request)) => {
                                let response = outgoing_message.init_append_entries_response();
                                self.replica.append_entries_request(from, request, response)
                            },
                            message::Which::AppendEntriesResponse(Ok(response)) => {
                                let request = outgoing_message.init_append_entries_request();
                                self.replica.append_entries_response(from, response, request)
                            },
                            message::Which::RequestVoteRequest(Ok(request)) => {
                                let response = outgoing_message.init_request_vote_response();
                                self.replica.request_vote_request(from, request, response)
                            },
                            message::Which::RequestVoteResponse(Ok(response)) => {
                                let request = outgoing_message.init_append_entries_request();
                                self.replica.request_vote_response(from, response, request)
                            },
                            message::Which::ClientAppendRequest(Ok(request)) => {
                                let response = outgoing_message.init_client_append_response();
                                self.replica.client_append_request(from, request, response)
                            },
                            _ => panic!("cannot handle message"),
                        }
                    };

                    match emit_type {
                        EmitType::None => (),
                        EmitType::Reply => {
                            self.connections[token].send_message(reactor, Rc::new(outgoing_message_builder)).unwrap()
                        },
                        EmitType::Broadcast => {
                            self.broadcast(reactor, outgoing_message_builder).unwrap()
                        },
                    }
                }
            }
        }
    }

    /// A registered timer has expired. This is either:
    ///
    /// * An election timeout, when a `Follower` node has waited too long for a heartbeat and doing
    /// to become a `Candidate`.
    /// * A heartbeat timeout, when the `Leader` node needs to refresh it's authority over the
    /// followers. Initializes and sends an `AppendEntries` request to all followers.
    fn timeout(&mut self, reactor: &mut EventLoop<Server<S, M>>, timeout: TimeoutType) {
        debug!("{:?}: Timeout", self);
        let mut message_builder = MallocMessageBuilder::new_default();
        let emit_type = {
            let message = message_builder.init_root::<message::Builder>();
            match timeout {
                TimeoutType::Election => {
                    let timeout = rand::thread_rng().gen_range::<u64>(ELECTION_MIN, ELECTION_MAX);
                    reactor.timeout_ms(TimeoutType::Election, timeout).unwrap();
                    self.replica.election_timeout(message.init_request_vote_request())
                },
                TimeoutType::Heartbeat => {
                    reactor.timeout_ms(TimeoutType::Heartbeat, HEARTBEAT_DURATION).unwrap();
                    self.replica.heartbeat_timeout(message.init_append_entries_request())
                },
            }
        };
        debug!("{:?}: emit_type: {:?}", self, emit_type);
        // Send if necessary.
        match emit_type {
            EmitType::None => (),
            EmitType::Broadcast => {
                self.broadcast(reactor, message_builder).unwrap();
            },
            _ => unreachable!(),
        }
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

struct Connection {
    stream: TcpStream,
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
            stream: socket,
            token: Token(0), // Effectively a `null`. This needs to be assigned by the caller.
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
