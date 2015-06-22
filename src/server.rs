use std::fmt;
use std::collections::{hash_map, HashMap, VecDeque};
use std::net::SocketAddr;
use std::rc::Rc;
use std::thread::{self, JoinHandle};

use mio::tcp::{TcpListener, TcpStream};
use mio::util::Slab;
use mio::{
    EventLoop,
    Handler,
    Interest,
    PollOpt,
    Token,
};
use mio::Timeout as TimeoutHandle;
use rand::{self, Rng};
use capnp::{
    MallocMessageBuilder,
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

use ClientId;
use Result;
use ServerId;
use messages;
use messages_capnp::connection_preamble;
use replica::{Replica, Actions, Timeout};
use state_machine::StateMachine;
use store::Store;

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
    timeouts: HashMap<Timeout, TimeoutHandle>,
}

/// The implementation of the Server.
impl<S, M> Server<S, M> where S: Store, M: StateMachine {

    fn new(id: ServerId,
           addr: SocketAddr,
           peers: HashMap<ServerId, SocketAddr>,
           store: S,
           state_machine: M) -> Result<(Server<S, M>, EventLoop<Server<S, M>>)> {
        assert!(!peers.contains_key(&id));
        let replica = Replica::new(id, peers.keys().cloned().collect(), store, state_machine);
        let mut event_loop = try!(EventLoop::<Server<S, M>>::new());
        let listener = try!(TcpListener::bind(&addr));
        try!(event_loop.register(&listener, LISTENER));
        let server = Server {
            id: id,
            peers: peers,
            replica: replica,
            listener: listener,
            connections: Slab::new_starting_at(Token(1), 129),
            peer_tokens: HashMap::new(),
            client_tokens: HashMap::new(),
            timeouts: HashMap::new(),
        };
        Ok((server, event_loop))
    }

    /// Runs a new Raft server in the current thread.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the new node.
    /// * `addr` - The address of the new node.
    /// * `peers` - The ID and address of all peers in the Raft cluster.
    /// * `store` - The persistent log store.
    /// * `state_machine` - The client state machine to which client commands will be applied.
    pub fn run(id: ServerId,
               addr: SocketAddr,
               peers: HashMap<ServerId, SocketAddr>,
               store: S,
               state_machine: M) -> Result<()> {
        let (mut server, mut event_loop) = try!(Server::new(id, addr, peers, store, state_machine));
        let actions = server.replica.init();
        server.execute_actions(&mut event_loop, actions);
        event_loop.run(&mut server).map_err(From::from)
    }

    /// Spawns a new Raft server in a background thread.
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
                 state_machine: M) -> Result<JoinHandle<Result<()>>> {
        thread::Builder::new().name(format!("raft::Server({})", id)).spawn(move || {
            Server::run(id, addr, peers, store, state_machine)
        }).map_err(From::from)
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
                let socket: TcpStream =
                    try_warn!(TcpStream::connect(&peer_addr),
                              "Server({}): unable to connect to peer Server {{ id: {}, address: {} }}: {}",
                              self.id, peer_id, peer_addr);
                let token: Token = self.connections.insert(Connection::peer(peer_id, peer_addr, socket)).unwrap();
                self.connections[token].token = token;
                try_warn!(event_loop.register_opt(&self.connections[token].stream,
                                                  token, self.connections[token].interest, poll_opt()),
                          "Server({}): unable to register connection with event loop: {}",
                          self.id);
                entry.insert(token);
                self.connections[token].write_queue.push_back(messages::server_connection_preamble(self.id));
                info!("opening new connection to {:?}", self.connections[token]);
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
        let Actions { peer_messages, client_messages, timeouts, clear_timeouts } = actions;

        for (peer, message) in peer_messages {
            debug!("sending message to peer: {:?}", peer);
            let _ = self.peer_connection(event_loop, peer)
                        .and_then(|connection| connection.send_message(event_loop, message));
        }
        for (client, message) in client_messages {
            if let Some(connection) = self.client_connection(client) {
                let _ = connection.send_message(event_loop, message);
            }
        }
        if clear_timeouts {
            self.clear_timeouts(event_loop);
        }
        for timeout in timeouts {
            self.register_timeout(event_loop, timeout);
        }
    }

    /// Registers a new timeout.
    fn register_timeout(&mut self,
                        event_loop: &mut EventLoop<Server<S, M>>,
                        timeout: Timeout) {
        let ms = match timeout {
            Timeout::Election(..) => rand::thread_rng().gen_range::<u64>(ELECTION_MIN, ELECTION_MAX),
            Timeout::Heartbeat(..) => HEARTBEAT_DURATION,
        };

        // Registering a timeout may only fail if the maximum number of timeouts
        // is already registered, which is by default 65,536. We (should) use a
        // maximum of one timeout per peer, so this unwrap should be safe.
        let handle = event_loop.timeout_ms(timeout, ms).unwrap();
        self.timeouts.insert(timeout, handle)
            .map(|handle| event_loop.clear_timeout(handle));
    }

    /// Clears all registered timeouts.
    fn clear_timeouts(&mut self, event_loop: &mut EventLoop<Server<S, M>>) {
        for (timeout, &handle) in self.timeouts.iter() {
            assert!(event_loop.clear_timeout(handle), "unable to clear timeout: {:?}", timeout);
        }
        self.timeouts.clear();
    }

    /// Closes the connection corresponding to the token.
    fn close_connection(&mut self, token: Token) {
        let connection = self.connections.remove(token).unwrap();
        match connection.id {
            ConnectionType::Peer(ref id) => {
                self.peer_tokens.remove(id);
            },
            ConnectionType::Client(ref id) => {
                self.client_tokens.remove(id);
            },
            _ => (),
        }
    }
}

impl<S, M> Handler for Server<S, M> where S: Store, M: StateMachine {

    type Message = ();
    type Timeout = Timeout;

    fn ready(&mut self, reactor: &mut EventLoop<Server<S, M>>, token: Token, events: Interest) {

        if events.is_error() {
            assert!(token != LISTENER, "Unexpected error event from LISTENER");
            warn!("error event on connection {:?}", self.connections[token]);
            self.close_connection(token);
            return;
        }

        if events.is_hup() {
            assert!(token != LISTENER, "Unexpected hup event from LISTENER");
            info!("hup event on connection {:?}", self.connections[token]);
            self.close_connection(token);
            return;
        }

        if events.is_writable() {
            assert!(token != LISTENER, "Unexpected writeable event for LISTENER");
            debug!("{:?}: connection writable: {:?}", self, self.connections[token]);
            self.connections[token].writable(reactor).unwrap();
        }

        if events.is_readable() {
            if token == LISTENER {
                let stream = self.listener.accept().unwrap().unwrap();
                let addr = stream.peer_addr().unwrap();
                debug!("{:?}: new connection received from {}", self, &addr);
                let conn = Connection::unknown(stream);
                let token = match self.connections.insert(conn) {
                    Ok(token) => token,
                    Err(conn) => {
                        warn!("Unable to accept connection from {}: connection slab is full.",
                              conn.stream.peer_addr().unwrap());
                        return;
                    },
                };
                // Register the connection.
                self.connections[token].token = token;
                reactor.register_opt(&self.connections[token].stream, token,
                                     self.connections[token].interest, poll_opt())
                       .unwrap();
            } else {
                debug!("{:?}: connection readable: {:?}, events: {:?}", self, self.connections[token], events);
                // Read messages from the socket until there are no more.
                while let Some(message) = self.connections[token].readable(reactor).unwrap() {
                    match self.connections[token].id {
                        ConnectionType::Peer(id) => {
                            let actions = self.replica.apply_peer_message(id, &message);
                            self.execute_actions(reactor, actions);
                        },
                        ConnectionType::Client(id) => {
                            let actions = self.replica.apply_client_message(id, &message);
                            self.execute_actions(reactor, actions);
                        },
                        ConnectionType::Unknown => {
                            let preamble = message.get_root::<connection_preamble::Reader>().unwrap();
                            match preamble.get_id().which().unwrap() {
                                connection_preamble::id::Which::Server(id) => {
                                    self.connections[token].id = ConnectionType::Peer(ServerId::from(id));
                                    assert!(self.peer_tokens.get(&ServerId(id)).is_none(), "connection already exists to {:?}", self.connections[token]);
                                    self.peer_tokens.insert(ServerId(id), token);
                                },
                                connection_preamble::id::Which::Client(Ok(id)) => {
                                    self.connections[token].id = ConnectionType::Client(ClientId::from_bytes(id).unwrap());
                                },
                                _ => {
                                    // TODO: drop the connection
                                    unimplemented!()
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn timeout(&mut self, reactor: &mut EventLoop<Server<S, M>>, timeout: Timeout) {
        debug!("{:?}: Timeout: {:?}", self, &timeout);
        let actions = self.replica.apply_timeout(timeout);
        self.execute_actions(reactor, actions);
    }
}

impl <S, M> fmt::Debug for Server<S, M> where S: Store, M: StateMachine {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Server({})", self.id)
    }
}

fn poll_opt() -> PollOpt {
    PollOpt::edge() | PollOpt::oneshot()
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
enum ConnectionType {
    Peer(ServerId),
    Client(ClientId),
    Unknown,
}

struct Connection {
    stream: TcpStream,
    id: ConnectionType,
    address: SocketAddr,
    token: Token,
    interest: Interest,
    read_continuation: Option<ReadContinuation>,
    write_continuation: Option<WriteContinuation>,
    write_queue: VecDeque<Rc<MallocMessageBuilder>>,
}

impl Connection {

    /// Creates a new `Connection` wrapping the provided socket.
    ///
    /// The socket must already be connected.
    ///
    /// Note: the caller must manually call `set_token` after inserting the
    /// connection into a slab.
    fn unknown(socket: TcpStream) -> Connection {
        let address = socket.peer_addr().unwrap();
        Connection {
            stream: socket,
            id: ConnectionType::Unknown,
            address: address,
            token: Token(0),
            interest: Interest::hup() | Interest::readable(),
            read_continuation: None,
            write_continuation: None,
            write_queue: VecDeque::new(),
        }
    }

    fn peer(id: ServerId, address: SocketAddr, socket: TcpStream) -> Connection {
        Connection {
            stream: socket,
            id: ConnectionType::Peer(id),
            address: address,
            token: Token(0),
            interest: Interest::hup() | Interest::readable() | Interest::writable(),
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
        debug!("{:?}: writable; queued messages: {}", self, self.write_queue.len());

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
        if self.write_queue.is_empty() {
            self.interest.insert(Interest::writable());
            try_warn!(event_loop.reregister(&self.stream, self.token, self.interest, poll_opt()),
                      "{:?}: unable to register with event loop: {}", self);
        }
        self.write_queue.push_back(message);
        Ok(())
    }

}

impl fmt::Debug for Connection {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self.id {
            ConnectionType::Peer(id) => {
                write!(fmt, "PeerConnection {{ id: {}, address: {} }}", id, self.address)
            },
            ConnectionType::Client(id) => {
                write!(fmt, "ClientConnection {{ id: {}, address: {} }}", id, self.address)
            },
            ConnectionType::Unknown => {
                write!(fmt, "UnknownConnection {{ address: {} }}", self.address)
            },
        }
    }
}

#[cfg(test)]
mod test {

    extern crate env_logger;

    use std::collections::HashMap;
    use std::net::{TcpListener, SocketAddr};
    use std::rc::Rc;
    use std::str::FromStr;

    use ServerId;
    use messages;
    use replica::Actions;
    use state_machine::NullStateMachine;
    use store::MemStore;
    use super::*;

    use mio::EventLoop;

    type TestServer = Server<MemStore, NullStateMachine>;

    fn new_test_server() -> (TestServer, EventLoop<TestServer>) {
        Server::new(ServerId::from(0),
                    SocketAddr::from_str("127.0.0.1:0").unwrap(),
                    HashMap::new(),
                    MemStore::new(),
                    NullStateMachine)
              .unwrap()
    }

    /// Attempts to grab a local, unbound socket address for testing.
    fn get_unbound_address() -> SocketAddr {
        TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
    }

    #[test]
    pub fn test_illegal_peer_address() {
        let _ = env_logger::init();
        let (mut server, mut event_loop) = new_test_server();
        let peer_id = ServerId::from(1);
        let peer_addr = SocketAddr::from_str("127.0.0.1:0").unwrap();
        server.peers.insert(peer_id, peer_addr);
        let actions = Actions::with_peer_message(peer_id, Rc::new(messages::ping_request()));
        server.execute_actions(&mut event_loop, actions);
        event_loop.run_once(&mut server).unwrap();
        assert!(server.peer_tokens.is_empty());
    }

    #[test]
    pub fn test_unbound_peer_address() {
        let _ = env_logger::init();
        let (mut server, mut event_loop) = new_test_server();
        let peer_id = ServerId::from(1);
        let peer_addr = get_unbound_address();
        server.peers.insert(peer_id, peer_addr);
        let actions = Actions::with_peer_message(peer_id, Rc::new(messages::ping_request()));
        server.execute_actions(&mut event_loop, actions);
        event_loop.run_once(&mut server).unwrap();
        assert!(server.peer_tokens.is_empty());
    }



}
