use std::{fmt, io};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::thread::{self, JoinHandle};

use mio::tcp::TcpListener;
use mio::util::Slab;
use mio::{
    EventLoop,
    EventSet,
    Handler,
    Token,
};
use mio::Timeout as TimeoutHandle;
use capnp::{
    MessageReader,
};

use ClientId;
use Result;
use Error;
use ErrorKind;
use ServerId;
use messages;
use messages_capnp::connection_preamble;
use replica::{Replica, Actions, ReplicaTimeout};
use state_machine::StateMachine;
use store::Store;
use connection::{Connection, ConnectionKind};

const LISTENER: Token = Token(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ServerTimeout {
    Replica(ReplicaTimeout),
    Reconnect(Token),
}

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

    /// Id of this server.
    id: ServerId,

    /// Raft state machine replica.
    replica: Replica<S, M>,

    /// Connection listener.
    listener: TcpListener,

    /// Collection of connections indexed by token.
    connections: Slab<Connection>,

    /// Index of peer id to connection token.
    peer_tokens: HashMap<ServerId, Token>,

    /// Index of client id to connection token.
    client_tokens: HashMap<ClientId, Token>,

    /// Currently registered replica timeouts.
    replica_timeouts: HashMap<ReplicaTimeout, TimeoutHandle>,

    /// Currently registered reconnection timeouts.
    reconnection_timeouts: HashMap<Token, TimeoutHandle>,
}

/// The implementation of the Server.
impl<S, M> Server<S, M> where S: Store, M: StateMachine {

    fn new(id: ServerId,
           addr: SocketAddr,
           peers: HashMap<ServerId, SocketAddr>,
           store: S,
           state_machine: M) -> Result<(Server<S, M>, EventLoop<Server<S, M>>)> {
        assert!(!peers.contains_key(&id), "peer set must not contain the local server");
        let replica = Replica::new(id, peers.keys().cloned().collect(), store, state_machine);
        let mut event_loop = try!(EventLoop::<Server<S, M>>::new());
        let listener = try!(TcpListener::bind(&addr));
        try!(event_loop.register(&listener, LISTENER));

        let mut server = Server {
            id: id,
            replica: replica,
            listener: listener,
            connections: Slab::new_starting_at(Token(1), 129),
            peer_tokens: HashMap::new(),
            client_tokens: HashMap::new(),
            replica_timeouts: HashMap::new(),
            reconnection_timeouts: HashMap::new(),
        };

        for (peer_id, peer_addr) in peers {
            let token: Token = try!(server.connections
                                          .insert(try!(Connection::peer(peer_id, peer_addr)))
                                          .map_err(|_| Error::Raft(ErrorKind::ConnectionLimitReached)));
            assert!(server.peer_tokens.insert(peer_id, token).is_none());

            let mut connection = &mut server.connections[token];
            connection.send_message(messages::server_connection_preamble(id));
            try!(connection.register(&mut event_loop, token));
        }

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

    /// Returns the connection to the peer.
    fn peer_connection(&mut self, peer_id: &ServerId) -> &mut Connection {
       let token = self.peer_tokens[peer_id];
       &mut self.connections[token]
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
        debug!("{:?}: executing actions: {:?}", self, actions);
        let Actions { peer_messages, client_messages, timeouts, clear_timeouts } = actions;

        for (peer, message) in peer_messages {
            self.peer_connection(&peer).send_message(message);
        }
        for (client, message) in client_messages {
            if let Some(connection) = self.client_connection(client) {
                connection.send_message(message);
            }
        }
        if clear_timeouts {
            for (timeout, &handle) in &self.replica_timeouts {
                assert!(event_loop.clear_timeout(handle),
                        "raft::{:?}: unable to clear timeout: {:?}", self, timeout);
            }
            self.replica_timeouts.clear();
        }
        for timeout in timeouts {
            let duration = timeout.duration_ms();

            // Registering a timeout may only fail if the maximum number of timeouts
            // is already registered, which is by default 65,536. We use a
            // maximum of one timeout per peer, so this unwrap should be safe.
            let handle = event_loop.timeout_ms(ServerTimeout::Replica(timeout), duration)
                                   .unwrap();
            self.replica_timeouts
                .insert(timeout, handle)
                .map(|handle| assert!(event_loop.clear_timeout(handle),
                                      "raft::{:?}: unable to clear timeout: {:?}", self, timeout));
        }
    }

    /// Resets the connection corresponding to the provided token.
    ///
    /// If the connection is to a peer, the server will attempt to reconnect after a waiting
    /// period.
    ///
    /// If the connection is to a client or unknown it will be closed.
    fn reset_connection(&mut self, event_loop: &mut EventLoop<Server<S, M>>, token: Token) {
        let kind = *self.connections[token].kind();
        match kind {
            ConnectionKind::Peer(..) => {
                // Crash if reseting the connection fails.
                let (timeout, handle) = self.connections[token]
                                            .reset_peer(event_loop, token)
                                            .unwrap();

                assert!(self.reconnection_timeouts.insert(token, handle).is_none(),
                        "raft::{:?}: timeout already registered: {:?}", self, timeout);
            },
            ConnectionKind::Client(ref id) => {
                self.connections.remove(token).expect("unable to find client connection");
                assert!(self.client_tokens.remove(id).is_some(),
                        "{:?}: client {:?} not connected");
            },
            ConnectionKind::Unknown => {
                self.connections.remove(token).expect("unable to find unknown connection");
            },
        }
    }

    /// Reads messages from the connection until no more are available.
    ///
    /// If the connection returns an error on any operation, or any message fails to be
    /// deserialized, an error result is returned.
    fn readable(&mut self, event_loop: &mut EventLoop<Server<S, M>>, token: Token) -> Result<()> {
        trace!("{:?}: connection readable: {:?}", self, self.connections[token]);
        // Read messages from the connection until there are no more.
        while let Some(message) = try!(self.connections[token].readable()) {
            match *self.connections[token].kind() {
                ConnectionKind::Peer(id) => {
                    let mut actions = Actions::new();
                    self.replica.apply_peer_message(id, &message, &mut actions);
                    self.execute_actions(event_loop, actions);
                },
                ConnectionKind::Client(id) => {
                    let mut actions = Actions::new();
                    self.replica.apply_client_message(id, &message, &mut actions);
                    self.execute_actions(event_loop, actions);
                },
                ConnectionKind::Unknown => {
                    let preamble = try!(message.get_root::<connection_preamble::Reader>());
                    match try!(preamble.get_id().which()) {
                        connection_preamble::id::Which::Server(id) => {
                            let peer_id = ServerId(id);
                            debug!("{:?}: received new connection from {:?}", self, peer_id);

                            self.connections[token].set_kind(ConnectionKind::Peer(peer_id));
                            let prev_token = self.peer_tokens
                                                 .insert(peer_id, token)
                                                 .expect("peer token not found");

                            // Close the existing connection.
                            self.connections
                                .remove(prev_token)
                                .expect("peer connection not found");

                            // Clear any timeouts associated with the existing connection.
                            self.reconnection_timeouts
                                .remove(&prev_token)
                                .map(|handle| assert!(event_loop.clear_timeout(handle)));

                            // TODO: add reconnect messages from replica
                        },
                        connection_preamble::id::Which::Client(Ok(id)) => {
                            let client_id = try!(ClientId::from_bytes(id));
                            debug!("{:?}: received new connection from {:?}", self, client_id);
                            self.connections[token]
                                .set_kind(ConnectionKind::Client(client_id));
                            let prev_token = self.client_tokens
                                                 .insert(client_id, token);
                            assert!(prev_token.is_none(),
                                    "{:?}: two clients connected with the same id: {:?}",
                                    self, client_id);
                        },
                        _ => {
                            return Err(Error::Raft(ErrorKind::UnknownConnectionType));
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl<S, M> Handler for Server<S, M> where S: Store, M: StateMachine {

    type Message = ();
    type Timeout = ServerTimeout;

    fn ready(&mut self, event_loop: &mut EventLoop<Server<S, M>>, token: Token, events: EventSet) {
        trace!("{:?}: ready; token: {:?}; events: {:?}", self, token, events);

        if events.is_error() {
            assert!(token != LISTENER, "raft::{:?}: unexpected error event from LISTENER", self);
            warn!("{:?}: error event on connection {:?}", self, self.connections[token]);
            self.reset_connection(event_loop, token);
            return;
        }

        if events.is_hup() {
            assert!(token != LISTENER, "raft::{:?}: unexpected hup event from LISTENER", self);
            trace!("{:?}: hup event on connection {:?}", self, self.connections[token]);
            self.reset_connection(event_loop, token);
            return;
        }

        if events.is_writable() {
            assert!(token != LISTENER, "raft::{:?}: unexpected writeable event for LISTENER", self);
            if let Err(error) = self.connections[token].writable() {
                warn!("{:?}: unable to write message to conection {:?}: {}",
                      self, self.connections[token], error);
                self.reset_connection(event_loop, token);
                return;
            }
        }

        if events.is_readable() {
            if token == LISTENER {
                self.listener
                    .accept().map_err(From::from)
                    .and_then(|stream_opt| {
                        match stream_opt {
                            Some(stream) => Connection::unknown(stream),
                            None => Err(Error::Io(io::Error::new(
                                        io::ErrorKind::WouldBlock,
                                        "listener.accept() returned None"))),
                        }
                    })
                    .and_then(|connection| {
                        debug!("{:?}: new connection received: {:?}", self, connection);
                        self.connections
                            .insert(connection)
                            .map_err(|_| Error::Raft(ErrorKind::ConnectionLimitReached))
                    })
                    .and_then(|token| self.connections[token].register(event_loop, token))
                    .unwrap_or_else(|error| warn!("{:?}: unable to accept connection: {}",
                                                  self, error));
            } else {
                self.readable(event_loop, token)
                    // Only reregister the connection with the event loop if no
                    // error occurs and the connection is *not* reset.
                    .and_then(|_| self.connections[token].reregister(event_loop, token))
                    .unwrap_or_else(|error| {
                        warn!("{:?}: unable to read message from connection {:?}: {}",
                              self, self.connections[token], error);
                        self.reset_connection(event_loop, token);
                    });
            }
        }
    }

    fn timeout(&mut self, event_loop: &mut EventLoop<Server<S, M>>, timeout: ServerTimeout) {
        trace!("{:?}: timeout: {:?}", self, &timeout);
        match timeout {
            ServerTimeout::Replica(replica) => {
                assert!(self.replica_timeouts.remove(&replica).is_some(),
                        "raft::{:?}: missing timeout: {:?}", self, timeout);
                let mut actions = Actions::new();
                self.replica.apply_timeout(replica, &mut actions);
                self.execute_actions(event_loop, actions);
            },

            ServerTimeout::Reconnect(token) => {
                assert!(self.reconnection_timeouts.remove(&token).is_some(),
                        "raft::{:?}: missing timeout: {:?}", self, timeout);
                self.connections[token]
                    .reconnect_peer(self.id)
                    .and_then(|_| self.connections[token].reregister(event_loop, token))
                    .unwrap_or_else(|error| {
                        warn!("{:?}: unable to reconnect connection {:?}: {}",
                              self, &self.connections[token], error);
                        self.reset_connection(event_loop, token);
                    });
                // TODO: add reconnect messages from replica
            },
        }
    }
}

impl <S, M> fmt::Debug for Server<S, M> where S: Store, M: StateMachine {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Server({})", self.id)
    }
}

#[cfg(test)]
mod test {

    extern crate env_logger;

    use std::collections::HashMap;
    use std::net::{TcpListener, TcpStream, SocketAddr};
    use std::str::FromStr;
    use std::io::{Read, Write};

    use mio::EventLoop;
    use capnp::{serialize, MessageReader, ReaderOptions};

    use ClientId;
    use Result;
    use ServerId;
    use messages;
    use messages_capnp::connection_preamble;
    use state_machine::NullStateMachine;
    use store::MemStore;
    use super::*;

    type TestServer = Server<MemStore, NullStateMachine>;

    fn new_test_server(peers: HashMap<ServerId, SocketAddr>)
                       -> Result<(TestServer, EventLoop<TestServer>)> {
        Server::new(ServerId::from(0),
                    SocketAddr::from_str("127.0.0.1:0").unwrap(),
                    peers,
                    MemStore::new(),
                    NullStateMachine)
    }

    /// Attempts to grab a local, unbound socket address for testing.
    fn get_unbound_address() -> SocketAddr {
        TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
    }

    /// Verifies that the proved stream has been sent a valid connection
    /// preamble.
    fn read_server_preamble<R>(read: &mut R) -> ServerId where R: Read {
        let message = serialize::read_message(read, ReaderOptions::new()).unwrap();
        let preamble = message.get_root::<connection_preamble::Reader>().unwrap();

        match preamble.get_id().which().unwrap() {
            connection_preamble::id::Which::Server(id) => {
                ServerId::from(id)
            },
            _ => {
                panic!("unexpected preamble id");
            }
        }
    }

    /// Returns true if the server has an open connection with the peer.
    fn peer_connected(server: &TestServer, peer: ServerId) -> bool {
        let token = server.peer_tokens[&peer];
        server.reconnection_timeouts.get(&token).is_none()
    }

    fn client_connected(server: &TestServer, client: ClientId) -> bool {
        server.client_tokens.contains_key(&client)
    }

    /// Returns true if the provided TCP connection has been shutdown.
    ///
    /// TODO: figure out a more robust way to implement this, the current check
    /// will block the thread indefinitely if the stream is not shutdown.
    fn stream_shutdown(stream: &mut TcpStream) -> bool {
        let mut buf = [0u8; 128];
        stream.read(&mut buf).unwrap() == 0
    }

    /// Tests that a Server will reject an invalid peer address on creation.
    #[test]
    fn test_illegal_peer_address() {
        let _ = env_logger::init();
        let peer_id = ServerId::from(1);
        let mut peers = HashMap::new();
        peers.insert(peer_id, SocketAddr::from_str("127.0.0.1:0").unwrap());
        assert!(new_test_server(peers).is_err());
    }

    /// Tests that a Server connects to peer at startup, and reconnects when the
    /// connection is droped.
    #[test]
    fn test_peer_connect() {
        let _ = env_logger::init();
        let peer_id = ServerId::from(1);

        let peer_listener = TcpListener::bind("127.0.0.1:0").unwrap();

        let mut peers = HashMap::new();
        peers.insert(peer_id, peer_listener.local_addr().unwrap());
        let (mut server, mut event_loop) = new_test_server(peers).unwrap();

        // Accept the server's connection.
        let (mut stream, _)  = peer_listener.accept().unwrap();

        // Check that the server sends a valid preamble.
        event_loop.run_once(&mut server).unwrap();
        assert_eq!(ServerId::from(0), read_server_preamble(&mut stream));
        assert!(peer_connected(&server, peer_id));

        // Drop the connection.
        drop(stream);
        event_loop.run_once(&mut server).unwrap();
        assert!(!peer_connected(&server, peer_id));

        // Check that the server reconnects after a timeout.
        event_loop.run_once(&mut server).unwrap();
        assert!(peer_connected(&server, peer_id));
        let (mut stream, _)  = peer_listener.accept().unwrap();

        // Check that the server sends a valid preamble after the connection is
        // established.
        event_loop.run_once(&mut server).unwrap();
        assert_eq!(ServerId::from(0), read_server_preamble(&mut stream));
        assert!(peer_connected(&server, peer_id));
    }

    /// Tests that a Server will replace a peer's TCP connection if the peer
    /// connects through another TCP connection.
    #[test]
    fn test_peer_accept() {
        let _ = env_logger::init();
        let peer_id = ServerId::from(1);

        let peer_listener = TcpListener::bind("127.0.0.1:0").unwrap();

        let mut peers = HashMap::new();
        peers.insert(peer_id, peer_listener.local_addr().unwrap());
        let (mut server, mut event_loop) = new_test_server(peers).unwrap();

        // Accept the server's connection.
        let (mut in_stream, _)  = peer_listener.accept().unwrap();

        // Check that the server sends a valid preamble.
        event_loop.run_once(&mut server).unwrap();
        assert_eq!(ServerId::from(0), read_server_preamble(&mut in_stream));
        assert!(peer_connected(&server, peer_id));

        let server_addr = server.listener.local_addr().unwrap();

        // Open a replacement connection to the server.
        let mut out_stream = TcpStream::connect(server_addr).unwrap();
        event_loop.run_once(&mut server).unwrap();

        // Send server the preamble message to the server.
        serialize::write_message(&mut out_stream, &*messages::server_connection_preamble(peer_id))
                 .unwrap();
        event_loop.run_once(&mut server).unwrap();

        // Check that the server has closed the old connection.
        assert!(stream_shutdown(&mut in_stream));
    }

    /// Tests that the server will accept a client connection, then dispose of
    /// it when the client disconnects.
    #[test]
    fn test_client_accept() {
        let _ = env_logger::init();

        let (mut server, mut event_loop) = new_test_server(HashMap::new()).unwrap();

        // Connect to the server.
        let server_addr = server.listener.local_addr().unwrap();
        let mut stream = TcpStream::connect(server_addr).unwrap();
        event_loop.run_once(&mut server).unwrap();

        let client_id = ClientId::new();

        // Send the client preamble message to the server.
        serialize::write_message(&mut stream, &*messages::client_connection_preamble(client_id))
                 .unwrap();
        event_loop.run_once(&mut server).unwrap();

        // Check that the server holds on to the client connection.
        assert!(client_connected(&server, client_id));

        // Check that the server disposes of the client connection when the TCP
        // stream is dropped.
        drop(stream);
        event_loop.run_once(&mut server).unwrap();
        assert!(!client_connected(&server, client_id));
    }

    /// Tests that the server will throw away connections that do not properly
    /// send a preamble.
    #[test]
    fn test_invalid_accept() {
        let _ = env_logger::init();

        let (mut server, mut event_loop) = new_test_server(HashMap::new()).unwrap();

        // Connect to the server.
        let server_addr = server.listener.local_addr().unwrap();
        let mut stream = TcpStream::connect(server_addr).unwrap();
        event_loop.run_once(&mut server).unwrap();

        // Send an invalid preamble.
        stream.write(b"foo bar baz").unwrap();
        event_loop.run_once(&mut server).unwrap();

        // Check that the server disposes of the connection.
        assert!(stream_shutdown(&mut stream));
    }

    /// Tests that the server will reset a peer connection when an invalid
    /// message is received.
    #[test]
    fn test_invalid_peer_message() {
        let _ = env_logger::init();

        let peer_id = ServerId::from(1);

        let peer_listener = TcpListener::bind("127.0.0.1:0").unwrap();

        let mut peers = HashMap::new();
        peers.insert(peer_id, peer_listener.local_addr().unwrap());
        let (mut server, mut event_loop) = new_test_server(peers).unwrap();

        // Accept the server's connection.
        let (mut stream_a, _)  = peer_listener.accept().unwrap();

        // Send an invalid message.
        stream_a.write(b"foo bar baz").unwrap();
        event_loop.run_once(&mut server).unwrap();

        // Check that the server resets the connection.
        assert!(!peer_connected(&server, peer_id));

        // Check that the server reconnects after a timeout.
        event_loop.run_once(&mut server).unwrap();
        assert!(peer_connected(&server, peer_id));
    }

    /// Tests that the server will reset a client connection when an invalid
    /// message is received.
    #[test]
    fn test_invalid_client_message() {
        let _ = env_logger::init();

        let (mut server, mut event_loop) = new_test_server(HashMap::new()).unwrap();

        // Connect to the server.
        let server_addr = server.listener.local_addr().unwrap();
        let mut stream = TcpStream::connect(server_addr).unwrap();
        event_loop.run_once(&mut server).unwrap();

        let client_id = ClientId::new();

        // Send the client preamble message to the server.
        serialize::write_message(&mut stream, &*messages::client_connection_preamble(client_id))
                 .unwrap();
        event_loop.run_once(&mut server).unwrap();

        // Check that the server holds on to the client connection.
        assert!(client_connected(&server, client_id));

        // Send an invalid client message to the server.
        stream.write(b"foo bar baz").unwrap();
        event_loop.run_once(&mut server).unwrap();

        // Check that the server disposes of the client connection.
        assert!(!client_connected(&server, client_id));
    }

    /// Tests that a Server will attempt to reconnect to an unreachable peer
    /// after failing to connect at startup.
    #[test]
    fn test_unreachable_peer_reconnect() {
        let _ = env_logger::init();
        let peer_id = ServerId::from(1);
        let mut peers = HashMap::new();
        peers.insert(peer_id, get_unbound_address());

        // Creates the Server, which registers the peer connection.
        let (mut server, mut event_loop) = new_test_server(peers).unwrap();

        // Error event for the peer connection; connection is reset.
        event_loop.run_once(&mut server).unwrap();
        assert!(!peer_connected(&mut server, peer_id));

        // Reconnection timeout fires and connection stream is recreated.
        event_loop.run_once(&mut server).unwrap();
        assert!(peer_connected(&mut server, peer_id));

        // Error event for the new peer connection; connection is reset.
        event_loop.run_once(&mut server).unwrap();
        assert!(!peer_connected(&mut server, peer_id));
    }
}
