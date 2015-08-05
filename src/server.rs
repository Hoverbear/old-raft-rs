//! `Server` is a Rust type which is responsible for coordinating with other remote `Server`
//! instances, responding to commands from the `Client`, and applying commands to a local
//! `StateMachine` consensus. A `Server` may be a `Leader`, `Follower`, or `Candidate` at any given
//! time as described by the Raft Consensus Algorithm.

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
use RaftError;
use ServerId;
use messages;
use messages_capnp::connection_preamble;
use consensus::{Consensus, Actions, ConsensusTimeout};
use state_machine::StateMachine;
use persistent_log::Log;
use connection::{Connection, ConnectionKind};

const LISTENER: Token = Token(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]

pub enum ServerTimeout {
    Consensus(ConsensusTimeout),
    Reconnect(Token),
}

/// The `Server` is responsible for receiving events from remote `Server` or `Client` instances,
/// as well as setting election and heartbeat timeouts.  When an event is received, it is applied
/// to the local `Consensus`. The `Consensus` may optionally return a new event which must be
/// dispatched to either the `Server` or `Client` which sent the original event, or to all
/// `Server` instances.
///
/// Because messages are passed asyncronously between `Server` instances, a `Server` could get
/// into a situation where multiple events are ready to be dispatched to a single remote `Server`.
/// In this situation, the `Server` will replace the existing event with the new event, except in
/// one special circumstance: if the new and existing messages are both `AppendEntryRequest`s with
/// the same `term`, then the new message will be dropped.
pub struct Server<L, M> where L: Log, M: StateMachine {

    /// Id of this server.
    id: ServerId,

    /// Raft state machine consensus.
    consensus: Consensus<L, M>,

    /// Connection listener.
    listener: TcpListener,

    /// Collection of connections indexed by token.
    connections: Slab<Connection>,

    /// Index of peer id to connection token.
    peer_tokens: HashMap<ServerId, Token>,

    /// Index of client id to connection token.
    client_tokens: HashMap<ClientId, Token>,

    /// Currently registered consensus timeouts.
    consensus_timeouts: HashMap<ConsensusTimeout, TimeoutHandle>,

    /// Currently registered reconnection timeouts.
    reconnection_timeouts: HashMap<Token, TimeoutHandle>,
}

/// The implementation of the Server.
impl<L, M> Server<L, M> where L: Log, M: StateMachine {

    /// Creates a new instance of the server.
    /// *Gotcha:* `peers` must not contain the local `id`.
    fn new(id: ServerId,
           addr: SocketAddr,
           peers: HashMap<ServerId, SocketAddr>,
           store: L,
           state_machine: M) -> Result<(Server<L, M>, EventLoop<Server<L, M>>)> {
        if peers.contains_key(&id) {
            return Err(Error::Raft(RaftError::InvalidPeerSet))
        }

        let consensus = Consensus::new(id, peers.clone(), store, state_machine);
        let mut event_loop = try!(EventLoop::<Server<L, M>>::new());
        let listener = try!(TcpListener::bind(&addr));
        try!(event_loop.register(&listener, LISTENER));

        let mut server = Server {
            id: id,
            consensus: consensus,
            listener: listener,
            connections: Slab::new_starting_at(Token(1), 129),
            peer_tokens: HashMap::new(),
            client_tokens: HashMap::new(),
            consensus_timeouts: HashMap::new(),
            reconnection_timeouts: HashMap::new(),
        };

        for (peer_id, peer_addr) in peers {
            let token: Token = try!(server.connections
                                          .insert(try!(Connection::peer(peer_id, peer_addr)))
                                          .map_err(|_| Error::Raft(RaftError::ConnectionLimitReached)));
            scoped_assert!(server.peer_tokens.insert(peer_id, token).is_none());

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
               store: L,
               state_machine: M) -> Result<()> {
        let (mut server, mut event_loop) = try!(Server::new(id, addr, peers, store, state_machine));
        let actions = server.consensus.init();
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
                 store: L,
                 state_machine: M) -> Result<JoinHandle<Result<()>>> {
        thread::Builder::new().name(format!("raft::Server({})", id)).spawn(move || {
            Server::run(id, addr, peers, store, state_machine)
        }).map_err(From::from)
    }

    fn execute_actions(&mut self,
                       event_loop: &mut EventLoop<Server<L, M>>,
                       actions: Actions) {
        scoped_debug!("executing actions: {:?}", actions);
        let Actions { peer_messages, client_messages, timeouts, clear_timeouts } = actions;

        for (peer, message) in peer_messages {
            let token = self.peer_tokens[&peer];
            if self.connections[token].send_message(message) {
                self.connections[token]
                    .reregister(event_loop, token)
                    .unwrap_or_else(|_| self.reset_connection(event_loop, token));
            }
        }
        for (client, message) in client_messages {
            if let Some(&token) = self.client_tokens.get(&client) {
                if self.connections[token].send_message(message) {
                    self.connections[token]
                        .reregister(event_loop, token)
                        .unwrap_or_else(|_| self.reset_connection(event_loop, token));
                }
            }
        }
        if clear_timeouts {
            for (timeout, &handle) in &self.consensus_timeouts {
                scoped_assert!(event_loop.clear_timeout(handle),
                               "unable to clear timeout: {:?}", timeout);
            }
            self.consensus_timeouts.clear();
        }
        for timeout in timeouts {
            let duration = timeout.duration_ms();

            // Registering a timeout may only fail if the maximum number of timeouts
            // is already registered, which is by default 65,536. We use a
            // maximum of one timeout per peer, so this unwrap should be safe.
            let handle = event_loop.timeout_ms(ServerTimeout::Consensus(timeout), duration)
                                   .unwrap();
            self.consensus_timeouts
                .insert(timeout, handle)
                .map(|handle| scoped_assert!(event_loop.clear_timeout(handle),
                                             "unable to clear timeout: {:?}", timeout));
        }
    }

    /// Resets the connection corresponding to the provided token.
    ///
    /// If the connection is to a peer, the server will attempt to reconnect after a waiting
    /// period.
    ///
    /// If the connection is to a client or unknown it will be closed.
    fn reset_connection(&mut self, event_loop: &mut EventLoop<Server<L, M>>, token: Token) {
        let kind = *self.connections[token].kind();
        match kind {
            ConnectionKind::Peer(..) => {
                // Crash if reseting the connection fails.
                let (timeout, handle) = self.connections[token]
                                            .reset_peer(event_loop, token)
                                            .unwrap();

                scoped_assert!(self.reconnection_timeouts.insert(token, handle).is_none(),
                               "timeout already registered: {:?}", timeout);
            },
            ConnectionKind::Client(ref id) => {
                self.connections.remove(token).expect("unable to find client connection");
                scoped_assert!(self.client_tokens.remove(id).is_some(),
                             "client {:?} not connected", id);
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
    fn readable(&mut self, event_loop: &mut EventLoop<Server<L, M>>, token: Token) -> Result<()> {
        scoped_trace!("{:?}: readable event", self.connections[token]);
        // Read messages from the connection until there are no more.
        while let Some(message) = try!(self.connections[token].readable()) {
            match *self.connections[token].kind() {
                ConnectionKind::Peer(id) => {
                    let mut actions = Actions::new();
                    self.consensus.apply_peer_message(id, &message, &mut actions);
                    self.execute_actions(event_loop, actions);
                },
                ConnectionKind::Client(id) => {
                    let mut actions = Actions::new();
                    self.consensus.apply_client_message(id, &message, &mut actions);
                    self.execute_actions(event_loop, actions);
                },
                ConnectionKind::Unknown => {
                    let preamble = try!(message.get_root::<connection_preamble::Reader>());
                    match try!(preamble.get_id().which()) {
                        connection_preamble::id::Which::Server(id) => {
                            let peer_id = ServerId(id);
                            scoped_debug!("received new connection from {:?}", peer_id);

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
                                .map(|handle| scoped_assert!(event_loop.clear_timeout(handle)));

                            // TODO: add reconnect messages from consensus
                        },
                        connection_preamble::id::Which::Client(Ok(id)) => {
                            let client_id = try!(ClientId::from_bytes(id));
                            scoped_debug!("received new connection from {:?}", client_id);
                            self.connections[token]
                                .set_kind(ConnectionKind::Client(client_id));
                            let prev_token = self.client_tokens
                                                 .insert(client_id, token);
                            scoped_assert!(prev_token.is_none(),
                                    "{:?}: two clients connected with the same id: {:?}",
                                    self, client_id);
                        },
                        _ => {
                            return Err(Error::Raft(RaftError::UnknownConnectionType));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Accepts a new TCP connection, adds it to the connection slab, and registers it with the
    /// event loop.
    fn accept_connection(&mut self, event_loop: &mut EventLoop<Server<L, M>>) -> Result<()> {
        scoped_trace!("accept_connection");
        self.listener.accept().map_err(From::from)
            .and_then(|stream_opt| stream_opt.ok_or(Error::Io(
                    io::Error::new(io::ErrorKind::WouldBlock, "listener.accept() returned None"))))
            .and_then(|stream| Connection::unknown(stream))
            .and_then(|mut conn| match self.connections.reserve_token() {
                Some(mut t) => conn.register(event_loop, *t.get_key()).map(|()| t.insert(conn)),
                None => Err(Error::Raft(RaftError::ConnectionLimitReached)),
            })
    }
}

impl<L, M> Handler for Server<L, M> where L: Log, M: StateMachine {

    type Message = ();
    type Timeout = ServerTimeout;

    fn ready(&mut self, event_loop: &mut EventLoop<Server<L, M>>, token: Token, events: EventSet) {
        push_log_scope!("{:?}", self);
        scoped_trace!("ready; token: {:?}; events: {:?}", token, events);

        if events.is_error() {
            scoped_assert!(token != LISTENER, "unexpected error event from LISTENER");
            scoped_warn!("{:?}: error event", self.connections[token]);
            self.reset_connection(event_loop, token);
            return;
        }

        if events.is_hup() {
            scoped_assert!(token != LISTENER, "unexpected hup event from LISTENER");
            scoped_trace!("{:?}: hup event", self.connections[token]);
            self.reset_connection(event_loop, token);
            return;
        }

        if events.is_writable() {
            scoped_assert!(token != LISTENER, "unexpected writeable event for LISTENER");
            if let Err(error) = self.connections[token].writable() {
                scoped_warn!("{:?}: failed write: {}",
                             self.connections[token], error);
                self.reset_connection(event_loop, token);
                return;
            }
            if !events.is_readable() {
                self.connections[token]
                    .reregister(event_loop, token)
                    .unwrap_or_else(|_| self.reset_connection(event_loop, token));
            }
        }

        if events.is_readable() {
            if token == LISTENER {
                self.accept_connection(event_loop)
                    .unwrap_or_else(|error| scoped_warn!("unable to accept connection: {}", error));
            } else {
                self.readable(event_loop, token)
                    // Only reregister the connection with the event loop if no error occurs and
                    // the connection is *not* reset.
                    .and_then(|_| self.connections[token].reregister(event_loop, token))
                    .unwrap_or_else(|error| {
                        scoped_warn!("{:?}: failed read: {}",
                                     self.connections[token], error);
                        self.reset_connection(event_loop, token);
                    });
            }
        }
    }

    fn timeout(&mut self, event_loop: &mut EventLoop<Server<L, M>>, timeout: ServerTimeout) {
        push_log_scope!("{:?}", self);
        scoped_trace!("timeout: {:?}", &timeout);
        match timeout {
            ServerTimeout::Consensus(consensus) => {
                scoped_assert!(self.consensus_timeouts.remove(&consensus).is_some(),
                               "missing timeout: {:?}", timeout);
                let mut actions = Actions::new();
                self.consensus.apply_timeout(consensus, &mut actions);
                self.execute_actions(event_loop, actions);
            },

            ServerTimeout::Reconnect(token) => {
                scoped_assert!(self.reconnection_timeouts.remove(&token).is_some(),
                               "{:?} missing timeout: {:?}", self.connections[token], timeout);
                self.connections[token]
                    .reconnect_peer(self.id)
                    .and_then(|_| self.connections[token].register(event_loop, token))
                    .unwrap_or_else(|error| {
                        scoped_warn!("unable to reconnect connection {:?}: {}",
                                     self.connections[token], error);
                        self.reset_connection(event_loop, token);
                    });
                // TODO: add reconnect messages from consensus
            },
        }
    }
}

impl <L, M> fmt::Debug for Server<L, M> where L: Log, M: StateMachine {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Server({})", self.id)
    }
}

#[cfg(test)]
mod tests {

    extern crate env_logger;
    extern crate test;

    use std::collections::HashMap;
    use std::io::{self, Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::str::FromStr;

    use capnp::{serialize, MessageReader, ReaderOptions};
    use mio::EventLoop;
    use test::Bencher;

    use ClientId;
    use Result;
    use ServerId;
    use messages;
    use messages_capnp::connection_preamble;
    use consensus::Actions;
    use state_machine::NullStateMachine;
    use persistent_log::MemLog;
    use super::*;

    type TestServer = Server<MemLog, NullStateMachine>;

    fn new_test_server(peers: HashMap<ServerId, SocketAddr>)
                       -> Result<(TestServer, EventLoop<TestServer>)> {
        Server::new(ServerId::from(0),
                    SocketAddr::from_str("127.0.0.1:0").unwrap(),
                    peers,
                    MemLog::new(),
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

    /// Returns true if the server has an open connection with the client.
    fn client_connected(server: &TestServer, client: ClientId) -> bool {
        server.client_tokens.contains_key(&client)
    }

    /// Returns true if the provided TCP connection has been shutdown.
    ///
    /// TODO: figure out a more robust way to implement this, the current check
    /// will block the thread indefinitely if the stream is not shutdown.
    fn stream_shutdown(stream: &mut TcpStream) -> bool {
        let mut buf = [0u8; 128];
        // OS X returns a read of 0 length for closed sockets.
        // Linux returns an errcode 104: Connection reset by peer.
        match stream.read(&mut buf) {
            Ok(0) => true,
            Err(ref error) if error.kind() == io::ErrorKind::ConnectionReset => true,
            Err(ref error) => panic!("unexpected error: {}", error),
            _ => false,
        }
    }

    /// Tests that a Server will reject an invalid peer configuration set.
    #[test]
    fn test_illegal_peer_set() {
        setup_test!("test_illegal_peer_set");
        let peer_id = ServerId::from(0);
        let mut peers = HashMap::new();
        peers.insert(peer_id, SocketAddr::from_str("127.0.0.1:0").unwrap());
        assert!(new_test_server(peers).is_err());
    }

    /// Tests that a Server connects to peer at startup, and reconnects when the
    /// connection is droped.
    #[test]
    fn test_peer_connect() {
        setup_test!("test_peer_connect");
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
        setup_test!("test_peer_accept");
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
        out_stream.flush().unwrap();
        event_loop.run_once(&mut server).unwrap();

        // Check that the server has closed the old connection.
        assert!(stream_shutdown(&mut in_stream));
    }

    /// Tests that the server will accept a client connection, then dispose of
    /// it when the client disconnects.
    #[test]
    fn test_client_accept() {
        setup_test!("test_client_accept");

        let (mut server, mut event_loop) = new_test_server(HashMap::new()).unwrap();

        // Connect to the server.
        let server_addr = server.listener.local_addr().unwrap();
        let mut stream = TcpStream::connect(server_addr).unwrap();
        event_loop.run_once(&mut server).unwrap();

        let client_id = ClientId::new();

        // Send the client preamble message to the server.
        serialize::write_message(&mut stream, &*messages::client_connection_preamble(client_id))
                 .unwrap();
        stream.flush().unwrap();
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
        setup_test!("test_invalid_accept");

        let (mut server, mut event_loop) = new_test_server(HashMap::new()).unwrap();

        // Connect to the server.
        let server_addr = server.listener.local_addr().unwrap();
        let mut stream = TcpStream::connect(server_addr).unwrap();
        event_loop.run_once(&mut server).unwrap();

        // Send an invalid preamble.
        stream.write(b"foo bar baz").unwrap();
        stream.flush().unwrap();
        event_loop.run_once(&mut server).unwrap();

        // Check that the server disposes of the connection.
        assert!(stream_shutdown(&mut stream));
    }

    /// Tests that the server will reset a peer connection when an invalid
    /// message is received.
    #[test]
    fn test_invalid_peer_message() {
        setup_test!("test_invalid_peer_message");

        let peer_id = ServerId::from(1);

        let peer_listener = TcpListener::bind("127.0.0.1:0").unwrap();

        let mut peers = HashMap::new();
        peers.insert(peer_id, peer_listener.local_addr().unwrap());
        let (mut server, mut event_loop) = new_test_server(peers).unwrap();

        // Accept the server's connection.
        let (mut stream_a, _)  = peer_listener.accept().unwrap();

        // Read the server's preamble.
        event_loop.run_once(&mut server).unwrap();
        assert_eq!(ServerId::from(0), read_server_preamble(&mut stream_a));

        // Send an invalid message.
        stream_a.write(b"foo bar baz").unwrap();
        stream_a.flush().unwrap();
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
        setup_test!("test_invalid_client_message");

        let (mut server, mut event_loop) = new_test_server(HashMap::new()).unwrap();

        // Connect to the server.
        let server_addr = server.listener.local_addr().unwrap();
        let mut stream = TcpStream::connect(server_addr).unwrap();
        event_loop.run_once(&mut server).unwrap();

        let client_id = ClientId::new();

        // Send the client preamble message to the server.
        serialize::write_message(&mut stream, &*messages::client_connection_preamble(client_id))
                 .unwrap();
        stream.flush().unwrap();
        event_loop.run_once(&mut server).unwrap();

        // Check that the server holds on to the client connection.
        assert!(client_connected(&server, client_id));

        // Send an invalid client message to the server.
        stream.write(b"foo bar baz").unwrap();
        stream.flush().unwrap();
        event_loop.run_once(&mut server).unwrap();

        // Check that the server disposes of the client connection.
        assert!(!client_connected(&server, client_id));
    }

    /// Tests that a Server will attempt to reconnect to an unreachable peer
    /// after failing to connect at startup.
    #[test]
    fn test_unreachable_peer_reconnect() {
        setup_test!("test_unreachable_peer_reconnect");
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

    /// Tests that the server will send a message to a peer connection.
    #[test]
    fn test_connection_send() {
        setup_test!("test_connection_send");
        let peer_id = ServerId::from(1);

        let peer_listener = TcpListener::bind("127.0.0.1:0").unwrap();

        let mut peers = HashMap::new();
        peers.insert(peer_id, peer_listener.local_addr().unwrap());
        let (mut server, mut event_loop) = new_test_server(peers).unwrap();

        // Accept the server's connection.
        let (mut in_stream, _)  = peer_listener.accept().unwrap();

        // Accept the preamble.
        event_loop.run_once(&mut server).unwrap();
        assert_eq!(ServerId::from(0), read_server_preamble(&mut in_stream));

        // Send a test message (the type is not important).
        let mut actions = Actions::new();
        actions.peer_messages.push((peer_id, messages::server_connection_preamble(peer_id)));
        server.execute_actions(&mut event_loop, actions);
        event_loop.run_once(&mut server).unwrap();

        assert_eq!(peer_id, read_server_preamble(&mut in_stream));
    }

    /// Literally the same thing as above.
    #[bench]
    fn bench_connection_send(b: &mut Bencher) {
        setup_test!("test_connection_send");
        b.iter(|| {
            let peer_id = ServerId::from(1);

            let peer_listener = TcpListener::bind("127.0.0.1:0").unwrap();

            let mut peers = HashMap::new();
            peers.insert(peer_id, peer_listener.local_addr().unwrap());
            let (mut server, mut event_loop) = new_test_server(peers).unwrap();

            // Accept the server's connection.
            let (mut in_stream, _)  = peer_listener.accept().unwrap();

            // Accept the preamble.
            event_loop.run_once(&mut server).unwrap();
            assert_eq!(ServerId::from(0), read_server_preamble(&mut in_stream));

            // Send a test message (the type is not important).
            let mut actions = Actions::new();
            actions.peer_messages.push((peer_id, messages::server_connection_preamble(peer_id)));
            server.execute_actions(&mut event_loop, actions);
            event_loop.run_once(&mut server).unwrap();

            assert_eq!(peer_id, read_server_preamble(&mut in_stream));
        })
    }
}
