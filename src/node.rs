use std::thread;
use std::collections::HashSet;
use std::net::SocketAddr;

// MIO
use mio::tcp::TcpListener;
use mio::{Token, EventLoop, Handler, ReadHint};

// Data structures.
use store::Store;
use replica::Replica;
use state_machine::StateMachine;

// Cap'n Proto
use capnp::serialize_packed;
use capnp::{MessageBuilder, MessageReader, ReaderOptions, MallocMessageBuilder};
use messages_capnp::{
    rpc_request,
    rpc_response,
    client_request,
    client_response,
    request_vote_response,
    append_entries_response,
    append_entries_request,
};

// MIO Tokens
const SOCKET:  Token = Token(0);
const TIMEOUT: Token = Token(1);

/// The Raft Distributed Consensus Algorithm requires two RPC calls to be available:
///
///   * `append_entries` which is used as both a heartbeat (with no payload) and the primary
///     interface for requests.
///   * `request_vote` which is used by candidates during campaigns to obtain a vote.
///
/// A `RaftNode` acts as a replicated state machine. The server's role in the cluster depends on it's
/// own status. It will maintain both volatile state (which can be safely lost) and persistent
/// state (which must be carefully stored and kept safe).
///
/// Currently, the `RaftNode` API is not well defined. **We are looking for feedback and suggestions.**
pub struct RaftNode<S, M> where S: Store, M: StateMachine {
    replica: Replica<S, M>,
    // Channels and Sockets
    listener: TcpListener, // TODO: Just use streams??
}

type Reactor<S, M> = EventLoop<RaftNode<S, M>>;

/// The implementation of the RaftNode. In most use cases, creating a `RaftNode` should just be
/// done via `::new()`.
impl<S, M> RaftNode<S, M> where S: Store, M: StateMachine {

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
        // Create an event loop
        let mut event_loop = Reactor::<S, M>::new().unwrap();
        // Setup the socket, make it not block.
        let listener = TcpListener::bind(&addr).unwrap();
        event_loop.register(&listener, SOCKET).unwrap();
        event_loop.timeout_ms(TIMEOUT, 250).unwrap();
        let replica = Replica::new(addr, peers, store, state_machine);
        // Fire up the thread.
        thread::Builder::new().name(format!("RaftNode {}", addr)).spawn(move || {
            let mut raft_node = RaftNode {
                listener: listener,
                replica: replica,
            };
            event_loop.run(&mut raft_node).unwrap();
        }).unwrap();
    }
}

impl<S, M> Handler for RaftNode<S, M> where S: Store, M: StateMachine {

    type Message = ();
    type Timeout = Token;

    /// A registered IoHandle has available data to read
    fn readable(&mut self, _reactor: &mut Reactor<S, M>, token: Token, hint: ReadHint) {
        let mut message = MallocMessageBuilder::new_default();
        // TODO: Determine the stream we got a message on?
        if token == SOCKET && hint == ReadHint::data() {
            let (mut stream, from) = self.listener.accept().unwrap();
            let message_reader = serialize_packed::new_reader_unbuffered(&stream, ReaderOptions::new())
                .unwrap();
            if let Ok(request) = message_reader.get_root::<rpc_request::Reader>() {
                // We will be responding.
                match request.which().unwrap() {
                    // TODO: Move these into replica?
                    rpc_request::Which::AppendEntries(Ok(call)) => {
                        let res = message.init_root::<append_entries_response::Builder>();
                        self.replica.append_entries_request(from, call, res);
                    },
                    rpc_request::Which::RequestVote(Ok(call)) => {
                        let res = message.init_root::<request_vote_response::Builder>();
                        self.replica.request_vote_request(from, call, res);
                    },
                    _ => unimplemented!(),
                }
                serialize_packed::write_packed_message_unbuffered(&mut stream, &mut message).unwrap();
            } else if let Ok(response) = message_reader.get_root::<rpc_response::Reader>() {
                // We won't be responding. This is already a response.
                match response.which().unwrap() {
                    rpc_response::Which::AppendEntries(Ok(call)) => {
                        self.replica.append_entries_response(from, call);
                    },
                    rpc_response::Which::RequestVote(Ok(call)) => {
                        let res = message.init_root::<append_entries_request::Builder>();
                        self.replica.request_vote_response(from, call, res);
                        // TODO: send the AppendEntries requests if necessary
                        // TODO: Can we just reset the timer?
                        unimplemented!();
                    },
                    _ => unimplemented!(),
                }
            } else if let Ok(client_req) = message_reader.get_root::<client_request::Reader>() {
                let mut should_die = false;
                // We will be responding.
                match client_req.which().unwrap() {
                    client_request::Which::Append(Ok(call)) => {
                        let mut res = message.init_root::<client_response::Builder>();
                        self.replica.client_append(from, call, res)
                    },
                    client_request::Which::Die(Ok(call)) => {
                        should_die = true;
                        let mut res = message.init_root::<client_response::Builder>();
                        res.set_success(());
                        debug!("Got a Die request from Client({}). Reason: {}", from, call);
                    },
                    client_request::Which::LeaderRefresh(()) => {
                        let mut res = message.init_root::<client_response::Builder>();
                        self.replica.client_leader_refresh(from, res)
                    },
                    _ => unimplemented!(),
                }
                serialize_packed::write_packed_message_unbuffered(&mut stream, &mut message);
                // Do this here so that we can send the response.
                if should_die {
                    panic!("Got a Die request.");
                }
            } else {
                // It's something we don't understand.
                unimplemented!();
            }
        } else {
            // TODO Log this?
            unimplemented!();
        }
    }

    /// A registered timer has expired
    fn timeout(&mut self, reactor: &mut Reactor<S, M>, token: Token) {
        let mut message = MallocMessageBuilder::new_default();
        let request = message.init_root::<rpc_request::Builder>();
        let (timeout, _send_message) = self.replica.timeout(request.init_request_vote());
        reactor.timeout_ms(TIMEOUT, timeout).unwrap();
        // TODO: send messages if necessary
    }
}
