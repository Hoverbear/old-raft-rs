use std::thread;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::ops::Deref;
// MIO
use mio::tcp::{listen, TcpListener, TcpStream};
use mio::util::Slab;
use mio::Socket;
use mio::buf::{ByteBuf, MutByteBuf, SliceBuf};
use mio::{Interest, PollOpt, NonBlock, Token, EventLoop, Handler, ReadHint};

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
use super::RaftError;

// MIO Tokens
const TIMEOUT: Token = Token(0);
const LISTENER:  Token = Token(1);

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
    listener: NonBlock<TcpListener>,
    connections: Slab<RaftConnection>,
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
        let listener = listen(&addr).unwrap();
        listener.set_reuseaddr(true);
        event_loop.register(&listener, LISTENER).unwrap();
        event_loop.timeout_ms(TIMEOUT, 250).unwrap();
        let replica = Replica::new(addr, peers, store, state_machine);
        // Fire up the thread.
        thread::Builder::new().name(format!("RaftNode {}", addr)).spawn(move || {
            let mut raft_node = RaftNode {
                listener: listener,
                replica: replica,
                connections: Slab::new_starting_at(Token(2), 128),
            };
            event_loop.run(&mut raft_node).unwrap();
        }).unwrap();
    }
}

impl<S, M> Handler for RaftNode<S, M> where S: Store, M: StateMachine {

    type Message = ();
    type Timeout = Token;

    /// A registered IoHandle has available writing space.
    fn writable(&mut self, reactor: &mut Reactor<S, M>, token: Token) {
        match token {
            TIMEOUT => unreachable!(),
            LISTENER => unreachable!(),
            tok => {
                self.connections[tok].writable(reactor, &mut self.replica);
            }
        }
    }

    /// A registered IoHandle has available data to read
    fn readable(&mut self, reactor: &mut Reactor<S, M>, token: Token, hint: ReadHint) {
        match token {
            TIMEOUT => unreachable!(),
            LISTENER => {
                let stream = self.listener.accept().unwrap().unwrap(); // Result<Option<_>,_>
                let conn = RaftConnection::new(stream);
                let tok = self.connections.insert(conn)
                    .ok().expect("Could not add connection to slab.");

                // Register the connection
                self.connections[tok].token = tok;
                reactor.register_opt(&self.connections[tok].stream, tok, Interest::readable(), PollOpt::edge() | PollOpt::oneshot())
                    .ok().expect("Could not register socket with event loop.");
            },
            tok => {
                self.connections[tok].readable(reactor, &mut self.replica);
            }
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

struct RaftConnection {
    stream: NonBlock<TcpStream>,
    token: Token,
    interest: Interest,
    current_read: Option<MutByteBuf>,
    current_write: Option<ByteBuf>,
    next_write: Option<ByteBuf>,
}

impl RaftConnection {
    fn new(sock: NonBlock<TcpStream>) -> RaftConnection {
        RaftConnection {
            stream: sock,
            token: Token(-1),
            interest: Interest::hup(),
            current_read: None,
            current_write: None, // TODO: Size??
            next_write: None,
        }
    }

    fn writable<S, M>(&mut self, event_loop: &mut EventLoop<RaftNode<S, M>>, replica: &mut Replica<S,M>)
                      -> Result<(), RaftError>
    where S: Store, M: StateMachine {
        unimplemented!();
    }

    fn readable<S, M>(&mut self, event_loop: &mut EventLoop<RaftNode<S, M>>, replica: &mut Replica<S,M>)
                      -> Result<(), RaftError>
    where S: Store, M: StateMachine {
        let mut message = MallocMessageBuilder::new_default();
        let from = self.stream.peer_addr().unwrap();
        let message_reader = serialize_packed::new_reader_unbuffered(&mut self.stream.deref(), ReaderOptions::new())
            .unwrap();
        if let Ok(request) = message_reader.get_root::<rpc_request::Reader>() {
            // We will be responding.
            match request.which().unwrap() {
                // TODO: Move these into replica?
                rpc_request::Which::AppendEntries(Ok(call)) => {
                    let res = message.init_root::<append_entries_response::Builder>();
                    replica.append_entries_request(from, call, res);
                },
                rpc_request::Which::RequestVote(Ok(call)) => {
                    let res = message.init_root::<request_vote_response::Builder>();
                    replica.request_vote_request(from, call, res);
                },
                _ => unimplemented!(),
            }
            serialize_packed::write_packed_message_unbuffered(&mut self.stream.deref(), &mut message).unwrap();
            Ok(())
        } else if let Ok(response) = message_reader.get_root::<rpc_response::Reader>() {
            // We won't be responding. This is already a response.
            match response.which().unwrap() {
                rpc_response::Which::AppendEntries(Ok(call)) => {
                    replica.append_entries_response(from, call);
                    Ok(())
                },
                rpc_response::Which::RequestVote(Ok(call)) => {
                    let res = message.init_root::<append_entries_request::Builder>();
                    replica.request_vote_response(from, call, res);
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
                    replica.client_append(from, call, res)
                },
                client_request::Which::Die(Ok(call)) => {
                    should_die = true;
                    let mut res = message.init_root::<client_response::Builder>();
                    res.set_success(());
                    debug!("Got a Die request from Client({}). Reason: {}", from, call);
                },
                client_request::Which::LeaderRefresh(()) => {
                    let mut res = message.init_root::<client_response::Builder>();
                    replica.client_leader_refresh(from, res)
                },
                _ => unimplemented!(),
            }
            serialize_packed::write_packed_message_unbuffered(&mut self.stream.deref(), &mut message);
            // Do this here so that we can send the response.
            if should_die {
                panic!("Got a Die request.");
            }
            Ok(())
        } else {
            // It's something we don't understand.
            unimplemented!();
        }
    }
}

// Old
// let mut message = MallocMessageBuilder::new_default();
// // TODO: Determine the stream we got a message on?
// if token == SOCKET && hint == ReadHint::data() {
//
// } else {
//     // TODO Log this?
//     unimplemented!();
// }
