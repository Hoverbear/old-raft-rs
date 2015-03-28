use std::{io, str, thread};
use std::collections::{HashMap, HashSet, VecDeque, BitSet};
use std::fmt::Debug;
use std::num::Int;
use std::net::SocketAddr;
use std::ops;
use std::marker::PhantomData;

use rand::{thread_rng, Rng, ThreadRng};
use rustc_serialize::{json, Encodable, Decodable};

// MIO
use mio::tcp::{TcpStream, TcpListener};
use mio::{Token, EventLoop, Handler, ReadHint};

// Data structures.
use state::LeaderState;
use state::NodeState::{Leader, Follower, Candidate};
use state::{NodeState, TransactionState, Transaction};
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
    request_vote_response,
    append_entries_response,
    client_response,
};

use Term;
use LogIndex;

// The maximum size of the read buffer.
const BUFFER_SIZE: usize = 4096;
const HEARTBEAT_MIN: u64 = 150;
const HEARTBEAT_MAX: u64 = 300;

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
pub struct RaftNode<T, S, M>
where T: Encodable + Decodable + Clone + Debug + Send + 'static,
      S: Store + Debug,
      M: StateMachine + Debug,
{
    // TODO: This should probably be split off.
    // All nodes need to know this otherwise they can't effectively lead or hold elections.
    leader: Option<SocketAddr>,
    replica: Replica<S, M>,
    // Channels and Sockets
    listener: TcpListener, // TODO: Just use streams??
    // State
    rng: ThreadRng,
    // TODO: Can we get rid of these?
    phantom_type: PhantomData<T>,
}

type Reactor<T, S, M> = EventLoop<RaftNode<T, S, M>>;

/// The implementation of the RaftNode. In most use cases, creating a `RaftNode` should just be
/// done via `::new()`.
impl<T, S, M> RaftNode<T, S, M>
where T: Encodable + Decodable + Clone + Debug + Send + 'static,
      S: Store + Debug,
      M: StateMachine + Debug
{

    /// Creates a new Raft node with the cluster members specified.
    ///
    /// # Arguments
    ///
    /// * `address` - The address of the new node.
    /// * `cluster_members` - The address of every cluster member, including all
    ///                       peer nodes and the new node.
    /// * `store` - The storage implementation.
    pub fn spawn(address: SocketAddr,
                 cluster_members: HashSet<SocketAddr>,
                 store: S, state_machine: M)
    {
        // Create an event loop
        let mut event_loop = EventLoop::<RaftNode<T, S, M>>::new().unwrap();
        // Setup the socket, make it not block.
        let listener = TcpListener::bind(&address).unwrap();
        event_loop.register(&listener, SOCKET).unwrap();
        event_loop.timeout_ms(TIMEOUT, 250).unwrap();
        let replica = Replica::new(address, cluster_members, store, state_machine);
        // Fire up the thread.
        thread::Builder::new().name(format!("RaftNode {}", address)).spawn(move || {
            // Start up a RNG and Timer
            let rng = thread_rng();
            // Create the struct.
            let mut raft_node = RaftNode {
                leader: None,
                rng: rng,
                listener: listener,
                replica: replica,
                phantom_type: PhantomData,
                // Recieve reqs from event_loop
            };
            // This is the main, strongly typed state machine. It loops indefinitely for now. It
            // would be nice if this was event based.
            event_loop.run(&mut raft_node).unwrap();
        }).unwrap();
    }

    fn reset_timer(&mut self, reactor: &mut Reactor<T, S, M>) {
        reactor.timeout_ms(TIMEOUT, self.rng.gen_range::<u64>(HEARTBEAT_MIN, HEARTBEAT_MAX)).unwrap();
    }
}

impl<T, S, M> Handler for RaftNode<T, S, M>
where T: Encodable + Decodable + Clone + Debug + Send + 'static,
      S: Store + Debug,
      M: StateMachine {
    type Message = T;
    type Timeout = Token;

    /// A registered IoHandle has available data to read
    fn readable(&mut self, reactor: &mut Reactor<T, S, M>, token: Token, hint: ReadHint) {
        // TODO: Determine the stream we got a message on?
        if token == SOCKET && hint == ReadHint::data() {
            let (mut stream, from) = self.listener.accept().unwrap();
            let message_reader = serialize_packed::new_reader_unbuffered(&stream, ReaderOptions::new())
                .unwrap();
            if let Ok(request) = message_reader.get_root::<rpc_request::Reader>() {
                // Build a response.
                let mut message = MallocMessageBuilder::new_default();
                // Act on it!
                {
                    match request.which().unwrap() {
                        // TODO: Move these into replica?
                        rpc_request::Which::AppendEntries(Ok(call)) => {
                            let mut res = message.init_root::<append_entries_response::Builder>();
                            self.replica.append_entries_request(from, call, res);
                        },
                        rpc_request::Which::RequestVote(Ok(call)) => {
                            let mut res = message.init_root::<request_vote_response::Builder>();
                            self.replica.request_vote_request(from, call, res);
                        },
                        _ => {
                            // TODO Log this?
                            unimplemented!()
                        },
                    }
                }
                serialize_packed::write_packed_message_unbuffered(&mut stream, &mut message).unwrap();
            } else if let Ok(response) = message_reader.get_root::<rpc_response::Reader>() {
                match response.which().unwrap() {
                    rpc_response::Which::AppendEntries(Ok(call)) => {
                        self.replica.append_entries_response(from, call);
                    },
                    rpc_response::Which::RequestVote(Ok(call)) => {
                        self.replica.request_vote_response(from, call);
                    },
                    _ => {
                        // TODO Log this?
                        unimplemented!()
                    },
                }
            } else if let Ok(client_req) = message_reader.get_root::<client_request::Reader>() {
                match client_req.which().unwrap() {
                    client_request::Which::Append(Ok(call)) => {
                        unimplemented!()
                    },
                    client_request::Which::Die(Ok(call)) => {
                        unimplemented!()
                    },
                    _ => {
                        // TODO Log this?
                        unimplemented!();
                    }
                }
            }
        } else {
            // TODO Log this?
            unimplemented!();
        }
    }

    /// A registered timer has expired
    fn timeout(&mut self, reactor: &mut Reactor<T, S, M>, token: Token) {
        // If timer has fired.
        // TODO: Reset timer.
        self.replica.timeout()
    }
}
