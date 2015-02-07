#![crate_name = "raft"]
#![crate_type="lib"]

#![feature(core)]
#![feature(io)]
#![feature(std_misc)]
#![feature(rand)]
extern crate "rustc-serialize" as rustc_serialize;
extern crate uuid;
pub mod interchange;

use std::old_io::net::ip::SocketAddr;
use std::old_io::net::udp::UdpSocket;
use std::old_io::timer::Timer;
use std::time::Duration;
use std::thread::Thread;
use std::rand::{thread_rng, Rng, ThreadRng};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::str;
use std::collections::HashMap;
use rustc_serialize::{json, Encodable, Decodable};
use uuid::Uuid;

// Enums and variants.
use interchange::{ClientRequest, RemoteProcedureCall, RemoteProcedureResponse};
// Data structures.
use interchange::{AppendEntries, RequestVote};
use interchange::{AppendRequest, IndexRange};
use NodeState::{Leader, Follower, Candidate};

// The maximum size of the read buffer.
const BUFFER_SIZE: usize = 4096;
const HEARTBEAT_MIN: i64 = 150;
const HEARTBEAT_MAX: i64 = 300;

/// The Raft Distributed Consensus Algorithm requires two RPC calls to be available:
///
///   * `append_entries` which is used as both a heartbeat (with no payload) and the primary
///     interface for requests.
///   * `request_vote` which is used by candidates during campaigns to obtain a vote.
///
pub trait Raft<T: Encodable + Decodable + Send + Clone> {
    /// Returns (term, success)
    fn append_entries(term: u64, leader_id: u64, prev_log_index: u64,
                      prev_log_term: u64, entries: Vec<T>,
                      leader_commit: u64) -> (u64, bool);

    /// Returns (term, voteGranted)
    fn request_vote(term: u64, candidate_id: u64, last_log_index: u64,
                    last_log_term: u64) -> (u64, bool);
}

/// A `RaftNode` acts as a replicated state machine. The server's role in the cluster depends on it's
/// own status. It will maintain both volatile state (which can be safely lost) and persistent
/// state (which must be carefully stored and kept safe).
pub struct RaftNode<T: Encodable + Decodable + Send + Clone> {
    // Raft related.
    state: NodeState,
    persistent_state: PersistentState<T>,
    volatile_state: VolatileState,
    // Auxilary Data.
    // TODO: This should probably be split off.
    // All nodes need to know this otherwise they can't effectively lead or hold elections.
    leader_id: Option<u64>,
    own_id: u64,
    id_to_addr: HashMap<u64, SocketAddr>, // TODO: Can we have a dual purpose?
    addr_to_id: HashMap<SocketAddr, u64>, // TODO
    heartbeat: Receiver<()>,
    rng: ThreadRng,
    timer: Timer,
    socket: UdpSocket,
    req_recv: Receiver<ClientRequest<T>>,
    res_send: Sender<Result<Vec<T>, String>>,
}

/// The implementation of the RaftNode. In most use cases, creating a `RaftNode` should just be
/// done via `::new()`.
///
/// ```
/// use raft::RaftNode;
/// use std::old_io::net::ip::SocketAddr;
/// use std::old_io::net::ip::IpAddr::Ipv4Addr;
/// use std::collections::HashMap;
///
/// let mut nodes = HashMap::new();
/// nodes.insert(1, SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11111 });
/// nodes.insert(2, SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11112 });
/// // Create the nodes.
/// let node = RaftNode::<String>::start(1, nodes.clone());
/// ```
impl<T: Encodable + Decodable + Send + Clone> RaftNode<T> {
    /// Creates a new RaftNode with the neighbors specified. `id` should be a valid index into
    /// `nodes`. The idea is that you can use the same `nodes` on all of the clients and only vary
    /// `id`.
    pub fn start (id: u64, nodes: Vec<(u64, SocketAddr)>) -> (Sender<ClientRequest<T>>, Receiver<Result<Vec<T>, String>>) {
        // TODO: Check index.
        // Build the Hashmap lookups. We don't have a bimap :(
        let mut id_to_addr = HashMap::new();
        let mut addr_to_id = HashMap::new();
        for (id, socket) in nodes {
            id_to_addr.insert(id, socket);
            addr_to_id.insert(socket, id);
        }
        // Setup the socket, make it not block.
        let own_socket_addr = id_to_addr.get(&id)
            .unwrap().clone(); // TODO: Can we do better?
        let mut socket = UdpSocket::bind(own_socket_addr)
            .unwrap(); // TODO: Can we do better?
        socket.set_read_timeout(Some(0));
        // Communication channels.
        let (req_send, req_recv) = channel::<ClientRequest<T>>();
        let (res_send, res_recv) = channel::<Result<Vec<T>, String>>();
        // Fire up the thread.
        Thread::spawn(move || {
            // Start up a RNG and Timer
            let mut rng = thread_rng();
            let mut timer = Timer::new().unwrap();
            // We need a read buffer.
            let mut read_buffer = [0; BUFFER_SIZE];
            // Create the struct.
            let mut raft_node = RaftNode {
                state: Follower,
                persistent_state: PersistentState {
                    current_term: 0, // TODO: Double Check.
                    voted_for: None,
                    log: Vec::<(u64, T)>::new(),
                },
                volatile_state: VolatileState {
                    commit_index: 0,
                    last_applied: 0,
                },
                leader_id: None,
                own_id: id,
                id_to_addr: id_to_addr,
                addr_to_id: addr_to_id,
                // Blank timer for now.
                heartbeat: timer.oneshot(Duration::milliseconds(rng.gen_range::<i64>(HEARTBEAT_MIN, HEARTBEAT_MAX))), // If this fails we're in trouble.
                timer: timer,
                rng: rng,
                socket: socket,
                req_recv: req_recv,
                res_send: res_send,
            };
            // This is the main, strongly typed state machine. It loops indefinitely for now. It
            // would be nice if this was event based.
            loop {
                raft_node.tick();
            }
        });
        (req_send, res_recv)
    }
    /// This is the main tick for a leader node.
    fn tick(&mut self) {
        // We need a read buffer.
        let mut read_buffer = [0; BUFFER_SIZE];
        // If socket has data.
        match self.socket.recv_from(&mut read_buffer) {
            Ok((num_read, source)) => { // Something on the socket.
                // TODO: Verify this is a legitimate request, just check if it's
                //       in the cluster for now?
                // This is possibly an RPC from another node. Try to parse it out
                // and determine what to do based on it's variant.
                let data = str::from_utf8(&mut read_buffer[.. num_read])
                    .unwrap();
                if let Ok(rpc) = json::decode::<RemoteProcedureCall<T>>(data) {
                    match rpc {
                        RemoteProcedureCall::RequestVote(call) =>
                            self.handle_request_vote(call, source),
                        RemoteProcedureCall::AppendEntries(call) =>
                            self.handle_append_entries(call, source),
                    }
                } else if let Ok(rpr) = json::decode::<RemoteProcedureResponse>(data) {
                    match rpr {
                        RemoteProcedureResponse::Accepted { .. } =>
                            self.handle_accepted(rpr, source),
                        RemoteProcedureResponse::Rejected { .. } =>
                            self.handle_rejected(rpr, source),
                    }
                }
            },
            Err(_) => (),                 // Nothing on the socket.
        }
        // If channel has data.
        match self.req_recv.try_recv() {
            Ok(request) => {              // Something in channel.
                match request {
                    ClientRequest::IndexRange(request) =>
                        self.handle_index_range(request),
                    ClientRequest::AppendRequest(request) =>
                        self.handle_append_request(request),
                }
            },
            Err(_) => (),               // Nothing in channel.
        }
        // If timer has fired.
        match self.heartbeat.try_recv() {
            Ok(_) => {                  // Timer has fired.
                // A heartbeat has fired.
                self.handle_timer()
            },
            Err(_) => (),               // Timer hasn't fired.
        }
    }
    /// A lookup for index -> SocketAddr
    fn lookup_addr(&self, id: u64) -> Option<&SocketAddr> {
        self.id_to_addr.get(&id)
    }
    fn lookup_id(&self, addr: SocketAddr) -> Option<&u64> {
        self.addr_to_id.get(&addr)
    }
    fn other_nodes(&self) -> Vec<SocketAddr> {
        self.id_to_addr.iter().filter_map(|(&k, &v)| {
            if k == self.own_id {
                None
            } else {
                Some(v)
            }
        }).collect::<Vec<SocketAddr>>()
    }
    /// When a `Follower`'s heartbeat times out it's time to start a campaign for election and
    /// become a `Candidate`. If successful, the `RaftNode` will transistion state into a `Leader`,
    /// otherwise it will become `Follower` again.
    /// This function accepts a `Follower` and transforms it into a `Candidate` then attempts to
    /// issue `RequestVote` remote procedure calls to other known nodes. If a majority come back
    /// accepted, it will become the leader.
    fn campaign(&mut self) {
        // On conversion to Candidate:
        // * Increment current_term
        // * Vote for self
        // * Reset election timer
        // * Send RequestVote RPC to all other nodes.
        match self.state {
            Follower  => self.follower_to_candidate(),
            Candidate => self.reset_candidate(),
            _ => panic!("Should not campaign as a Leader!")
        };
        self.persistent_state.current_term += 1;
        self.persistent_state.voted_for = Some(self.own_id); // TODO: Is this correct?
        self.reset_timer();
        // Issue `RequestVote` to known nodes.
        let request = RemoteProcedureCall::request_vote(
            self.persistent_state.current_term,
            self.persistent_state.voted_for.unwrap(), // TODO: Safe because we just set it. But correct
            self.volatile_state.last_applied,
            0); // TODO: Get this.
        self.broadcast(request);
            // We rely on the loop to handle incoming responses regarding `RequestVote`, don't worry
            // about that here.
    }
    //////////////
    // Handlers //
    //////////////
    /// Handles a `RemoteProcedureCall::RequestVote` call.
    ///
    ///   * Reply false if term < currentTerm.
    ///   * If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as
    ///     receiver’s log, grant vote.
    fn handle_request_vote(&mut self, call: RequestVote, source: SocketAddr) {
        match self.state {
            Leader(_) => {
                // Re-assert leadership.
                assert!(self.leader_id.is_some());
                assert!(self.leader_id.unwrap() == self.own_id);
                let rpr = RemoteProcedureResponse::reject(
                    call.uuid,
                    self.persistent_state.current_term,
                    self.leader_id.unwrap(), // Should be self.
                );
                let encoded = json::encode::<RemoteProcedureResponse>(&rpr)
                    .unwrap();
                self.send_to(encoded.as_bytes(), source)
                    .unwrap(); // TODO: Can we do better?
            },
            Follower => {
                let checks = [
                    self.persistent_state.current_term < call.term,
                    self.persistent_state.voted_for.is_none(),
                    self.volatile_state.last_applied < call.last_log_index,
                    true, // TODO: Is the last log term the same?
                ];
                if checks.iter().all(|&x| x) {
                    // Grant.
                    let rpr = RemoteProcedureResponse::accept(call.uuid, call.term);
                    let encoded = json::encode::<RemoteProcedureResponse>(&rpr)
                        .unwrap();
                    self.send_to(encoded.as_bytes(), source)
                        .unwrap(); // TODO: Can we do better?
                } else {
                    // Reject.
                }
                // If voted_for is None or the requester's ID, and their log is up to date, accept.
                unimplemented!();
            },
            Candidate => {
                // TODO ???
                unimplemented!();
            }
        }
    }
    /// Handles an `AppendEntries` request from a caller.
    fn handle_append_entries(&mut self, call: AppendEntries<T>, source: SocketAddr) {
        match self.state {
            Leader(ref state) => {
                unimplemented!();
            },
            Follower => {
                unimplemented!();
            },
            Candidate => {
                unimplemented!();
            },
        }
    }
    fn handle_append_request(&mut self, request: AppendRequest<T>) {
        match self.state {
            Leader(ref state) => {
                unimplemented!();
            },
            Follower => {
                unimplemented!();
            },
            Candidate => {
                unimplemented!();
            },
        }
    }
    fn handle_index_range(&mut self, request: IndexRange) {
        unimplemented!();
    }
    fn handle_accepted(&mut self, response: RemoteProcedureResponse, source: SocketAddr) {
        unimplemented!();
    }
    fn handle_rejected(&mut self, response: RemoteProcedureResponse, source: SocketAddr) {
        unimplemented!();
    }
    ////////////
    // Timers //
    ////////////
    fn handle_timer(&mut self) {
        match self.state {
            Leader(ref state) => {
                // Send heartbeats.
                unimplemented!();
            },
            Follower => self.campaign(),
            Candidate => self.campaign(),
        }
    }
    fn reset_timer(&mut self) {
        self.heartbeat = self.timer.oneshot(Duration::milliseconds(self.rng.gen_range::<i64>(HEARTBEAT_MIN, HEARTBEAT_MAX)));
    }
    //////////////////
    // Transmission //
    //////////////////
    fn broadcast(&mut self, rpc: RemoteProcedureCall<T>) -> Result<(), std::old_io::IoError> {
        let encoded = json::encode::<RemoteProcedureCall<T>>(&rpc)
            .unwrap();
        for node in self.other_nodes() {
            try!(self.send_to(encoded.as_bytes(), node));
        }
        Ok(())
    }
    // TODO: Improve "message" to not be &[u8]
    fn send_to(&mut self, message: &[u8], node: SocketAddr) -> Result<(), std::old_io::IoError> {
        self.socket.send_to(message, node)
    }
    fn respond(&mut self, rpr: RemoteProcedureResponse, id: u64) -> Result<(), std::old_io::IoError> {
        let encoded = json::encode::<RemoteProcedureResponse>(&rpr)
            .unwrap();
        let destination = self.lookup_addr(id).unwrap().clone();
        self.send_to(encoded.as_bytes(), destination)
    }
    ///////////////////
    // State Changes //
    ///////////////////
    /// Called on heartbeat timeout.
    pub fn follower_to_candidate(&mut self) {
        self.state = match self.state {
            Follower => Candidate,
            _ => panic!("Called follower_to_candidate() but was not Follower.")
        }
    }
    /// Called when the Leader recieves information that they are not the leader.
    pub fn leader_to_follower(&mut self) {
        self.state = match self.state {
            Leader(_) => Follower,
            _ => panic!("Called leader_to_follower() but was not Leader.")
        }
    }
    /// Called when a Candidate successfully gets elected.
    pub fn candidate_to_leader(&mut self) {
        self.state = match self.state {
            Candidate => Leader(LeaderState {
                next_index: Vec::new(),
                match_index: Vec::new()
            }),
            _ => panic!("Called candidate_to_leader() but was not Candidate.")
        }
    }
    /// Called when a Candidate needs to hold another election.
    /// TODO: This is currently pointless, but will be meaningful when Candidates
    /// have data as part of their variant.
    pub fn reset_candidate(&mut self) {
        self.state = match self.state {
            Candidate => Candidate,
            _ => panic!("Called reset_candidate() but was not Candidate.")
        }
    }
}

/// The RPC calls required by the Raft protocol.
impl<T: Encodable + Decodable + Send + Clone> Raft<T> for RaftNode<T> {
    /// Returns (term, success)
    fn append_entries(term: u64, leader_id: u64, prev_log_index: u64,
                      prev_log_term: u64, entries: Vec<T>,
                      leader_commit: u64) -> (u64, bool) {
        (0, false) // TODO: Implement
    }
    /// Returns (term, voteGranted)
    fn request_vote(term: u64, candidate_id: u64, last_log_index: u64,
                    last_log_term: u64) -> (u64, bool) {
        (0, false) // TODO: Implement
    }
}

/// Nodes can either be:
///
///   * A `Follower`, which replicates AppendEntries requests and votes for it's leader.
///   * A `Leader`, which leads the cluster by serving incoming requests, ensuring data is
///     replicated, and issuing heartbeats..
///   * A `Candidate`, which campaigns in an election and may become a `Leader` (if it gets enough
///     votes) or a `Follower`, if it hears from a `Leader`.
#[derive(PartialEq, Eq)]
pub enum NodeState {
    Follower,
    Leader(LeaderState),
    Candidate,
}

/// Persistent state
/// **Must be updated to stable storage before RPC response.**
pub struct PersistentState<T: Encodable + Decodable + Send + Clone> {
    current_term: u64,
    voted_for: Option<u64>, // request_vote cares if this is `None`
    log: Vec<(u64, T)>,
}

/// Volatile state
#[derive(Copy)]
pub struct VolatileState {
    commit_index: u64,
    last_applied: u64
}

/// Leader Only
/// **Reinitialized after election.**
#[derive(PartialEq, Eq)]
pub struct LeaderState {
    next_index: Vec<u64>,
    match_index: Vec<u64>
}
