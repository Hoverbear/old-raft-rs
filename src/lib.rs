#![crate_name = "raft"]
#![crate_type="lib"]

#![feature(core)]
#![feature(io)]
#![feature(std_misc)]
#![feature(rand)]
extern crate "rustc-serialize" as rustc_serialize;
extern crate uuid;
extern crate rand;
pub mod interchange;

use std::old_io::net::ip::SocketAddr;
use std::old_io::net::udp::UdpSocket;
use std::old_io::timer::Timer;
use std::old_io::IoError;
use std::io;
use std::io::Seek;
use std::time::Duration;
use std::thread::Thread;
use std::fs::File;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::str;
use std::collections::{HashMap, RingBuf};
use rustc_serialize::{json, Encodable, Decodable};
use uuid::Uuid;
use rand::{thread_rng, Rng, ThreadRng};

// Enums and variants.
use interchange::{ClientRequest, RemoteProcedureCall, RemoteProcedureResponse};
// Data structures.
use interchange::{AppendEntries, RequestVote};
use interchange::{AppendRequest, IndexRange};
use interchange::{Accepted, Rejected};
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
    // Lookups
    id_to_addr: HashMap<u64, SocketAddr>, // TODO: Can we have a dual purpose?
    addr_to_id: HashMap<SocketAddr, u64>, // TODO
    // Channels and Sockets
    heartbeat: Receiver<()>,
    socket: UdpSocket,
    req_recv: Receiver<ClientRequest<T>>,
    res_send: Sender<Result<Vec<T>, IoError>>,
    // State
    rng: ThreadRng,
    timer: Timer,
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
    pub fn start (id: u64, nodes: Vec<(u64, SocketAddr)>, log_path: Path) -> (Sender<ClientRequest<T>>, Receiver<Result<Vec<T>, IoError>>) {
        // TODO: Check index.
        let size = nodes.len();
        // Build the Hashmap lookups. We don't have a bimap :(
        let mut id_to_addr = HashMap::with_capacity(size);
        let mut addr_to_id = HashMap::with_capacity(size);
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
        let (res_send, res_recv) = channel::<Result<Vec<T>, IoError>>();
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
                persistent_state: PersistentState::new(0, log_path),
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
                    let rpr = match rpc {
                        RemoteProcedureCall::RequestVote(call) =>
                            self.handle_request_vote(call),
                        RemoteProcedureCall::AppendEntries(call) =>
                            self.handle_append_entries(call),
                    };
                    self.respond(source, rpr).unwrap();
                } else if let Ok(rpr) = json::decode::<RemoteProcedureResponse>(data) {
                    match rpr {
                        RemoteProcedureResponse::Accepted(response) =>
                            self.handle_accepted(response, source),
                        RemoteProcedureResponse::Rejected(response) =>
                            self.handle_rejected(response, source),
                    }
                }
            },
            Err(_) => (),                 // Nothing on the socket.
        }
        // If channel has data.
        match self.req_recv.try_recv() {
            Ok(request) => {              // Something in channel.
                let result = match request {
                    ClientRequest::IndexRange(request) =>
                        self.handle_index_range(request),
                    ClientRequest::AppendRequest(request) =>
                        self.handle_append_request(request),
                };
                self.res_send.send(result).unwrap();
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
            Follower     => self.follower_to_candidate(),
            Candidate(_) => self.reset_candidate(),
            _ => panic!("Should not campaign as a Leader!")
        };
        self.persistent_state.current_term += 1;
        self.persistent_state.voted_for = Some(self.own_id); // TODO: Is this correct?
        self.reset_timer();
        let mut status = Vec::with_capacity(self.id_to_addr.len());
        for (&id, &addr) in self.id_to_addr.clone().iter() {
            // Do it in the loop so we different Uuids.
            let (uuid, request) = RemoteProcedureCall::request_vote(
                self.persistent_state.current_term,
                self.persistent_state.voted_for.unwrap(), // TODO: Safe because we just set it. But correct
                self.volatile_state.last_applied,
                0); // TODO: Get this.
            status[id as usize] = Transaction { uuid: uuid, state: TransactionState::Polling };
            self.send(addr, request);
        }
        // The old map
        self.state = Candidate(status);
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
    fn handle_request_vote(&mut self, call: RequestVote) -> RemoteProcedureResponse {
        match self.state {
            Leader(_) => {
                // Re-assert leadership.
                // TODO: Might let someone take over if they have a higher term?
                assert!(self.leader_id.is_some());
                assert!(self.leader_id.unwrap() == self.own_id);
                RemoteProcedureResponse::reject(
                    call.uuid,
                    self.persistent_state.current_term,
                    self.leader_id.unwrap(), // Should be self.
                    self.persistent_state.last_index().unwrap(),
                    self.volatile_state.commit_index
                )
            },
            Follower => {
                let checks = [
                    self.persistent_state.current_term < call.term,
                    self.persistent_state.voted_for.is_none(),
                    self.volatile_state.last_applied < call.last_log_index,
                    true, // TODO: Is the last log term the same?
                ];
                let last_index = self.persistent_state.last_index().unwrap();
                match checks.iter().all(|&x| x) {
                    true  => RemoteProcedureResponse::accept(call.uuid, call.term,
                            self.persistent_state.last_index().unwrap(), self.volatile_state.commit_index),
                    false => {
                        // TODO: Handle various error cases.
                        // Decrement next_index
                        RemoteProcedureResponse::reject(call.uuid, call.term, self.leader_id.unwrap(),
                            self.persistent_state.last_index().unwrap() - 1, self.volatile_state.commit_index)
                    },
                }
            },
            Candidate(_) => {
                // TODO ???
                unimplemented!();
            }
        }
    }
    /// Handles an `AppendEntries` request from a caller.
    fn handle_append_entries(&mut self, call: AppendEntries<T>) -> RemoteProcedureResponse {
        match self.state {
            Leader(ref state) => {
                unimplemented!();
            },
            Follower => {
                // We need to append the entries to our log and respond.
                // Reject if:
                //  * term < current_term
                //  * Log does not contain entry at prev_log_index which matches the term.
                // If an existing entry conflicts with a new one:
                //  * Delete existing entry and all that follow it.
                // Append any entries not in the log.
                // If leader_commit > commit_index set commit_index to the min.
                let (node_prev_log_term, _) = self.persistent_state.retrieve_entry(call.prev_log_index)
                    .unwrap(); // TODO: Do better.
                if call.term < self.persistent_state.current_term {
                    RemoteProcedureResponse::reject(
                        call.uuid,
                        self.persistent_state.current_term,
                        self.leader_id.unwrap(),
                        self.persistent_state.last_index().unwrap(), // TODO Maybe wrong
                        self.volatile_state.commit_index
                    )
                } else if call.term != call.prev_log_term {
                    // prev_log_term is wrong.
                    // Delete it and all that follow it.
                    self.persistent_state.purge_from(call.prev_log_index)
                        .unwrap();
                    RemoteProcedureResponse::reject(
                        call.uuid,
                        self.persistent_state.current_term,
                        self.leader_id.unwrap(),
                        self.persistent_state.last_index().unwrap(), // TODO Maybe wrong.
                        self.volatile_state.commit_index,
                    )
                } else {
                    // Accept it!
                    self.persistent_state.append_entries(call.prev_log_index, call.prev_log_term, call.entries)
                        .unwrap();
                    RemoteProcedureResponse::accept(
                        call.uuid,
                        self.persistent_state.current_term,
                        self.persistent_state.last_index().unwrap(), // TODO Maybe wrong.
                        self.volatile_state.commit_index,
                    )
                }

            },
            Candidate(_) => {
                unimplemented!();
            },
        }
    }
    /// This function handles `RemoteProcedureResponse::Accepted` requests.
    fn handle_accepted(&mut self, response: Accepted, source: SocketAddr) {
        // Doubles as a check against outsiders.
        let source_id = match self.lookup_id(source) {
            Some(id) => *id,
            None => return,
        };
        match self.state {
            Leader(ref state) => {
                // Should be an AppendEntries request response.
                unimplemented!();
            },
            Follower => {
                // Must have been a response to our last AppendEntries request?
                unimplemented!();
            },
            Candidate(_) => {
                // Hopefully a response to one of our request_votes.
                let mut check_polls = false;
                if let Candidate(ref mut status) = self.state {
                    if status[source_id as usize].uuid == response.uuid {
                        // Set it.
                        status[source_id as usize].state = TransactionState::Accepted;
                        check_polls = true;
                    };
                }
                // Clone state because we'll replace it.
                if let (true, Candidate(status)) = (check_polls, self.state.clone()) {
                    // Do we have a majority?
                    let number_of_votes = status.iter().filter(|&transaction| {
                        transaction.state == TransactionState::Accepted
                    }).count();
                    if number_of_votes > self.addr_to_id.len() / 2 {
                        // Won election.
                        self.candidate_to_leader();
                    }
                }
            }
        }
    }
    /// This function handles `RemoteProcedureResponse::Rejected` requests.
    fn handle_rejected(&mut self, response: Rejected, source: SocketAddr) {
        // Doubles as a check against outsiders.
        let source_id = match self.lookup_id(source) {
            Some(id) => *id,
            None => return,
        };
        match self.state {
            Leader(ref state) => {
                // Should be an AppendEntries request response.
                unimplemented!();
            },
            Follower => {
                // Must have been a response to our last AppendEntries request?
                unimplemented!();
            },
            Candidate(_) => {
                // The vote has failed. This means there is most likely an existing leader.
                unimplemented!();
            }
        }

    }
    fn handle_append_request(&mut self, request: AppendRequest<T>) -> Result<Vec<T>, IoError> {
        match self.state {
            Leader(ref state) => {
                unimplemented!();
            },
            Follower => {
                unimplemented!();
            },
            Candidate(_) => {
                unimplemented!();
            },
        }
    }
    fn handle_index_range(&mut self, request: IndexRange) -> Result<Vec<T>, IoError> {
        unimplemented!();
    }
    ////////////
    // Timers //
    ////////////
    fn handle_timer(&mut self) {
        match self.state {
            Leader(ref state) => {
                // Send heartbeats.
                let last_index = self.persistent_state.last_index().unwrap();
                let (last_log_term, _) = self.persistent_state.retrieve_entry(last_index).unwrap();
                for (&id, &addr) in self.id_to_addr.iter() {
                    let (uuid, rpc) = RemoteProcedureCall::append_entries(
                        self.persistent_state.current_term,
                        self.leader_id.unwrap(),
                        self.volatile_state.commit_index,  // TODO: Check this.
                        last_log_term, // TODO: This will need to change.
                        Vec::<(u64, T)>::new(),
                        self.volatile_state.commit_index
                    );
                }
            },
            Follower => self.campaign(),
            Candidate(_) => self.campaign(),
        }
    }
    fn reset_timer(&mut self) {
        self.heartbeat = self.timer.oneshot(Duration::milliseconds(self.rng.gen_range::<i64>(HEARTBEAT_MIN, HEARTBEAT_MAX)));
    }
    //////////////////
    // Transmission //
    //////////////////
    // TODO: Improve "message" to not be &[u8]
    fn send(&mut self, node: SocketAddr, rpc: RemoteProcedureCall<T>) -> Result<(), std::old_io::IoError> {
        let encoded = json::encode::<RemoteProcedureCall<T>>(&rpc)
            .unwrap();
        self.socket.send_to(encoded.as_bytes(), node)
    }
    fn respond(&mut self, node: SocketAddr, rpr: RemoteProcedureResponse) -> Result<(), std::old_io::IoError> {
        let encoded = json::encode::<RemoteProcedureResponse>(&rpr)
            .unwrap();
        self.socket.send_to(encoded.as_bytes(), node)
    }
    ///////////////////
    // State Changes //
    ///////////////////
    /// Why are these in `RaftNode`? So we can use data available in the `RaftNode`.
    /// TODO: Would it be pleasant to spin these into `NodeState` itself?
    /// Called on heartbeat timeout.
    pub fn follower_to_candidate(&mut self) {
        // Need to increase term.
        self.persistent_state.current_term += 1;
        self.state = match self.state {
            Follower => Candidate(Vec::with_capacity(self.id_to_addr.len())),
            _ => panic!("Called follower_to_candidate() but was not Follower.")
        };
        self.leader_id = None;
    }
    /// Called when the Leader recieves information that they are not the leader.
    pub fn leader_to_follower(&mut self) {
        self.state = match self.state {
            Leader(_) => Follower,
            _ => panic!("Called leader_to_follower() but was not Leader.")
        };
    }
    /// Called when a Candidate successfully gets elected.
    pub fn candidate_to_leader(&mut self) {
        self.state = match self.state {
            Candidate(_) => Leader(LeaderState {
                next_index: Vec::with_capacity(self.id_to_addr.len()),
                match_index: Vec::with_capacity(self.id_to_addr.len())
            }),
            _ => panic!("Called candidate_to_leader() but was not Candidate.")
        };
        self.leader_id = Some(self.own_id);
    }
    /// Called when a candidate fails an election. Takes the new leader's ID.
    pub fn candidate_to_follower(&mut self, leader_id: u64) {
        self.state = match self.state {
            Candidate(_) => Follower,
            _ => panic!("Called candidate_to_follower() but was not Candidate.")
        };
        self.leader_id = Some(leader_id);
    }
    /// Called when a Candidate needs to hold another election.
    /// TODO: This is currently pointless, but will be meaningful when Candidates
    /// have data as part of their variant.
    pub fn reset_candidate(&mut self) {
        self.state = match self.state {
            Candidate(_) => Candidate(Vec::with_capacity(self.id_to_addr.len())),
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
#[derive(PartialEq, Eq, Clone)]
pub enum NodeState {
    Follower,
    Leader(LeaderState),
    Candidate(Vec<Transaction>),
}

#[derive(PartialEq, Eq, Clone)]
pub struct Transaction {
    uuid: Uuid,
    state: TransactionState,
}

/// Used to signify the state of of a Request/Response pair. This is only needed
/// on the original sender... not on the reciever.
#[derive(PartialEq, Eq, Copy, Clone)]
pub enum TransactionState {
    Polling,
    Accepted,
    Rejected,
}

/// Persistent state
/// **Must be updated to stable storage before RPC response.**
pub struct PersistentState<T: Encodable + Decodable + Send + Clone> {
    current_term: u64,
    voted_for: Option<u64>, // request_vote cares if this is `None`
    log: File,
}

impl<T: Encodable + Decodable + Send + Clone> PersistentState<T> {
    pub fn new(current_term: u64, log_path: Path) -> PersistentState<T> {
        PersistentState {
            current_term: current_term,
            voted_for: None,
            log: File::open(&log_path).unwrap(),
        }
    }
    pub fn append_entries(&mut self, prev_log_index: u64, prev_log_term: u64,
                      entries: Vec<(u64, T)>) -> Result<(), IoError> {
        unimplemented!()
    }
    /// Removes all entries from `from` to the last entry, inclusively.
    pub fn purge_from(&mut self, from: u64) -> Result<(), IoError> {
        unimplemented!()
    }
    pub fn retrieve_entries(&self, start: u64, end: u64) -> Result<Vec<(u64, T)>, IoError> {
        unimplemented!()
    }
    pub fn retrieve_entry(&self, entry: u64) -> Result<(u64, T), IoError> {
        unimplemented!()
    }
    pub fn last_index(&mut self) -> io::Result<u64> {
        self.log.seek(std::io::SeekFrom::End(0i64))
    }
}

/// Volatile state
#[derive(Copy)]
pub struct VolatileState {
    commit_index: u64,
    last_applied: u64
}

/// Leader Only
/// **Reinitialized after election.**
#[derive(PartialEq, Eq, Clone)]
pub struct LeaderState {
    next_index: Vec<u64>,
    match_index: Vec<u64>
}
