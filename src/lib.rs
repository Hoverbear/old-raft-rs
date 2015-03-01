#![crate_name = "raft"]
#![crate_type="lib"]
#![doc(html_logo_url = "https://raw.githubusercontent.com/Hoverbear/raft/master/raft.png")]
#![doc(html_root_url = "https://hoverbear.github.io/raft/raft/")]

#![feature(core)]
#![feature(io)]
#![feature(old_io)]
#![feature(old_path)]
#![feature(fs)]
#![feature(std_misc)]

extern crate "rustc-serialize" as rustc_serialize;
extern crate uuid;
extern crate rand;
#[macro_use] extern crate log;
pub mod interchange;
pub mod state;

use std::old_io::net::ip::SocketAddr;
use std::old_io::net::udp::UdpSocket;
use std::old_io::timer::Timer;
use std::time::Duration;
use std::old_io::IoError;
use std::io;
use std::thread;
use std::num::Float;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::str;
use std::collections::{HashMap, VecDeque};
use rustc_serialize::{json, Encodable, Decodable};
use rand::{thread_rng, Rng, ThreadRng};
use std::ops::IndexMut;
use std::fmt::Debug;

// Enums and variants.
use interchange::{ClientRequest, RemoteProcedureCall, RemoteProcedureResponse};
// Data structures.
use interchange::{AppendEntries, RequestVote};
use interchange::{AppendRequest, IndexRange};
use interchange::{Accepted, Rejected};
use state::{PersistentState, LeaderState, VolatileState};
use state::NodeState::{Leader, Follower, Candidate};
use state::{NodeState, TransactionState, Transaction};
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
/// A `RaftNode` acts as a replicated state machine. The server's role in the cluster depends on it's
/// own status. It will maintain both volatile state (which can be safely lost) and persistent
/// state (which must be carefully stored and kept safe).
///
/// Currently, the `RaftNode` API is not well defined. **We are looking for feedback and suggestions.**
///
/// You can create a cluster like so:
///
/// ```
/// #![feature(old_io)]
/// #![feature(old_path)]
/// use raft::RaftNode;
/// use std::old_io::net::ip::SocketAddr;
/// use std::old_io::net::ip::IpAddr::Ipv4Addr;
/// // Generally, your nodes will come from a file, or something.
/// let nodes = vec![
///     (0, SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11110 }),
///     (1, SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11111 }),
///     (2, SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11112 }),
/// ];
/// // Create the nodes. You recieve a channel back to communicate on.
/// // TODO: We will probably change this and make it less awkward.
/// let (command_sender, result_reciever) = RaftNode::<String>::start(
///     0,
///     nodes.clone(),
///     Path::new("/tmp/test0")
/// );
/// ```
///
/// > Note: The Raft paper suggests a minimum cluster size of 3 nodes.
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
    res_send: Sender<io::Result<Vec<(u64, T)>>>,
    // State
    rng: ThreadRng,
    timer: Timer,
}

/// The implementation of the RaftNode. In most use cases, creating a `RaftNode` should just be
/// done via `::new()`.
impl<T: Encodable + Decodable + Debug + Send + 'static + Clone> RaftNode<T> {
    /// Creates a new RaftNode with the neighbors specified. `id` should be a valid index into
    /// `nodes`. The idea is that you can use the same `nodes` on all of the clients and only vary
    /// `id`.
    pub fn start (id: u64, nodes: Vec<(u64, SocketAddr)>, log_path: Path) -> (Sender<ClientRequest<T>>, Receiver<io::Result<Vec<(u64, T)>>>) {
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
        let (res_send, res_recv) = channel::<io::Result<Vec<(u64, T)>>>();
        // Fire up the thread.
        thread::Builder::new().name(format!("Node {}", id)).spawn(move || {
            // Start up a RNG and Timer
            let mut rng = thread_rng();
            let mut timer = Timer::new().unwrap();
            // Create the struct.
            let mut raft_node = RaftNode {
                state: Follower(VecDeque::new()),
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
        }).unwrap();
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
                let data = str::from_utf8(&mut read_buffer[.. num_read]).unwrap();
                if let Ok(rpc) = json::decode::<RemoteProcedureCall<T>>(data) {
                    debug!("ID {}: FROM {:?} RECIEVED {:?}", self.own_id, source, rpc);
                    let rpr = match rpc {
                        RemoteProcedureCall::RequestVote(call) =>
                            self.handle_request_vote(call, source),
                        RemoteProcedureCall::AppendEntries(call) =>
                            self.handle_append_entries(call, source),
                    };
                    debug!("ID {}: TO {:?} RESPONDS {:?}", self.own_id, source, rpr);
                    self.respond(source, rpr).unwrap();
                } else if let Ok(rpr) = json::decode::<RemoteProcedureResponse>(data) {
                    debug!("ID {}: FROM {:?} RECIEVED {:?}", self.own_id, source, rpr);
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
        // Only check the channel if we can actually deal with a request.
        // (This is mostly a problem for Followers in initialization and Candidates)
        match self.state {
            Follower(_) | Leader(_) if self.leader_id != None => {
                // If channel has data.
                match self.req_recv.try_recv() {
                    Ok(request) => {          // Something in channel.
                        debug!("ID {}: GOT CLIENT REQUEST {:?}, LEADER: {:?}", self.own_id, request, self.leader_id);
                        match request {
                            ClientRequest::IndexRange(request) => {
                                let result = self.handle_index_range(request);
                                info!("ID {}:F: RESPONDS TO CLIENT {:?}", self.own_id, result);
                                self.res_send.send(result).unwrap();
                            },
                            // TODO: Redesign this...
                            ClientRequest::AppendRequest(request) => {
                                let result = self.handle_append_request(request)
                                    .map(|_| Vec::new());
                                    info!("ID {}:F: RESPONDS TO CLIENT {:?}", self.own_id, result);
                                self.res_send.send(result).unwrap();
                            },
                        };

                    },
                    Err(_) => (),               // Nothing in channel.
                }
            },
            _ => (),
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
    #[allow(dead_code)]
    fn lookup_addr(&self, id: u64) -> Option<&SocketAddr> {
        self.id_to_addr.get(&id)
    }
    fn lookup_id(&self, addr: SocketAddr) -> Option<&u64> {
        self.addr_to_id.get(&addr)
    }
    #[allow(dead_code)]
    fn other_nodes(&self) -> Vec<SocketAddr> {
        self.id_to_addr.iter().filter_map(|(&k, &v)| {
            if k == self.own_id {
                None
            } else {
                Some(v)
            }
        }).collect::<Vec<SocketAddr>>()
    }
    fn majority(&self) -> u64 {
        (self.addr_to_id.len() as f64 / 2.0).ceil() as u64
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
            Follower(_)  => self.follower_to_candidate(),
            Candidate(_) => self.reset_candidate(),
            _ => panic!("Should not campaign as a Leader!")
        };
        self.persistent_state.set_voted_for(Some(self.own_id)).unwrap(); // TODO: Is this correct?
        self.reset_timer();
        let status = vec![0; self.id_to_addr.len()].into_iter().enumerate().map(|(id, _)| {
            // Do it in the loop so we different Uuids.
            let (uuid, request) = RemoteProcedureCall::request_vote(
                self.persistent_state.get_current_term() + 1,
                self.persistent_state.get_voted_for().unwrap(), // TODO: Safe because we just set it. But correct
                self.volatile_state.last_applied,
                0); // TODO: Get this.
            if id as u64 == self.own_id {
                // Don't request of self.
                Transaction { uuid: uuid, state: TransactionState::Accepted }
            } else {
                let addr = self.id_to_addr[id as u64];
                self.send(addr, request).unwrap();
                Transaction { uuid: uuid, state: TransactionState::Polling }
            }
        }).collect();
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
    fn handle_request_vote(&mut self, call: RequestVote, source: SocketAddr) -> RemoteProcedureResponse {
        // Doubles as a check against outsiders.
        let source_id = match self.lookup_id(source) {
            Some(id) => *id,
            None => unimplemented!(),
        };
        // Possible Outputs:
        info!("ID {}: FROM {} HANDLE request_vote", self.own_id, source_id);
        match self.state {
            Leader(_) => {
                // Re-assert leadership.
                // TODO: Might let someone take over if they have a higher term?
                assert!(self.leader_id.is_some());
                assert_eq!(self.leader_id.unwrap(), self.own_id);
                info!("ID {}:L: TO {} REJECT request_vote: Already leader", self.own_id, source_id);
                RemoteProcedureResponse::reject(
                    call.uuid,
                    self.persistent_state.get_current_term(),
                    self.leader_id, // Should be self.
                    self.persistent_state.get_last_index(),
                    self.volatile_state.commit_index
                )
            },
            Follower(_) => {
                // We don't update the leader until we hear back again from them that they won.
                // But we should update voted_for
                let current_term = self.persistent_state.get_current_term();
                let checks = [
                    current_term < call.term,
                    self.persistent_state.get_voted_for().is_none(),
                    self.volatile_state.last_applied <= call.last_log_index,
                    true, // TODO: Is the last log term the same?
                ];
                let last_index = self.persistent_state.get_last_index();
                match checks.iter().all(|&x| x) {
                    true  => {
                        self.persistent_state.set_voted_for(Some(source_id)).unwrap();
                        self.reset_timer();
                        info!("ID {}:F: TO {} ACCEPT request_vote", self.own_id, source_id);
                        RemoteProcedureResponse::accept(call.uuid, current_term,
                            last_index, self.volatile_state.commit_index)
                    },
                    false => {
                        // TODO: Handle various error cases.
                        // Decrement next_index
                        let prev = {
                            let idx = last_index;
                            if idx == 0 { 0 } else { idx - 1 }
                        };
                        info!("ID {}:F: TO {} REJECT request_vote: Checks {:?}", self.own_id, source_id, checks);
                        RemoteProcedureResponse::reject(
                            call.uuid,
                            current_term,
                            self.leader_id,
                            prev,
                            self.volatile_state.commit_index
                        )
                    },
                }
            },
            Candidate(_) => {
                // From the Raft paper:
                // While waiting for votes, a candidate may receive an AppendEntries RPC from another server
                // claiming to be leader. If the leader’s term (included in its RPC) is at least as large
                // as the candidate’s current term, then the candidate recognizes the leader as legitimate
                // and returns to follower state. If the term in the RPC is smaller than the candidate’s
                // current term, then the candidate rejects the RPC and continues in candidate state.
                // ---
                // The Raft paper doesn't talk about this case at all.
                // I assume the node simply refuses.
                // At first, i had this acting like AppendEntries, but this seems to be causing voting deadlocks.
                // ---
                // if self.persistent_state.get_current_term() < call.term {
                //     // TODO: I guess we should accept and become a follower?
                //     info!("ID {}:C: TO {} ACCEPT request_vote", self.own_id, source_id);
                //     self.candidate_to_follower(call.candidate_id, call.term);
                //     RemoteProcedureResponse::accept(call.uuid, call.term,
                //             self.persistent_state.get_last_index(), self.volatile_state.commit_index)
                // } else {
                // Reject it.
                info!("ID {}:C: TO {} REJECT request_vote: Am Candidate.", self.own_id, source_id);
                RemoteProcedureResponse::reject(call.uuid, call.term, self.leader_id,
                    self.persistent_state.get_last_index() - 1, self.volatile_state.commit_index)
                // }
            }
        }
    }
    /// Handles an `AppendEntries` request from a caller.
    fn handle_append_entries(&mut self, call: AppendEntries<T>, source: SocketAddr) -> RemoteProcedureResponse {
        // Doubles as a check against outsiders.
        let source_id = match self.lookup_id(source) {
            Some(id) => *id,
            None => unimplemented!(),
        };
        info!("ID {}: FROM {} HANDLE append_entries", self.own_id, source_id);
        match self.state {
            Leader(_) => {
                // **This is a non-standard implementation detail.
                // If a follower recieves an append_request it will forward it to the leader.
                // The leader will treat this no differently than an append_request from it's client.
                // TODO: The terms get updated, not sure if that's the right approach.
                let updated_terms = call.entries.into_iter().map(|(_, v)| v).collect();
                match ClientRequest::append_request(call.prev_log_index, call.prev_log_term, updated_terms) {
                    ClientRequest::AppendRequest(transformed) => {
                        match self.handle_append_request(transformed) {
                            // TODO We shouldn't really report back errors...
                            Ok(_) => {
                                info!("ID {}:L: FROM {} ACCEPT append_entries", self.own_id, source_id);
                                RemoteProcedureResponse::accept(
                                    call.uuid,
                                    self.persistent_state.get_current_term(),
                                    self.volatile_state.commit_index, // TODO Maybe wrong.
                                    self.persistent_state.get_last_index())
                            },
                            Err(_) => {
                                info!("ID {}:L: FROM {} REJECT append_entries", self.own_id, source_id);
                                RemoteProcedureResponse::reject(
                                    call.uuid,
                                    self.persistent_state.get_current_term(),
                                    self.leader_id,
                                    self.volatile_state.commit_index, // TODO Maybe wrong.
                                    self.persistent_state.get_last_index())
                            },
                        }
                    },
                    _ => unreachable!()
                }
            },
            Follower(_) => {
                // We need to append the entries to our log and respond.
                // Reject if:
                //  * term < current_term
                //  * Log does not contain entry at prev_log_index which matches the term.
                // If an existing entry conflicts with a new one:
                //  * Delete existing entry and all that follow it.
                // Append any entries not in the log.
                // If leader_commit > commit_index set commit_index to the min.
                let last_index = self.persistent_state.get_last_index();
                let calculated_prev_log_term = {
                    match self.persistent_state.retrieve_entry(last_index) {
                        Ok((term, _)) => term,
                        Err(_) => 0, // Means we don't even have that entry.
                    }
                };
                if call.term < self.persistent_state.get_current_term() {
                    info!("ID {}:F: FROM {} REJECT append_entries: Term out of date {} < {}", self.own_id, source_id,
                        call.term,
                        self.persistent_state.get_current_term()
                    );
                    RemoteProcedureResponse::reject(
                        call.uuid,
                        self.persistent_state.get_current_term(),
                        self.leader_id,
                        self.volatile_state.commit_index, // TODO Maybe wrong.
                        self.persistent_state.get_last_index() + 1,
                    )
                } else if calculated_prev_log_term != call.prev_log_term {
                    info!("ID {}:F: FROM {} REJECT append_entries: prev_log_term is wrong {} != {}", self.own_id, source_id,
                        calculated_prev_log_term,
                        call.prev_log_term
                    );
                    // prev_log_term is wrong.
                    // Delete it and all that follow it.
                    self.leader_id = Some(source_id); // They're the leader now!
                    self.persistent_state.set_current_term(call.term).unwrap();
                    self.persistent_state.purge_from_index(call.prev_log_index)
                        .unwrap();
                    self.volatile_state.commit_index = call.leader_commit;
                    self.persistent_state.append_entries(call.prev_log_index, call.prev_log_term, call.entries)
                        .unwrap();
                    self.reset_timer();
                    RemoteProcedureResponse::accept(
                        call.uuid,
                        self.persistent_state.get_current_term(),
                        self.volatile_state.commit_index, // TODO Maybe wrong.
                        self.persistent_state.get_last_index(), // Will be -1
                    )
                } else {
                    // Accept it!
                    info!("ID {}:F: FROM {} ACCEPT append_entries", self.own_id, source_id);
                    self.leader_id = Some(source_id); // They're the leader now!
                    self.persistent_state.set_current_term(call.term).unwrap();
                    self.volatile_state.commit_index = call.leader_commit;
                    self.persistent_state.append_entries(call.prev_log_index, call.prev_log_term, call.entries)
                        .unwrap();
                    // If leader_commit > commit_index set commit_index to the min.
                    if call.leader_commit > self.volatile_state.commit_index {
                        self.volatile_state.commit_index = call.leader_commit;
                    }
                    self.reset_timer();
                    RemoteProcedureResponse::accept(
                        call.uuid,
                        self.persistent_state.get_current_term(),
                        call.prev_log_index, // TODO Maybe wrong.
                        self.persistent_state.get_last_index() + 1,
                    )
                }

            },
            Candidate(_) => {
                // If it has a higher term, accept it and become follower.
                // Otherwise, reject it.
                if call.term >= self.persistent_state.get_current_term() {
                    info!("ID {}:C: FROM {} ACCEPT append_entries", self.own_id, source_id);
                    self.candidate_to_follower(source_id, call.term);
                    // Pass back into Follower.
                    self.handle_append_entries(call, source)
                } else {
                    info!("ID {}:C: FROM {} REJECT append_entries: Term not higher or equal.", self.own_id, source_id);
                    RemoteProcedureResponse::reject(
                        call.uuid,
                        self.persistent_state.get_current_term(),
                        self.leader_id,
                        self.persistent_state.get_last_index(), // TODO Maybe wrong.
                        self.volatile_state.commit_index,
                    )
                }
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
        info!("ID {}: FROM {} HANDLE accepted", self.own_id, source_id);
        let majority = self.majority() as usize;
        match self.state {
            Leader(ref mut state) => {
                // Should be an AppendEntries request response.
                state.set_match_index(source_id, response.match_index);
                state.set_next_index(source_id, response.next_index);
                if response.match_index > self.volatile_state.commit_index
                    && state.count_match_indexes(response.match_index) >= majority {
                    self.volatile_state.commit_index = response.match_index;
                    info!("ID {}:L: COMMITS {}", self.own_id, self.volatile_state.commit_index);
                }
            },
            Follower(_) => {
                // Must have been a response to our last AppendEntries request?
                // We should ~not~ act on it, the Leader is the only one who
                // really tells us what to do.
                // TODO: Is it possible that a request_vote might fool us? Check here~
                let mut found = false;
                if let Follower(ref mut queue) = self.state {
                    match queue.front() {
                        Some(head) => {
                            if head.uuid == response.uuid {
                                found = true;
                            }
                        },
                        None => (),
                    }
                    if found == true {
                        debug!("ID {}:F: FOUND MATCH", self.own_id);
                        let _ = queue.pop_front();
                    }
                };
            },
            Candidate(_) => {
                // Hopefully a response to one of our request_votes.
                let mut check_polls = false;
                if let Candidate(ref mut status) = self.state {
                    if status[source_id as usize].uuid == response.uuid {
                        // Set it.
                        debug!("ID {}:C: FROM {} MATCHED", self.own_id, source_id);
                        status[source_id as usize].state = TransactionState::Accepted;
                        check_polls = true;
                    } else {
                        debug!("ID {}:C: FROM {} NO MATCH", self.own_id, source_id);
                    }
                }
                // Clone state because we'll replace it.
                if let (true, Candidate(status)) = (check_polls, self.state.clone()) {
                    // Do we have a majority?
                    let number_of_votes = status.iter().filter(|&transaction| {
                        transaction.state == TransactionState::Accepted
                    }).count();
                    info!("ID {}:C: VOTES {} NEEDS {}", self.own_id, number_of_votes, majority);
                    if number_of_votes > majority {  // +1 for itself.
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
        info!("ID {}: FROM {} HANDLE rejected", self.own_id, source_id);
        match self.state {
            Leader(_) => {
                // Should be an AppendEntries request response.
                // Great! Update `next_index` and `match_index`
                unimplemented!();
            },
            Follower(_) => {
                // Must have been a response to our last AppendEntries request?
                // Might also be a stale response to a request_vote.
                let mut found = false;
                if let Follower(ref mut queue) = self.state {
                    match queue.front() {
                        Some(head) => {
                            if head.uuid == response.uuid {
                                found = true;
                            }
                        },
                        None => (),
                    }
                    if found == true {
                        let _ = queue.pop_front();
                    }
                };
                if found == true {
                    info!("ID {}:F: FROM {} MATCHED: Rejected by leader.", self.own_id, source_id);
                    self.res_send.send(Err(io::Error::new(io::ErrorKind::Other, "Request was rejected by leader.", None))).unwrap();
                }
            },
            Candidate(_) => {
                // The vote has failed. This means there is most likely an existing leader.
                // Check the UUID and make sure it's fresh.
                if let Candidate(ref mut transactions) = self.state {
                    let ref mut transaction =  transactions.index_mut(&(source_id as usize));
                    if transaction.uuid == response.uuid {
                        transaction.state = TransactionState::Rejected;
                    }
                }
                info!("ID {}:C: REJECT FROM {}.", self.own_id, source_id);
                // The raft paper explicitly states that the candidate will only follow a different node
                // if it recieves an AppendEntries.
            }
        }

    }
    /// This is called when the consuming application issues an append request on it's channel.
    fn handle_append_request(&mut self, request: AppendRequest<T>) -> io::Result<()> {
        info!("ID {}: HANDLE append_request", self.own_id);
        match self.state {
            Leader(_) => {
                // Handle the request appropriately.
                // The client shouldn't need to worry about the term.
                let current_term = self.persistent_state.get_current_term();
                let entries = request.entries.into_iter()
                    .map(|x| (current_term, x))
                    .collect();
                self.persistent_state.append_entries(request.prev_log_index, request.prev_log_term, entries)
                // Once a majority of nodes have commited this we will return a response.
            },
            Follower(_) => {
                let current_term = self.persistent_state.get_current_term();
                // Make a request to the leader.
                match self.leader_id {
                    Some(id) => {
                        // Can act.
                        let (uuid, rpc) = RemoteProcedureCall::append_entries(
                            self.persistent_state.get_current_term(),
                            id,
                            request.prev_log_index,
                            request.prev_log_term,
                            request.entries.into_iter().map(|x| (current_term, x)).collect(),
                            self.volatile_state.commit_index);
                        if let Follower(ref mut queue) = self.state {
                            queue.push_back(Transaction { uuid: uuid, state: TransactionState::Polling });
                        } else { unreachable!(); }
                        let destination = self.id_to_addr[id];
                        self.send(destination, rpc)
                            // TODO: Update to be io::Result
                            .map_err(|_| {
                                info!("ID {}: RESPONDS ERROR", self.own_id);
                                io::Error::new(io::ErrorKind::Other, "TODO", None)
                            })
                    },
                    None     => {
                        // Need to wait... Store it? Same implementation as a candidate.
                        unreachable!()
                    },
                }
            },
            Candidate(_) => {
                // ???
                unreachable!();
            },
        }
    }
    /// This is called when the client requests a specific index range on it's channel.
    fn handle_index_range(&mut self, request: IndexRange) -> io::Result<Vec<(u64, T)>> {
        info!("ID {}: HANDLE index_range", self.own_id);
        let end = if request.end_index > self.volatile_state.commit_index {
            self.volatile_state.commit_index
        } else { request.end_index };
        let result = self.persistent_state.retrieve_entries(request.start_index, end);
        result
    }
    ////////////
    // Timers //
    ////////////
    fn handle_timer(&mut self) {
        info!("ID {}: HANDLE timer", self.own_id);
        match self.state {
            Leader(_) => {
                // Send heartbeats.
                // TODO: Don't clone.
                for (&id, &addr) in self.id_to_addr.clone().iter() {
                    info!("ID {}: TO {} HEARTBEAT", self.own_id, id);
                    if id == self.own_id { continue }
                    let entries_they_need = {
                        if let Leader(ref mut state) = self.state {
                            let next_index = state.next_index(id);
                            let last_in_log = self.persistent_state.get_last_index();

                            self.persistent_state.retrieve_entries(next_index, last_in_log+1) // Get them all.
                                .ok().expect("Failed to retrieve entries from log.")
                        } else { unreachable!() }
                    };
                    let (prev_log_term, prev_log_index) = {
                        if let Leader(ref mut state) = self.state {
                            let mut prev_log_index = state.next_index(id); // Want prev
                            if prev_log_index != 0 { prev_log_index -= 1; }
                            let term = self.persistent_state.retrieve_entry(prev_log_index)
                                .map(|(t, _)| t)
                                .unwrap_or(0);
                            (term, prev_log_index)
                        } else { unreachable!() }
                    };
                    let (_, rpc) = RemoteProcedureCall::append_entries(
                        self.persistent_state.get_current_term(),
                        self.leader_id.unwrap(),
                        prev_log_index,  // TODO: Check this.
                        prev_log_term, // TODO: This will need to change.
                        entries_they_need,
                        self.volatile_state.commit_index
                    );
                    self.send(addr, rpc).unwrap();
                }
            },
            Follower(_) => self.campaign(),
            Candidate(_) => self.campaign(),
        }
        self.reset_timer();
    }
    fn reset_timer(&mut self) {
        debug!("Node {} timer RESET", self.own_id);
        self.heartbeat = match self.state {
            Leader(_) => {
                self.timer.oneshot(Duration::milliseconds(HEARTBEAT_MIN))
            },
            Follower(_) | Candidate(_) => {
                self.timer.oneshot(Duration::milliseconds(self.rng.gen_range::<i64>(HEARTBEAT_MIN, HEARTBEAT_MAX)))
            },
        }
    }
    //////////////////
    // Transmission //
    //////////////////
    // TODO: Improve "message" to not be &[u8]
    fn send(&mut self, node: SocketAddr, rpc: RemoteProcedureCall<T>) -> Result<(), std::old_io::IoError> {
        debug!("ID {}: SEND {:?}", self.own_id, rpc);
        let encoded = json::encode::<RemoteProcedureCall<T>>(&rpc)
            .unwrap();
        self.socket.send_to(encoded.as_bytes(), node)
    }
    fn respond(&mut self, node: SocketAddr, rpr: RemoteProcedureResponse) -> Result<(), std::old_io::IoError> {
        debug!("ID {}: RESPOND {:?}", self.own_id, rpr);
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
    fn follower_to_candidate(&mut self) {
        // Need to increase term.
        debug!("ID {}: FOLLOWER -> CANDIDATE: Term {}", self.own_id, self.persistent_state.get_current_term());
        self.state = match self.state {
            Follower(_) => Candidate(Vec::with_capacity(self.id_to_addr.len())),
            _ => panic!("Called follower_to_candidate() but was not Follower.")
        };
        self.leader_id = None;
        self.reset_timer()
    }
    /// Called when the Leader recieves information that they are not the leader.
    fn leader_to_follower(&mut self) {
        info!("ID {}: LEADER -> FOLLOWER", self.own_id);
        self.state = match self.state {
            Leader(_) => Follower(VecDeque::new()),
            _ => panic!("Called leader_to_follower() but was not Leader.")
        };
        self.reset_timer()
    }
    /// Called when a Candidate successfully gets elected.
    fn candidate_to_leader(&mut self) {
        info!("ID {}: LEADER -> LEADER", self.own_id);
        self.state = match self.state {
            Candidate(_) => Leader(LeaderState::new(self.persistent_state.get_last_index())),
            _ => panic!("Called candidate_to_leader() but was not Candidate.")
        };
        self.persistent_state.inc_current_term();
        self.leader_id = Some(self.own_id);
        // This will cause us to immediately heartbeat.
        self.handle_timer();
    }
    /// Called when a candidate fails an election. Takes the new leader's ID, term.
    fn candidate_to_follower(&mut self, leader_id: u64, term: u64) {
        info!("ID {}: CANDIDATE -> FOLLOWER: Leader {}, Term {}", self.own_id, leader_id, term);
        self.state = match self.state {
            Candidate(_) => Follower(VecDeque::new()),
            _ => panic!("Called candidate_to_follower() but was not Candidate.")
        };
        self.persistent_state.set_current_term(term).unwrap();
        self.leader_id = Some(leader_id);
        self.reset_timer();
    }
    /// Called when a Candidate needs to hold another election.
    /// TODO: This is currently pointless, but will be meaningful when Candidates
    /// have data as part of their variant.
    fn reset_candidate(&mut self) {
        info!("ID {}: CANDIDATE RESET", self.own_id);
        self.state = match self.state {
            Candidate(_) => Candidate(Vec::with_capacity(self.id_to_addr.len())),
            _ => panic!("Called reset_candidate() but was not Candidate.")
        }
    }
}
