#![crate_name = "raft"]
#![crate_type="lib"]
#![doc(html_logo_url = "https://raw.githubusercontent.com/Hoverbear/raft/master/raft.png")]
#![doc(html_root_url = "https://hoverbear.github.io/raft/raft/")]

#![feature(
    collections,
    convert,
    io,
)]

extern crate rustc_serialize;
extern crate uuid;
extern crate rand;
#[macro_use] extern crate log;
extern crate mio;
pub mod interchange;
pub mod state;
pub mod store;

use std::{io, str, thread};
use std::collections::{HashMap, HashSet, VecDeque, BitSet};
use std::fmt::Debug;
use std::num::Int;
use std::net::SocketAddr;
use std::ops;
use std::sync::mpsc::{channel, Sender, Receiver};

use rand::{thread_rng, Rng, ThreadRng};
use rustc_serialize::{json, Encodable, Decodable};

// MIO
use mio::udp::UdpSocket;
use mio::{EventLoop, Handler, ReadHint, Token};

// Enums and variants.
use interchange::{ClientRequest, RemoteProcedureCall, RemoteProcedureResponse};
// Data structures.
use interchange::{AppendEntries, RequestVote};
use interchange::{AppendRequest, IndexRange};
use interchange::{Accepted, Rejected};
use state::{LeaderState, VolatileState};
use state::NodeState::{Leader, Follower, Candidate};
use state::{NodeState, TransactionState, Transaction};
use store::Store;

// The maximum size of the read buffer.
const BUFFER_SIZE: usize = 4096;
const HEARTBEAT_MIN: u64 = 150;
const HEARTBEAT_MAX: u64 = 300;

// MIO Tokens
const SOCKET:  Token = Token(0);
const TIMEOUT: Token = Token(1);

/// The term of a log entry.
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, RustcEncodable, RustcDecodable)]
pub struct Term(u64);
impl From<u64> for Term {
    fn from(val: u64) -> Term {
        Term(val)
    }
}
impl Into<u64> for Term {
    fn into(self) -> u64 {
        self.0
    }
}
impl ops::Add<u64> for Term {
    type Output = Term;
    fn add(self, rhs: u64) -> Term {
        Term(self.0.checked_add(rhs).expect("overflow while incrementing Term"))
    }
}
impl ops::Sub<u64> for Term {
    type Output = Term;
    fn sub(self, rhs: u64) -> Term {
        Term(self.0.checked_sub(rhs).expect("underflow while decrementing Term"))
    }
}

/// The index of a log entry.
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, RustcEncodable, RustcDecodable)]
pub struct LogIndex(u64);
impl From<u64> for LogIndex {
    fn from(val: u64) -> LogIndex {
        LogIndex(val)
    }
}
impl Into<u64> for LogIndex {
    fn into(self) -> u64 {
        self.0
    }
}
impl ops::Add<u64> for LogIndex {
    type Output = LogIndex;
    fn add(self, rhs: u64) -> LogIndex {
        LogIndex(self.0.checked_add(rhs).expect("overflow while incrementing LogIndex"))
    }
}
impl ops::Sub<u64> for LogIndex {
    type Output = LogIndex;
    fn sub(self, rhs: u64) -> LogIndex {
        LogIndex(self.0.checked_sub(rhs).expect("underflow while decrementing LogIndex"))
    }
}

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
///     SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11110 },
///     SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11111 },
///     SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11112 },
/// ];
/// // Create the nodes. You recieve a channel back to communicate on.
/// // TODO: We will probably change this and make it less awkward.
/// let (command_sender, result_reciever) = RaftNode::<String>::start(
///     nodes[0].clone(),
///     nodes.into_iter().collect(),
///     Path::new("/tmp/test0")
/// );
/// ```
///
/// > Note: The Raft paper suggests a minimum cluster size of 3 nodes.
pub struct RaftNode<T, S>
where T: Encodable + Decodable + Clone + Debug + Send + 'static,
      S: Store + Debug
{
    // Raft related.
    state: NodeState,
    store: S,
    volatile_state: VolatileState,
    // Auxilary Data.
    // TODO: This should probably be split off.
    // All nodes need to know this otherwise they can't effectively lead or hold elections.
    leader: Option<SocketAddr>,
    address: SocketAddr,
    cluster_members: HashSet<SocketAddr>,
    notice_requests: BitSet, // When append_request is issues this gets flagged for that commit.
    // Channels and Sockets
    socket: UdpSocket,
    res_send: Sender<io::Result<Vec<(Term, T)>>>,
    // State
    rng: ThreadRng,
}

type Reactor<T, S> = EventLoop<RaftNode<T, S>>;

/// The implementation of the RaftNode. In most use cases, creating a `RaftNode` should just be
/// done via `::new()`.
impl <T, S> RaftNode<T, S>
where T: Encodable + Decodable + Clone + Debug + Send + 'static,
      S: Store + Debug {

    /// Creates a new Raft node with the cluster members specified.
    ///
    /// # Arguments
    ///
    /// * `address` - The address of the new node.
    /// * `cluster_members` - The address of every cluster member, including all
    ///                       peer nodes and the new node.
    /// * `state_file` - The path the to the file in which node state will be persisted.
    pub fn start(address: SocketAddr,
                 cluster_members: HashSet<SocketAddr>,
                 store: S)
                 -> (mio::Sender<ClientRequest<T>>, Receiver<io::Result<Vec<(Term, T)>>>) {
        // Create an event loop
        let mut event_loop = Reactor::new().unwrap();
        // Setup the socket, make it not block.
        let socket = UdpSocket::bind(&address).unwrap();
        event_loop.register(&socket, SOCKET).unwrap();
        event_loop.timeout_ms(TIMEOUT, 250).unwrap();
        // Communication channels.
        let (res_send, res_recv) = channel::<io::Result<Vec<(Term, T)>>>();
        let req_send = event_loop.channel();
        // Fire up the thread.
        thread::Builder::new().name(format!("RaftNode {}", address)).spawn(move || {
            // Start up a RNG and Timer
            let rng = thread_rng();
            // Create the struct.
            let mut raft_node = RaftNode {
                state: Follower(VecDeque::new()),
                store: store,
                volatile_state: VolatileState {
                    commit_index: LogIndex(0),
                    last_applied: LogIndex(0),
                },
                leader: None,
                address: address,
                cluster_members: cluster_members,
                notice_requests: BitSet::new(),
                rng: rng,
                socket: socket,
                res_send: res_send,
                // Recieve reqs from event_loop
            };
            // This is the main, strongly typed state machine. It loops indefinitely for now. It
            // would be nice if this was event based.
            event_loop.run(&mut raft_node).unwrap();
        }).unwrap();
        (req_send, res_recv)
    }

    /// Returns the required number of nodes for a majority.
    fn majority(&self) -> u64 {
        (self.cluster_members.len() as u64 + 2) >> 1
    }

    /// When a `Follower`'s heartbeat times out it's time to start a campaign for election and
    /// become a `Candidate`. If successful, the `RaftNode` will transistion state into a `Leader`,
    /// otherwise it will become `Follower` again.
    /// This function accepts a `Follower` and transforms it into a `Candidate` then attempts to
    /// issue `RequestVote` remote procedure calls to other known nodes. If a majority come back
    /// accepted, it will become the leader.
    fn campaign(&mut self, reactor: &mut Reactor<T, S>) {
        // On conversion to Candidate:
        // * Increment current_term
        // * Vote for self
        // * Reset election timer
        // * Send RequestVote RPC to all other nodes.
        match self.state {
            Follower(_)  => self.follower_to_candidate(reactor),
            Candidate(_) => self.reset_candidate(),
            _ => panic!("Should not campaign as a Leader!")
        };
        self.store.set_voted_for(Some(self.address)).unwrap(); // TODO: Is this correct?
        self.reset_timer(reactor);
        // TODO: get rid of clone
        let status: HashMap<SocketAddr, Transaction> = self.cluster_members.clone().into_iter().map(|member| {
            // Do it in the loop so we different Uuids.
            let (uuid, request) = RemoteProcedureCall::request_vote(
                self.store.current_term().unwrap() + 1,
                self.volatile_state.last_applied,
                Term(0)); // TODO: Get this.
            if member == self.address {
                // Don't request of self.
                (member, Transaction { uuid: uuid, state: TransactionState::Accepted })
            } else {
                self.send(member.clone(), request).unwrap();
                (member, Transaction { uuid: uuid, state: TransactionState::Polling })
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
    ///   * Reply false if `term` < `currentTerm`.
    ///   * If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as
    ///     receiver’s log, grant vote.
    fn handle_request_vote(&mut self, reactor: &mut Reactor<T, S>, call: RequestVote, source: SocketAddr) -> RemoteProcedureResponse {
        if !self.cluster_members.contains(&source) {
            panic!("Received request vote request from unknown node {}.", source)
        };
        // Possible Outputs:
        info!("ID {}: FROM {} HANDLE request_vote", self.address, source);
        match self.state {
            Leader(_) => {
                // Re-assert leadership.
                // TODO: Might let someone take over if they have a higher term?
                assert!(self.leader.is_some());
                assert_eq!(self.leader.unwrap(), self.address);
                info!("ID {}:L: TO {} REJECT request_vote: Already leader", self.address, source);
                RemoteProcedureResponse::reject(
                    call.uuid,
                    self.store.current_term().unwrap(),
                    self.store.latest_index().unwrap(),
                    self.volatile_state.commit_index
                )
            },
            Follower(_) => {
                // We don't update the leader until we hear back again from them that they won.
                // But we should update voted_for
                let current_term = self.store.current_term().unwrap();
                let checks = [
                    current_term < call.term,
                    self.store.voted_for().unwrap().is_none(),
                    self.volatile_state.last_applied <= call.last_log_index,
                    true, // TODO: Is the last log term the same?
                ];
                let last_index = self.store.latest_index().unwrap();
                match checks.iter().all(|&x| x) {
                    true  => {
                        self.store.set_voted_for(Some(source)).unwrap();
                        self.reset_timer(reactor);
                        info!("ID {}:F: TO {} ACCEPT request_vote", self.address, source);
                        RemoteProcedureResponse::accept(call.uuid, current_term,
                            last_index, self.volatile_state.commit_index)
                    },
                    false => {
                        // TODO: Handle various error cases.
                        // Decrement next_index
                        let prev = {
                            let idx = last_index;
                            if idx == LogIndex(0) { idx } else { idx - 1 }
                        };
                        info!("ID {}:F: TO {} REJECT request_vote: Checks {:?}", self.address, source, checks);
                        RemoteProcedureResponse::reject(
                            call.uuid,
                            current_term,
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
                // if self.store.current_term().unwrap() < call.term {
                //     // TODO: I guess we should accept and become a follower?
                //     info!("ID {}:C: TO {} ACCEPT request_vote", self.address, source);
                //     self.candidate_to_follower(call.candidate_id, call.term);
                //     RemoteProcedureResponse::accept(call.uuid, call.term,
                //             self.store.latest_index().unwrap(), self.volatile_state.commit_index)
                // } else {
                // Reject it.
                info!("ID {}:C: TO {} REJECT request_vote: Am Candidate.", self.address, source);
                RemoteProcedureResponse::reject(call.uuid,
                                                call.term,
                                                self.store.latest_index().unwrap() - 1,
                                                self.volatile_state.commit_index)
                // }
            }
        }
    }

    /// Handles an `AppendEntries` request from a caller.
    fn handle_append_entries(&mut self, reactor: &mut Reactor<T, S>, call: AppendEntries<T>, source: SocketAddr) -> RemoteProcedureResponse {
        if !self.cluster_members.contains(&source) {
            panic!("Received append entries request from unknown node {}.", source)
        };
        info!("ID {}: FROM {} HANDLE append_entries", self.address, source);
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
                                info!("ID {}:L: FROM {} ACCEPT append_entries", self.address, source);
                                RemoteProcedureResponse::accept(
                                    call.uuid,
                                    self.store.current_term().unwrap(),
                                    self.volatile_state.commit_index, // TODO Maybe wrong.
                                    self.store.latest_index().unwrap())
                            },
                            Err(_) => {
                                info!("ID {}:L: FROM {} REJECT append_entries", self.address, source);
                                RemoteProcedureResponse::reject(
                                    call.uuid,
                                    self.store.current_term().unwrap(),
                                    self.volatile_state.commit_index, // TODO Maybe wrong.
                                    self.store.latest_index().unwrap())
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
                let last_index = self.store.latest_index().unwrap();
                let calculated_prev_log_term = {
                    match self.store.entry(last_index) {
                        Ok((term, _)) => term,
                        Err(_) => Term(0), // Means we don't even have that entry.
                    }
                };
                if call.term < self.store.current_term().unwrap() {
                    info!("ID {}:F: FROM {} REJECT append_entries: Term out of date {:?} < {:?}", self.address, source,
                        call.term,
                        self.store.current_term().unwrap()
                    );
                    let mut next = self.store.latest_index().unwrap().0;
                    if next == 0 { next = 1; } else { next += 1; }
                    RemoteProcedureResponse::reject(
                        call.uuid,
                        self.store.current_term().unwrap(),
                        self.volatile_state.commit_index, // TODO Maybe wrong.
                        LogIndex(next),
                    )
                } else if calculated_prev_log_term != call.prev_log_term {
                    info!("ID {}:F: FROM {} ACCEPT append_entries: prev_log_term is wrong {:?} != {:?}", self.address, source,
                        calculated_prev_log_term,
                        call.prev_log_term
                    );
                    // prev_log_term is wrong.
                    // Delete it and all that follow it.
                    self.leader = Some(source); // They're the leader now!
                    self.store.set_current_term(call.term).unwrap();
                    self.store.truncate_entries(call.prev_log_index).unwrap();
                    // Update Commit and notify if needed.
                    self.volatile_state.commit_index = call.leader_commit;
                    let mut next = self.store.latest_index().unwrap().0;
                    if next == 0 { next = 1; } else { next += 1; }
                    RemoteProcedureResponse::accept(
                        call.uuid,
                        self.store.current_term().unwrap(),
                        self.volatile_state.commit_index, // TODO Maybe wrong.
                        LogIndex(next), // Will be -1
                    )
                } else {
                    // Accept it!
                    info!("ID {}:F: FROM {} ACCEPT append_entries", self.address, source);
                    self.leader = Some(source); // They're the leader now!
                    self.store.set_current_term(call.term).unwrap();
                    self.volatile_state.commit_index = call.leader_commit;
                    self.store.append_entries_encode(call.prev_log_index, &call.entries).unwrap();
                    // If leader_commit > commit_index set commit_index to the min.
                    if call.leader_commit > self.volatile_state.commit_index {
                        self.volatile_state.commit_index = call.leader_commit;
                    }
                    if self.notice_requests.remove(&(self.volatile_state.commit_index.0 as usize)) {
                        // Need to notify.
                        info!("ID {}:F: RESPONDS TO CLIENT OK", self.address);
                        self.res_send.send(Ok(vec![])).unwrap();
                    }
                    self.reset_timer(reactor);
                    let mut next = self.store.latest_index().unwrap().0;
                    if next == 0 { next = 1; } else { next += 1; }
                    self.reset_timer(reactor);
                    RemoteProcedureResponse::accept(
                        call.uuid,
                        self.store.current_term().unwrap(),
                        call.prev_log_index, // TODO Maybe wrong.
                        LogIndex(next),
                    )
                }

            },
            Candidate(_) => {
                // If it has a higher term, accept it and become follower.
                // Otherwise, reject it.
                if call.term >= self.store.current_term().unwrap() {
                    info!("ID {}:C: FROM {} ACCEPT append_entries", self.address, source);
                    self.candidate_to_follower(reactor, source, call.term);
                    // Pass back into Follower.
                    self.handle_append_entries(reactor, call, source)
                } else {
                    info!("ID {}:C: FROM {} REJECT append_entries: Term not higher or equal.", self.address, source);
                    RemoteProcedureResponse::reject(
                        call.uuid,
                        self.store.current_term().unwrap(),
                        self.store.latest_index().unwrap(), // TODO Maybe wrong.
                        self.volatile_state.commit_index,
                    )
                }
            },
        }
    }

    /// This function handles `RemoteProcedureResponse::Accepted` requests.
    fn handle_accepted(&mut self, reactor: &mut Reactor<T, S>, response: Accepted, source: SocketAddr) {
        if !self.cluster_members.contains(&source) {
            panic!("Received accepted response from unknown node {}.", source)
        };
        info!("ID {}: FROM {} HANDLE accepted", self.address, source);
        let majority = self.majority() as usize;
        match self.state {
            Leader(ref mut state) => {
                // Should be an AppendEntries request response.
                state.set_match_index(source, response.match_index);
                state.set_next_index(source, response.next_index);
                if response.match_index > self.volatile_state.commit_index
                    && state.count_match_indexes(response.match_index) >= majority {
                    self.volatile_state.commit_index = response.match_index;
                    info!("ID {}:L: COMMITS {:?}", self.address, self.volatile_state.commit_index);
                    if self.notice_requests.remove(&(self.volatile_state.commit_index.0 as usize)) {
                        // Need to notify.
                        info!("ID {}:F: RESPONDS TO CLIENT OK", self.address);
                        self.res_send.send(Ok(vec![])).unwrap();
                    }
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
                        debug!("ID {}:F: FOUND MATCH", self.address);
                        let _ = queue.pop_front();
                    }
                };
            },
            Candidate(_) => {
                // Hopefully a response to one of our request_votes.
                let mut check_polls = false;
                if let Candidate(ref mut status) = self.state {
                    if status[&source].uuid == response.uuid {
                        // Set it.
                        debug!("ID {}:C: FROM {} MATCHED", self.address, source);
                        status.get_mut(&source).unwrap().state = TransactionState::Accepted;
                        check_polls = true;
                    } else {
                        debug!("ID {}:C: FROM {} NO MATCH", self.address, source);
                    }
                }
                // Clone state because we'll replace it.
                if let (true, Candidate(status)) = (check_polls, self.state.clone()) {
                    // Do we have a majority?
                    let number_of_votes = status.values().filter(|&transaction| {
                        transaction.state == TransactionState::Accepted
                    }).count();
                    info!("ID {}:C: VOTES {} NEEDS MORE THAN {}", self.address, number_of_votes, majority);
                    if number_of_votes > majority {  // +1 for itself.
                        // Won election.
                        self.candidate_to_leader(reactor);
                    }
                }
            }
        }
    }

    /// This function handles `RemoteProcedureResponse::Rejected` requests.
    fn handle_rejected(&mut self, reactor: &mut Reactor<T, S>, response: Rejected, source: SocketAddr) {
        if !self.cluster_members.contains(&source) {
            panic!("Received rejected response from unknown node {}.", source)
        };
        info!("ID {}: FROM {} HANDLE rejected", self.address, source);
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
                    info!("ID {}:F: FROM {} MATCHED: Rejected by leader.", self.address, source);
                    self.res_send.send(Err(io::Error::new(io::ErrorKind::Other, "Request was rejected by leader.", None))).unwrap();
                }
            },
            Candidate(_) => {
                // The vote has failed. This means there is most likely an existing leader.
                // Check the UUID and make sure it's fresh.
                if let Candidate(ref mut transactions) = self.state {
                    let transaction = transactions.get_mut(&source).unwrap();
                    if transaction.uuid == response.uuid {
                        transaction.state = TransactionState::Rejected;
                    }
                }
                info!("ID {}:C: REJECT FROM {}.", self.address, source);
                // The raft paper explicitly states that the candidate will only follow a different node
                // if it recieves an AppendEntries.
            }
        }

    }

    /// This is called when the consuming application issues an append request on it's channel.
    fn handle_append_request(&mut self, request: AppendRequest<T>) -> io::Result<()> {
        info!("ID {}: HANDLE append_request", self.address);
        match self.state {
            Leader(_) => {
                // Handle the request appropriately.
                // The client shouldn't need to worry about the term.
                let current_term = self.store.current_term().unwrap();
                let entries = request.entries
                                     .into_iter()
                                     .map(|x| (current_term, x))
                                     .collect::<Vec<(Term, T)>>();
                Ok(self.store.append_entries_encode(request.prev_log_index, &entries).unwrap())
                // Once a majority of nodes have commited this we will return a response.
            },
            Follower(_) => {
                let current_term = self.store.current_term().unwrap();
                // Make a request to the leader.
                match self.leader {
                    Some(leader) => {
                        // Can act.
                        let (uuid, rpc) = RemoteProcedureCall::append_entries(
                            self.store.current_term().unwrap(),
                            request.prev_log_index,
                            request.prev_log_term,
                            request.entries.into_iter().map(|x| (current_term, x)).collect(),
                            self.volatile_state.commit_index);
                        if let Follower(ref mut queue) = self.state {
                            queue.push_back(Transaction { uuid: uuid, state: TransactionState::Polling });
                        } else { unreachable!(); }
                        self.send(leader, rpc)
                            .map(|_| ())
                            // TODO: Update to be io::Result
                            .map_err(|_| {
                                info!("ID {}: RESPONDS ERROR", self.address);
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
    fn handle_index_range(&mut self, request: IndexRange) -> io::Result<Vec<(Term, T)>> {
        info!("ID {}: HANDLE index_range", self.address);
        let end = if request.end_index > self.volatile_state.commit_index {
            self.volatile_state.commit_index
        } else { request.end_index };

        Ok(self.store.entries_decode(request.start_index, end).unwrap())
    }

    ////////////
    // Timers //
    ////////////

    fn handle_timer(&mut self, reactor: &mut Reactor<T, S>) {
        info!("ID {}: HANDLE timer", self.address);
        match self.state {
            Leader(_) => {
                // Send heartbeats.
                // TODO: get rid of clone
                for &member in self.cluster_members.clone().iter() {
                    info!("ID {}: TO {} HEARTBEAT", self.address, member);
                    if member == self.address { continue }
                    let entries_they_need = {
                        if let Leader(ref mut state) = self.state {
                            let next_index = state.next_index(member);
                            let last_in_log = self.store.latest_index().unwrap();

                            self.store.entries_decode(next_index, last_in_log+1).unwrap() // Get them all.
                        } else { unreachable!() }
                    };
                    let (prev_log_term, prev_log_index) = {
                        if let Leader(ref mut state) = self.state {
                            let mut prev_log_index = state.next_index(member); // Want prev
                            if prev_log_index != LogIndex(0) { prev_log_index = prev_log_index - 1; }
                            let term = self.store.entry(prev_log_index)
                                .map(|(t, _)| t)
                                .unwrap_or(Term(0));
                            (term, prev_log_index)
                        } else { unreachable!() }
                    };
                    let (_, rpc) = RemoteProcedureCall::append_entries(
                        self.store.current_term().unwrap(),
                        prev_log_index,  // TODO: Check this.
                        prev_log_term, // TODO: This will need to change.
                        entries_they_need,
                        self.volatile_state.commit_index
                    );
                    self.send(member, rpc).unwrap();
                }
            },
            Follower(_) => self.campaign(reactor),
            Candidate(_) => self.campaign(reactor),
        }
        self.reset_timer(reactor);
    }

    fn reset_timer(&mut self, reactor: &mut Reactor<T, S>) {
        debug!("Node {} timer RESET", self.address);
        match self.state {
            Leader(_) => {
                reactor.timeout_ms(TIMEOUT, 250).unwrap();
            },
            Follower(_) | Candidate(_) => {
                reactor.timeout_ms(TIMEOUT, self.rng.gen_range::<u64>(HEARTBEAT_MIN, HEARTBEAT_MAX)).unwrap();
            },
        }
    }

    //////////////////
    // Transmission //
    //////////////////

    // TODO: Improve "message" to not be &[u8]
    fn send(&mut self, node: SocketAddr, rpc: RemoteProcedureCall<T>) -> io::Result<usize> {
        debug!("ID {}: SEND {:?}", self.address, rpc);
        let encoded = json::encode::<RemoteProcedureCall<T>>(&rpc)
            .unwrap();
        self.socket.send_to(encoded.as_bytes(), &node)
    }

    fn respond(&mut self, node: SocketAddr, rpr: RemoteProcedureResponse) -> io::Result<usize> {
        debug!("ID {}: RESPOND {:?}", self.address, rpr);
        let encoded = json::encode::<RemoteProcedureResponse>(&rpr)
            .unwrap();
        self.socket.send_to(encoded.as_bytes(), &node)
    }

    ///////////////////
    // State Changes //
    ///////////////////

    /// Why are these in `RaftNode`? So we can use data available in the `RaftNode`.
    /// TODO: Would it be pleasant to spin these into `NodeState` itself?
    /// Called on heartbeat timeout.
    fn follower_to_candidate(&mut self, reactor: &mut Reactor<T, S>) {
        // Need to increase term.
        debug!("ID {}: FOLLOWER -> CANDIDATE: Term {:?}", self.address, self.store.current_term().unwrap());
        self.state = match self.state {
            Follower(_) => Candidate(HashMap::new()),
            _ => panic!("Called follower_to_candidate() but was not Follower.")
        };
        self.leader = None;
        self.reset_timer(reactor)
    }

    /// Called when the Leader recieves information that they are not the leader.
    fn leader_to_follower(&mut self, reactor: &mut Reactor<T, S>) {
        info!("ID {}: LEADER -> FOLLOWER", self.address);
        self.state = match self.state {
            Leader(_) => Follower(VecDeque::new()),
            _ => panic!("Called leader_to_follower() but was not Leader.")
        };
        self.reset_timer(reactor)
    }

    /// Called when a Candidate successfully gets elected.
    fn candidate_to_leader(&mut self, reactor: &mut Reactor<T, S>) {
        info!("ID {}: CANDIDATE -> LEADER", self.address);
        self.state = match self.state {
            Candidate(_) => Leader(LeaderState::new(self.store.latest_index().unwrap())),
            _ => panic!("Called candidate_to_leader() but was not Candidate.")
        };
        self.store.inc_current_term().unwrap();
        self.leader = Some(self.address);
        // This will cause us to immediately heartbeat.
        self.handle_timer(reactor);
    }

    /// Called when a candidate fails an election. Takes the new leader's ID, term.
    /// *Note:* This does not reset the timer.
    fn candidate_to_follower(&mut self, reactor: &mut Reactor<T, S>, leader: SocketAddr, term: Term) {
        info!("ID {}: CANDIDATE -> FOLLOWER: Leader {}, Term {:?}", self.address, leader, term);
        self.state = match self.state {
            Candidate(_) => Follower(VecDeque::new()),
            _ => panic!("Called candidate_to_follower() but was not Candidate.")
        };
        self.store.set_current_term(term).unwrap();
        self.leader = Some(leader);
        self.reset_timer(reactor);
    }

    /// Called when a Candidate needs to hold another election.
    /// TODO: This is currently pointless, but will be meaningful when Candidates
    /// have data as part of their variant.
    fn reset_candidate(&mut self) {
        info!("ID {}: CANDIDATE RESET", self.address);
        self.state = match self.state {
            Candidate(_) => Candidate(HashMap::new()),
            _ => panic!("Called reset_candidate() but was not Candidate.")
        }
    }
}

impl<T, S> Handler for RaftNode<T, S>
where T: Encodable + Decodable + Clone + Debug + Send + 'static,
      S: Store + Debug {
    type Message = ClientRequest<T>;
    type Timeout = Token;

    /// A registered IoHandle has available data to read
    fn readable(&mut self, reactor: &mut Reactor<T, S>, token: Token, hint: ReadHint) {
        if token == SOCKET && hint == ReadHint::data() {
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
                        debug!("ID {}: FROM {:?} RECIEVED {:?}", self.address, source, rpc);
                        let rpr = match rpc {
                            RemoteProcedureCall::RequestVote(call) =>
                                self.handle_request_vote(reactor, call, source),
                            RemoteProcedureCall::AppendEntries(call) =>
                                self.handle_append_entries(reactor, call, source),
                        };
                        debug!("ID {}: TO {:?} RESPONDS {:?}", self.address, source, rpr);
                        self.respond(source, rpr).unwrap();
                    } else if let Ok(rpr) = json::decode::<RemoteProcedureResponse>(data) {
                        debug!("ID {}: FROM {:?} RECIEVED {:?}", self.address, source, rpr);
                        match rpr {
                            RemoteProcedureResponse::Accepted(response) =>
                                self.handle_accepted(reactor, response, source),
                            RemoteProcedureResponse::Rejected(response) =>
                                self.handle_rejected(reactor, response, source),
                        }
                    }
                },
                Err(_) => (),                 // Nothing on the socket.
            }
        } else {
            unimplemented!();
        }

    }

    /// A registered timer has expired
    fn timeout(&mut self, reactor: &mut Reactor<T, S>, token: Token) {
        // If timer has fired.
        self.handle_timer(reactor)
    }

    /// A message has been delivered.
    fn notify(&mut self, _reactor: &mut Reactor<T, S>, request: ClientRequest<T>) {
        // Something in channel.
        debug!("ID {}: GOT CLIENT REQUEST {:?}, LEADER: {:?}", self.address, request, self.leader);
        match request {
            ClientRequest::IndexRange(request) => {
                let result = self.handle_index_range(request);
                info!("ID {}:F: RESPONDS TO CLIENT {:?}", self.address, result);
                self.res_send.send(result).unwrap();
            },
            ClientRequest::AppendRequest(request) => {
                let target = request.prev_log_index + request.entries.len() as u64;
                let result = self.handle_append_request(request)
                    .map(|_| Vec::new());
                // If it's `Ok` we should respond once it's commited.
                if result.is_ok() {
                    self.notice_requests.insert(target.0 as usize);
                } else {
                    info!("ID {}:F: RESPONDS TO CLIENT {:?}", self.address, result);
                    self.res_send.send(result).unwrap();
                }
            },
        };
    }
}
