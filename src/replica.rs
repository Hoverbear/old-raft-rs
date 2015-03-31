use std::collections::HashSet;
use std::{cmp, fmt};
use std::net::SocketAddr;

use {LogIndex, Term};
use messages_capnp::{
    append_entries_request,
    append_entries_response,
    client_request,
    client_response,
    request_vote_request,
    request_vote_response,
};
use state::{ReplicaState, LeaderState, CandidateState, FollowerState};
use state_machine::StateMachine;
use store::Store;

/// Should issue requests to all nodes.
pub struct Broadcast;

/// Should respond to the sender.
pub struct Emit;

/// A replica of a Raft distributed state machine. A Raft replica controls a client state machine,
/// to which it applies commands in a globally consistent order.
pub struct Replica<S, M> {
    /// The network address of this `Replica`.
    addr: SocketAddr,
    /// The network addresses of the other `Replica`s in the Raft cluster.
    peers: HashSet<SocketAddr>,

    /// The persistent log store.
    store: S,
    /// The client state machine to which client commands will be applied.
    state_machine: M,

    /// Index of the latest entry known to be committed.
    commit_index: LogIndex,
    /// Index of the latest entry applied to the state machine.
    last_applied: LogIndex,
    /// Whether this replica should campaign after the next election timeout.
    should_campaign: bool,

    /// The current state of the `Replica` (`Leader`, `Candidate`, or `Follower`).
    state: ReplicaState,
    /// State necessary while a `Leader`. Should not be used otherwise.
    leader_state: LeaderState,
    /// State necessary while a `Candidate`. Should not be used otherwise.
    candidate_state: CandidateState,
    /// State necessary while a `Follower`. Should not be used otherwise.
    follower_state: FollowerState,
}

impl <S, M> Replica<S, M> where S: Store, M: StateMachine {

    pub fn new(addr: SocketAddr,
               peers: HashSet<SocketAddr>,
               store: S,
               state_machine: M)
               -> Replica<S, M> {
        let leader_state = LeaderState::new(store.latest_log_index().unwrap(), &peers);
        Replica {
            addr: addr,
            peers: peers,
            store: store,
            state_machine: state_machine,
            commit_index: LogIndex(0),
            last_applied: LogIndex(0),
            should_campaign: true,
            state: ReplicaState::Follower,
            leader_state: leader_state,
            candidate_state: CandidateState::new(),
            follower_state: FollowerState::new(),
        }
    }

    /// Apply an append entries request to the Raft replica.
    pub fn append_entries_request(&mut self,
                                  from: SocketAddr,
                                  request: append_entries_request::Reader,
                                  mut response: append_entries_response::Builder) -> Option<Emit> {
        assert!(self.peers.contains(&from), "Received append entries request from unknown node {}.", from);
        debug!("{:?}: AppendEntriesRequest from Replica({})", self, from);

        let leader_term = Term(request.get_term());
        let current_term = self.store.current_term().unwrap();

        if leader_term < current_term {
            response.set_term(current_term.into());
            response.set_stale_term(());
            return None;
        }

        let leader_commit_index = LogIndex::from(request.get_leader_commit());
        assert!(self.commit_index <= leader_commit_index);
        self.commit_index = leader_commit_index;

        match self.state {
            ReplicaState::Follower => {
                if current_term < leader_term {
                    self.store.set_current_term(leader_term).unwrap();
                    response.set_term(leader_term.into());
                    self.follower_state.set_leader(from);
                } else {
                    response.set_term(current_term.into());
                }

                let leader_prev_log_index = LogIndex(request.get_prev_log_index());
                let leader_prev_log_term = Term(request.get_prev_log_term());

                let latest_log_index = self.store.latest_log_index().unwrap();
                if latest_log_index < leader_prev_log_index {
                    response.set_inconsistent_prev_entry(());
                } else {
                    println!("{:?}", leader_prev_log_index);
                    let existing_term = if leader_prev_log_index == LogIndex::from(0) {
                        Term::from(0)
                    } else {
                        self.store.entry(leader_prev_log_index).unwrap().0
                    };

                    if existing_term != leader_prev_log_term {
                        response.set_inconsistent_prev_entry(());
                    } else {
                        let entries = request.get_entries().unwrap();
                        let num_entries = entries.len();
                        if num_entries > 0 {
                            let mut entries_vec = Vec::with_capacity(num_entries as usize);
                            for i in 0..num_entries {
                                entries_vec.push((leader_term, entries.get(i).unwrap()));
                            }
                            self.store.append_entries(leader_prev_log_index + 1, &entries_vec).unwrap();
                        }
                        let latest_log_index = leader_prev_log_index + num_entries as u64;
                        // We are matching the leaders log up to and including `latest_log_index`.
                        self.apply_commits_until(latest_log_index);
                        response.set_success(latest_log_index.into());
                    }
                }
                return Some(Emit) // Need to respond to the leader.
            },
            ReplicaState::Candidate => {
                // recognize the new leader, return to follower state, and apply the entries
                self.transition_to_follower(leader_term, from.clone());
                return self.append_entries_request(from, request, response)
            },
            ReplicaState::Leader => {
                if leader_term == current_term {
                    // The single leader-per-term invariant is broken; there is a bug in the Raft
                    // implementation.
                    panic!("ID {}: peer leader {} with matching term {:?} detected.",
                           self.addr, from, current_term);
                }

                // recognize the new leader, return to follower state, and apply the entries
                self.transition_to_follower(leader_term, from.clone());
                return self.append_entries_request(from, request, response)
            },
        }
    }

    /// Apply an append entries response to the Raft replica.
    ///
    /// The provided message may be initialized with a new AppendEntries request to send back to
    /// the follower in the case that the follower's log is behind.
    #[must_use]
    pub fn append_entries_response(&mut self,
                                   from: SocketAddr,
                                   response: append_entries_response::Reader,
                                   mut message: append_entries_request::Builder) -> Option<Emit> {
        assert!(self.peers.contains(&from), "{:?} received AppendEntries response from unknown peer {}.", self, from);
        debug!("{:?}: AppendEntriesResponse from Replica({})", self, from);

        let local_term = self.store.current_term().unwrap();
        let responder_term = Term::from(response.get_term());
        let local_latest_log_index = self.store.latest_log_index().unwrap();

        if local_term < responder_term {
            // Responder has a higher term number. Relinquish leader position (if it is held), and
            // return to follower status.

            // The responder is not necessarily the leader, but it is somewhat likely, so we will
            // use it as the leader hint.
            self.transition_to_follower(responder_term, from);
            return None
        } else if local_term > responder_term {
            // Responder is responding to an AppendEntries request from a different term. Ignore
            // the response.
            return None
        }

        let mut send_message = false;
        if self.is_leader() {
            match response.which() {
                Ok(append_entries_response::Which::Success(follower_latest_log_index)) => {
                    let follower_latest_log_index = LogIndex::from(follower_latest_log_index);
                    assert!(follower_latest_log_index <= local_latest_log_index);
                    self.leader_state.set_next_index(from.clone(), follower_latest_log_index + 1);
                    self.leader_state.set_match_index(from.clone(), follower_latest_log_index);
                    self.advance_commit_index();
                    send_message = local_latest_log_index > follower_latest_log_index;
                }
                Ok(append_entries_response::Which::InconsistentPrevEntry(..)) => {
                    let next_index = self.leader_state.next_index(&from) - 1;
                    self.leader_state.set_next_index(from, next_index);
                    send_message = true;
                }
                Ok(append_entries_response::Which::StaleTerm(..)) => {
                    // This case is handled above by checking local_term against
                    // responder_term.
                    unreachable!("{:?}: AppendEntries.StaleTerm response from Replica({}). local term: {:?}, responder term: {:?}.",
                    self, from, local_term, responder_term);
                }
                Ok(append_entries_response::Which::InternalError(error_result)) => {
                    let error = error_result.unwrap_or("[unable to decode internal error]");
                    warn!("{:?}: AppendEntries.InternalError response from Replica({}): {}",
                           self, from, error);
                    // TODO: should we retry?
                }
                Err(..) => {
                    warn!("{:?}: ignoring unknown AppendEntries response from Replica({}): incompatible protocol version.",
                           self, from);
                }
            }
        } else {
            // This is not allowed in the Raft protocol, since we only send AppendEntries
            // requests while in the Leader state, so we should never receive AppendEntries
            // responses from the current term while not the Leader (It's impossible to go
            // from Leader to Candidate or Follower states without increasing the term, and
            // we have already checked that local_term == responder_term).
            unreachable!("{:?}: received AppendEntries response from Replica({}) while in state {:?}.",
                          self.addr, from, self.state);
        }

        if send_message {
            let next_index = self.leader_state.next_index(&from);

            if next_index <= local_latest_log_index {
                let prev_log_index = next_index - 1;
                let (prev_log_term, _) = self.store.entry(prev_log_index).unwrap();

                message.set_term(local_term.into());
                message.set_prev_log_index(prev_log_index.into());
                message.set_prev_log_term(prev_log_term.into());
                message.set_leader_commit(self.commit_index.into());

                let from_index = Into::<u64>::into(next_index);
                let until_index = Into::<u64>::into(local_latest_log_index) + 1;
                let mut entries = message.init_entries((until_index - from_index) as u32);
                for (n, index) in (from_index..until_index).enumerate() {
                    entries.set(n as u32, self.store.entry(LogIndex::from(index)).unwrap().1);
                }
                Some(Emit)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Apply a request vote request to the Raft replica.
    pub fn request_vote_request(&mut self,
                                candidate: SocketAddr,
                                request: request_vote_request::Reader,
                                mut response: request_vote_response::Builder) -> Option<Emit> {
        assert!(self.peers.contains(&candidate), "Received request vote request from unknown node {}.", candidate);
        debug!("{:?}: RequestVoteRequest from Replica({})", self, candidate);

        let candidate_term = Term(request.get_term());
        let candidate_index = LogIndex(request.get_last_log_index());
        let local_term = self.store.current_term().unwrap();
        let local_index = self.store.latest_log_index().unwrap();

        if candidate_term > local_term {
            self.store.set_current_term(candidate_term).unwrap();
            response.set_term(candidate_term.into());
        } else {
            response.set_term(local_term.into());
        }

        if candidate_term < local_term {
            response.set_stale_term(());
        } else if candidate_index < local_index {
            response.set_inconsistent_log(());
        } else {
            match self.store.voted_for().unwrap() {
                None => {
                    self.store.set_voted_for(candidate).unwrap();
                    response.set_granted(());
                    self.should_campaign = false;
                },
                Some(voted_for) if voted_for == candidate => {
                    response.set_granted(());
                    self.should_campaign = false;
                },
                _ => {
                    response.set_already_voted(());
                },
            }
        }
        Some(Emit) // Always need to send.
    }

    /// Apply a request vote response to the Raft replica.
    ///
    /// # Return
    ///
    /// Returns `Some(())` if the provided AppendEntriesRequest should be sent to every peer cluster
    /// member.
    pub fn request_vote_response(&mut self, from: SocketAddr,
                                 response: request_vote_response::Reader,
                                 message: append_entries_request::Builder) -> Option<Broadcast> {
        assert!(self.peers.contains(&from), "Received request vote response from unknown node {}.", from);
        debug!("{:?}: RequestVoteResponse from Replica({})", self, from);

        let local_term = self.store.current_term().unwrap();
        let voter_term = Term::from(response.get_term());

        let majority = self.majority();
        let mut transition_to_leader = false;

        if local_term < voter_term {
            // Responder has a higher term number. The election is compromised; abandon it and
            // revert to follower state with the updated term number. Any further responses we
            // receive from this election term will be ignored because the term will be outdated.

            // The responder is not necessarily teh leader, but it is somewhat likely, so we will
            // use it as the leader hint.
            self.transition_to_follower(voter_term, from);
        } else if local_term > voter_term {
            // Ignore this message; it came from a previous election cycle.
        } else if self.is_candidate() {
            // A vote was recieved!
            if let Ok(request_vote_response::Granted(_)) = response.which() {
                self.candidate_state.record_vote(from);
                if self.candidate_state.count_votes() >= majority {
                    // The election was won!
                    transition_to_leader = true;
                }
            }
        }

        if transition_to_leader {
            // Need to transition and broadcast the first heartbeat.
            self.transition_to_leader(message);
            Some(Broadcast)
        } else {
            None
        }
    }

    /// Apply a client append request to the Raft replica.
    pub fn client_append(&mut self, from: SocketAddr, entry: &[u8],
                         message: client_response::Builder) -> Option<Broadcast> {
        debug!("{:?}: Append from Client({})", self, from);
        unimplemented!();
        Some(Broadcast)
    }

    /// Refreshes the client with the leader address.
    pub fn client_leader_refresh(&mut self, from: SocketAddr,
                                 message: client_response::Builder) -> Option<Emit> {
        debug!("{:?}: LeaderRefresh from Client({})", self, from);
        unimplemented!();
        Some(Emit)
    }

    /// Trigger a heartbeat timeout on the Raft replica.
    ///
    /// The provided AppendEntriesRequest builder may be initialized with a message to send to each
    /// cluster peer.
    pub fn heartbeat_timeout(&mut self, mut message: append_entries_request::Builder) -> Option<Broadcast> {
        debug!("{:?}: HeartbeatTimeout", self);
        if self.is_leader() {
            // Send a heartbeat
            message.set_term(self.store.current_term().unwrap().into());
            message.set_prev_log_index(self.store.latest_log_index().unwrap().into());
            message.set_prev_log_term(self.store.latest_log_term().unwrap().into());
            message.set_leader_commit(self.commit_index.into());
            message.init_entries(0);
            Some(Broadcast)
        } else { None }
    }

    /// Trigger an election timeout on the Raft replica.
    ///
    /// The provided RequestVoteRequest builder may be initialized with a message to send to each
    /// cluster peer.
    pub fn election_timeout(&mut self, message: request_vote_request::Builder) -> Option<Broadcast> {
        debug!("{:?}: ElectionTimeout", self);
        if self.should_campaign && !self.is_leader() {
            if self.peers.is_empty() {
                // Solitary replica special case; jump straight to leader status
                assert!(self.is_follower());
                assert!(self.store.voted_for().unwrap().is_none());
                self.store.inc_current_term().unwrap();
                self.store.set_voted_for(self.addr).unwrap();
                let latest_log_index = self.store.latest_log_index().unwrap();
                self.state = ReplicaState::Leader;
                self.leader_state.reinitialize(latest_log_index);
                None
            } else {
                self.transition_to_candidate(message);
                Some(Broadcast)
            }
        } else {
            self.should_campaign = true;
            None
        }
    }

    /// Transition this Replica to Leader state.
    ///
    /// The provided AppendEntriesRequest builder will be initialized with a message to send to each
    /// cluster peer.
    fn transition_to_leader(&mut self, mut message: append_entries_request::Builder) -> Option<Broadcast> {
        info!("{:?}: Transition to Leader", self);
        let current_term = self.store.current_term().unwrap();
        let latest_log_index = self.store.latest_log_index().unwrap();
        let latest_log_term = self.store.latest_log_term().unwrap();
        self.state = ReplicaState::Leader;
        self.leader_state.reinitialize(latest_log_index);

        message.set_term(current_term.into());
        message.set_prev_log_index(latest_log_index.into());
        message.set_prev_log_term(latest_log_term.into());
        message.set_leader_commit(self.commit_index.into());
        Some(Broadcast)
    }

    /// Transition this Replica to Candidate state.
    ///
    /// The provided RequestVoteRequest message will be initialized with a message to send to each
    /// cluster peer.
    fn transition_to_candidate(&mut self, mut message: request_vote_request::Builder) -> Option<Broadcast> {
        info!("{:?}: Transition to Candidate", self);
        self.store.inc_current_term().unwrap();
        self.store.set_voted_for(self.addr).unwrap();
        self.state = ReplicaState::Candidate;
        self.candidate_state.clear();
        self.candidate_state.record_vote(self.addr.clone());

        let current_term = self.store.current_term().unwrap();
        let latest_index = self.store.latest_log_index().unwrap();
        let latest_term = self.store.latest_log_term().unwrap();

        message.set_term(current_term.into());
        message.set_last_log_index(latest_index.into());
        message.set_last_log_term(latest_term.into());
        Some(Broadcast)
    }

    /// Advance the commit index and apply committed entries to the state machine, if possible.
    fn advance_commit_index(&mut self) {
        assert!(self.is_leader());
        let majority = self.majority();
        while self.leader_state.count_match_indexes(self.commit_index + 1) >= majority {
            self.commit_index = self.commit_index + 1;
        }

        // Apply all committed but unapplied entries
        while self.last_applied < self.commit_index {
            let (_, entry) = self.store.entry(self.last_applied + 1).unwrap();
            self.state_machine.apply(entry).unwrap();
            self.last_applied = self.last_applied + 1;
        }
    }

    /// Apply all committed but unapplied log entries up to and including the provided index.
    fn apply_commits_until(&mut self, until: LogIndex) {
        let index = cmp::min(self.commit_index, until);
        if index == LogIndex::from(0) { return };
        while self.last_applied <= index {
            let (_, entry) = self.store.entry(self.last_applied + 1).unwrap();
            self.state_machine.apply(entry).unwrap();
            self.last_applied = self.last_applied + 1;
        }
    }

    /// Transition to follower state with the provided term. The `voted_for` field will be reset.
    /// The provided leader hint will replace the last known leader.
    fn transition_to_follower(&mut self, term: Term, leader: SocketAddr) {
        info!("{:?}: Transition to Follower", self);
        self.store.set_current_term(term).unwrap();
        self.state = ReplicaState::Follower;
        self.follower_state.set_leader(leader);
    }

    /// Returns `true` if the replica is in the Leader state.
    fn is_leader(&self) -> bool {
        self.state == ReplicaState::Leader
    }

    /// Returns `true` if the replica is in the Follower state.
    fn is_follower(&self) -> bool {
        self.state == ReplicaState::Follower
    }

    /// Returns `true` if the replica is in the Candidate state.
    fn is_candidate(&self) -> bool {
        self.state == ReplicaState::Candidate
    }

    /// Returns the address of the replica.
    fn addr(&self) -> &SocketAddr {
        &self.addr
    }

    /// Returns the current term of the replica.
    fn current_term(&self) -> Term {
        self.store.current_term().unwrap()
    }

    /// Get the cluster quorum majority size.
    fn majority(&self) -> usize {
        let peers = self.peers.len();
        let cluster_members = peers.checked_add(1).expect(&format!("unable to support {} cluster members", peers));
        (cluster_members >> 1) + 1
    }
}

impl <S, M> fmt::Debug for Replica<S, M> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Replica({})", self.addr)
    }
}

#[cfg(test)]
mod test {

    use std::collections::HashSet;
    use std::net::{SocketAddr};
    use std::sync::mpsc;
    use std::str::FromStr;

    use capnp::{MallocMessageBuilder, MessageBuilder};

    use messages_capnp::{
        append_entries_request,
        append_entries_response,
        request_vote_request,
        request_vote_response,
    };
    use replica::Replica;
    use state_machine::ChannelStateMachine;
    use store::MemStore;
    use Term;

    type TestReplica = Replica<MemStore, ChannelStateMachine>;

    fn new_cluster(size: u16) -> Vec<(TestReplica, mpsc::Receiver<Vec<u8>>)> {
        // the actual port does not matter here since they won't be bound
        let addrs: HashSet<SocketAddr> =
            (0..size).map(|port| FromStr::from_str(&format!("127.0.0.1:{}", port)).unwrap()).collect();

        addrs.iter().map(|addr| {
            let mut peers = addrs.clone();
            peers.remove(addr);
            let store = MemStore::new();
            let (state_machine, recv) = ChannelStateMachine::new();
            (Replica::new(addr.clone(), peers, store, state_machine), recv)
        }).collect()
    }

    /// Elect `leader` as the leader of a cluster with the provided followers.
    /// The leader and the followers must be in the same term.
    fn elect_leader(leader: &mut TestReplica,
                    followers: &mut [(TestReplica, mpsc::Receiver<Vec<u8>>)]) {
        let mut request_vote_request = MallocMessageBuilder::new_default();
        let mut append_entries_request = MallocMessageBuilder::new_default();
        let mut response = MallocMessageBuilder::new_default();

        let send_request = leader.election_timeout(request_vote_request.init_root::<request_vote_request::Builder>());
        if !send_request {
            // The leader could have had an AppendEntries request since the last timeout, so it may
            // take two timeouts.
            let send_request = leader.election_timeout(request_vote_request.init_root::<request_vote_request::Builder>());
            assert!(send_request);
        }

        for &mut (ref mut follower, _) in followers.iter_mut() {
            follower.request_vote_request(leader.addr().clone(),
                                          request_vote_request.get_root::<request_vote_request::Builder>().unwrap().as_reader(),
                                          response.init_root::<request_vote_response::Builder>());

            let resp = response.get_root::<request_vote_response::Builder>().unwrap().as_reader();
            assert!(if let request_vote_response::Which::Granted(_) = resp.which().unwrap() { true } else { false });

            // Return success vote to candidate, and make sure it transitions to leader
            let send_message = leader.request_vote_response(follower.addr().clone(),
                                                            resp,
                                                            append_entries_request.init_root::<append_entries_request::Builder>());
            assert!(send_message);
            assert!(follower.is_follower());
        }
        assert!(leader.is_leader());
    }

    /// Tests that a single-replica cluster will behave appropriately.
    ///
    /// The single replica should transition straight to the Leader state upon the first timeout.
    #[test]
    fn test_solitary_replica_transition_to_leader() {
        let (mut replica, _) = new_cluster(1).pop().unwrap();
        assert!(replica.is_follower());

        let mut message = MallocMessageBuilder::new_default();
        let request = message.init_root::<request_vote_request::Builder>();

        let send_message = replica.election_timeout(request);
        assert!(!send_message);
        assert!(replica.is_leader());
    }

    /// A simple election test of a two-replica cluster.
    #[test]
    fn test_election() {
        let mut replicas = new_cluster(2);
        let (mut replica1, _) = replicas.pop().unwrap();
        let (mut replica2, _) = replicas.pop().unwrap();

        let mut request = MallocMessageBuilder::new_default();
        let mut response = MallocMessageBuilder::new_default();

        // Trigger replica1's timeout, and make sure it transitions to candidate

        let send_message = replica1.election_timeout(request.init_root::<request_vote_request::Builder>());
        assert!(send_message);
        assert!(replica1.is_candidate());

        // Send replica1's RequestVoteRequest to replica2

        replica2.request_vote_request(replica1.addr().clone(),
                                      request.get_root::<request_vote_request::Builder>().unwrap().as_reader(),
                                      response.init_root::<request_vote_response::Builder>());

        let resp = response.get_root::<request_vote_response::Builder>().unwrap().as_reader();
        assert!(if let request_vote_response::Which::Granted(_) = resp.which().unwrap() { true } else { false });

        // Trigger replica2's timeout, and make sure it does *not* transitition to candidate, since
        // it has already voted in an election during this timeout period.
        let send_message = replica2.election_timeout(request.init_root::<request_vote_request::Builder>());
        assert!(!send_message);

        // Return success vote to candidate, and make sure it transitions to leader
        let send_message = replica1.request_vote_response(replica2.addr().clone(),
                                                          resp,
                                                          request.init_root::<append_entries_request::Builder>());
        assert!(send_message);
        assert!(replica1.is_leader());
        assert!(replica1.current_term() == Term::from(1));
    }

    /// Tests a two node cluster with leader and follower.  The leader sends a heartbeat to the
    /// follower, who then has an election_timeout, but does not transition to candidate because
    /// the heartbeat was observed.  A second election_timeout will transition the follower to
    /// candidate state.
    #[test]
    fn test_heartbeat() {
        let mut request = MallocMessageBuilder::new_default();
        let mut response = MallocMessageBuilder::new_default();
        let mut replicas = new_cluster(2);
        let (mut leader, _) = replicas.pop().unwrap();

        elect_leader(&mut leader, &mut replicas[..]);
        let (mut follower, _) = replicas.pop().unwrap();

        let send_message =
            leader.heartbeat_timeout(request.init_root::<append_entries_request::Builder>());
        assert!(send_message);

        follower.append_entries_request(leader.addr().clone(),
                                        request.get_root::<append_entries_request::Builder>().unwrap().as_reader(),
                                        response.init_root::<append_entries_response::Builder>());

        let resp = response.get_root::<append_entries_response::Builder>().unwrap().as_reader();
        assert!(if let append_entries_response::Which::Success(0) = resp.which().unwrap() { true } else { false });

        let send_message = follower.election_timeout(request.init_root::<request_vote_request::Builder>());
        assert!(!send_message);
        assert!(follower.is_follower());
        let send_message = follower.election_timeout(request.init_root::<request_vote_request::Builder>());
        assert!(send_message);
        assert!(follower.is_candidate());
    }
}
