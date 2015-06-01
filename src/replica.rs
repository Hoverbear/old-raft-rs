use std::collections::HashSet;
use std::{cmp, fmt};
use std::net::SocketAddr;
use std::rc::Rc;

use capnp::{
    MallocMessageBuilder,
    MessageBuilder,
    MessageReader,
};

use {LogIndex, Term, messages};
use messages_capnp::{
    append_entries_request,
    append_entries_response,
    propose_request,
    message,
    request_vote_request,
    request_vote_response,
};
use state::{ReplicaState, LeaderState, CandidateState, FollowerState};
use state_machine::StateMachine;
use store::Store;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Timeout {
    Election,
    Heartbeat,
}

pub struct Actions {
    messages: Vec<(SocketAddr, Rc<MallocMessageBuilder>)>,
    timeouts: Vec<(Timeout, u64)>,
}

impl Actions {
    pub fn empty() -> Actions {
        Actions { messages: vec![], timeouts: vec![] }
    }
}

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
            state: ReplicaState::Follower,
            leader_state: leader_state,
            candidate_state: CandidateState::new(),
            follower_state: FollowerState::new(),
        }
    }

    pub fn peers(&self) -> &HashSet<SocketAddr> {
        &self.peers
    }

    pub fn apply_message<R>(&mut self,
                            from: SocketAddr,
                            message: &R)
                            -> Actions
    where R: MessageReader {
        let reader = message.get_root::<message::Reader>().unwrap().which().unwrap();
        match reader {
            message::Which::AppendEntriesRequest(Ok(request)) =>
                self.append_entries_request(from, request),
            message::Which::AppendEntriesResponse(Ok(response)) =>
                self.append_entries_response(from, response),
            message::Which::RequestVoteRequest(Ok(request)) =>
                self.request_vote_request(from, request),
            message::Which::RequestVoteResponse(Ok(response)) =>
                self.request_vote_response(from, response),
            message::Which::ProposeRequest(Ok(request)) =>
                self.propose_request(from, request),
            _ => panic!("cannot handle message"),
        }
    }

    pub fn apply_timeout(&mut self, timeout: Timeout) -> Actions {
        match timeout {
            Timeout::Election => self.election_timeout(),
            Timeout::Heartbeat => self.heartbeat_timeout(),
        }
    }

    /// Apply an append entries request to the Raft replica.
    pub fn append_entries_request(&mut self,
                                  from: SocketAddr,
                                  request: append_entries_request::Reader)
                                  -> Actions {
        assert!(self.peers.contains(&from),
                "Received append entries request from unknown node {}.", &from);
        debug!("{:?}: AppendEntriesRequest from Replica({})", self, &from);

        let leader_term = Term(request.get_term());
        let current_term = self.current_term();

        if leader_term < current_term {
            let message = messages::append_entries_response_stale_term(current_term);
            return Actions { messages: vec![(from.clone(), message)], timeouts: vec![] };
        }

        let leader_commit_index = LogIndex::from(request.get_leader_commit());
        assert!(self.commit_index <= leader_commit_index);
        self.commit_index = leader_commit_index;

        match self.state {
            ReplicaState::Follower => {
                let message = {
                    if current_term < leader_term {
                        self.store.set_current_term(leader_term).unwrap();
                        self.follower_state.set_leader(from);
                    }

                    let leader_prev_log_index = LogIndex(request.get_prev_log_index());
                    let leader_prev_log_term = Term(request.get_prev_log_term());

                    let latest_log_index = self.store.latest_log_index().unwrap();
                    if latest_log_index < leader_prev_log_index {
                        messages::append_entries_response_inconsistent_prev_entry(current_term)
                    } else {
                        let existing_term = if leader_prev_log_index == LogIndex::from(0) {
                            Term::from(0)
                        } else {
                            self.store.entry(leader_prev_log_index).unwrap().0
                        };

                        if existing_term != leader_prev_log_term {
                            messages::append_entries_response_inconsistent_prev_entry(current_term)
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
                            messages::append_entries_response_success(current_term, latest_log_index)
                        }
                    }
                };
                return Actions { messages: vec![(from.clone(), message)], timeouts: vec![] };
            },
            ReplicaState::Candidate => {
                // recognize the new leader, return to follower state, and apply the entries
                self.transition_to_follower(leader_term, from);
                return self.append_entries_request(from, request)
            },
            ReplicaState::Leader => {
                if leader_term == current_term {
                    // The single leader-per-term invariant is broken; there is a bug in the Raft
                    // implementation.
                    panic!("ID {}: peer leader {} with matching term {:?} detected.",
                           self.addr, from, current_term);
                }

                // recognize the new leader, return to follower state, and apply the entries
                self.transition_to_follower(leader_term, from);
                return self.append_entries_request(from, request)
            },
        }
    }

    /// Apply an append entries response to the Raft replica.
    ///
    /// The provided message may be initialized with a new AppendEntries request to send back to
    /// the follower in the case that the follower's log is behind.
    pub fn append_entries_response(&mut self,
                                   from: SocketAddr,
                                   response: append_entries_response::Reader)
                                   -> Actions {
        assert!(self.peers.contains(&from),
                "{:?} received AppendEntries response from unknown peer {}.",
                self, &from);
        debug!("{:?}: AppendEntriesResponse from Replica({})", self, from);

        let mut actions = Actions::empty();

        let local_term = self.current_term();
        let responder_term = Term::from(response.get_term());
        let local_latest_log_index = self.store.latest_log_index().unwrap();

        if local_term < responder_term {
            // Responder has a higher term number. Relinquish leader position (if it is held), and
            // return to follower status.

            // The responder is not necessarily the leader, but it is somewhat likely, so we will
            // use it as the leader hint.
            self.transition_to_follower(responder_term, from);
            return actions;
        } else if local_term > responder_term {
            // Responder is responding to an AppendEntries request from a different term. Ignore
            // the response.
            return actions;
        }

        // At this point we must be the Leader, since we only send AppendEntries
        // requests while in the Leader state, so we should never receive AppendEntries
        // responses from the current term while not the Leader (It's impossible to go
        // from Leader to Candidate or Follower states without increasing the term, and
        // we have already checked that local_term == responder_term).
        assert!(self.is_leader(),
                "{:?}: received AppendEntries response from Replica({}) for the \
                 current term while in state {:?}.", self.addr, from, self.state);

        match response.which() {
            Ok(append_entries_response::Which::Success(follower_latest_log_index)) => {
                let follower_latest_log_index = LogIndex::from(follower_latest_log_index);
                assert!(follower_latest_log_index <= local_latest_log_index);
                self.leader_state.set_match_index(from, follower_latest_log_index);
                self.advance_commit_index();
            }
            Ok(append_entries_response::Which::InconsistentPrevEntry(..)) => {
                let next_index = self.leader_state.next_index(&from) - 1;
                self.leader_state.set_next_index(from, next_index);
            }
            Ok(append_entries_response::Which::StaleTerm(..)) => {
                // This case is handled above by checking local_term against
                // responder_term.
                unreachable!("{:?}: AppendEntries.StaleTerm response from \
                             Replica({}). local term: {:?}, responder term: {:?}.",
                             self, from, local_term, responder_term);
            }
            Ok(append_entries_response::Which::InternalError(error_result)) => {
                let error = error_result.unwrap_or("[unable to decode internal error]");
                warn!("{:?}: AppendEntries.InternalError response from Replica({}): {}",
                      self, from, error);
            }
            Err(error) => {
                warn!("{:?}: Error decoding AppendEntriesResponse from Replica({}): {}",
                      self, from, error);
            }
        }

        // TODO: set a new heartbeat
        // TODO: add client responses based on new commit index

        let next_index = self.leader_state.next_index(&from);
        if next_index <= local_latest_log_index {
            let prev_log_index = next_index - 1;
            let (prev_log_term, _) = self.store.entry(prev_log_index).unwrap();

            let mut message = MallocMessageBuilder::new_default();
            {
                let mut request = message.init_root::<message::Builder>()
                                         .init_append_entries_request();
                request.set_term(local_term.into());
                request.set_prev_log_index(prev_log_index.into());
                request.set_prev_log_term(prev_log_term.into());
                request.set_leader_commit(self.commit_index.into());

                let from_index = Into::<u64>::into(next_index);
                let until_index = Into::<u64>::into(local_latest_log_index) + 1;
                let mut entries = request.init_entries((until_index - from_index) as u32);
                for (n, index) in (from_index..until_index).enumerate() {
                    entries.set(n as u32, self.store.entry(LogIndex::from(index)).unwrap().1);
                }
            }
            actions.messages.push((from.clone(), Rc::new(message)));
            self.leader_state.set_next_index(from, local_latest_log_index + 1);
        }
        actions
    }

    /// Apply a request vote request to the Raft replica.
    pub fn request_vote_request(&mut self,
                                candidate: SocketAddr,
                                request: request_vote_request::Reader)
                                -> Actions {
        assert!(self.peers.contains(&candidate),
                "Received request vote request from unknown node {}.", &candidate);
        debug!("{:?}: RequestVoteRequest from Replica({})", self, &candidate);

        let candidate_term = Term(request.get_term());
        let candidate_index = LogIndex(request.get_last_log_index());
        let local_term = self.current_term();
        let local_index = self.store.latest_log_index().unwrap();

        let new_local_term = if candidate_term > local_term {
            self.store.set_current_term(candidate_term).unwrap();
            candidate_term
        } else {
            local_term
        };

        let message = if candidate_term < local_term {
            messages::request_vote_response_stale_term(new_local_term)
        } else if candidate_index < local_index {
            messages::request_vote_response_inconsistent_log(new_local_term)
        } else {
            match self.store.voted_for().unwrap() {
                None => {
                    self.store.set_voted_for(candidate).unwrap();
                    messages::request_vote_response_granted(new_local_term)
                },
                Some(voted_for) if voted_for == candidate.clone() => {
                    messages::request_vote_response_granted(new_local_term)
                },
                _ => {
                    messages::request_vote_response_already_voted(new_local_term)
                },
            }
        };
        Actions { messages: vec![(candidate.clone(), message)], timeouts: vec![] }
    }

    /// Apply a request vote response to the Raft replica.
    pub fn request_vote_response(&mut self,
                                 from: SocketAddr,
                                 response: request_vote_response::Reader)
                                 -> Actions {
        assert!(self.peers.contains(&from), "Received request vote response from unknown node {}.", from);
        debug!("{:?}: RequestVoteResponse from Replica({})", self, from);

        let local_term = self.current_term();
        let voter_term = Term::from(response.get_term());

        let mut actions = Actions::empty();
        let majority = self.majority();
        if local_term < voter_term {
            // Responder has a higher term number. The election is compromised; abandon it and
            // revert to follower state with the updated term number. Any further responses we
            // receive from this election term will be ignored because the term will be outdated.

            // The responder is not necessarily the leader, but it is somewhat likely, so we will
            // use it as the leader hint.
            self.transition_to_follower(voter_term, from);
        } else if local_term > voter_term {
            // Ignore this message; it came from a previous election cycle.
        } else if self.is_candidate() {
            // A vote was received!
            if let Ok(request_vote_response::Granted(_)) = response.which() {
                self.candidate_state.record_vote(from.clone());
                if self.candidate_state.count_votes() >= majority {
                    self.transition_to_leader(&mut actions);
                }
            }
        }
        actions
    }

    /// Apply a client propose request to the Raft replica.
    pub fn propose_request(&mut self,
                           from: SocketAddr,
                           request: propose_request::Reader)
                           -> Actions {
        debug!("{:?}: Propose from Client({})", self, from);
        unimplemented!()
    }

    /// Trigger a heartbeat timeout on the Raft replica.
    ///
    /// The provided AppendEntriesRequest builder may be initialized with a message to send to each
    /// cluster peer.
    pub fn heartbeat_timeout(&mut self) -> Actions {
        debug!("{:?}: HeartbeatTimeout", self);
        /*
        // Send a heartbeat
        message.set_term(self.current_term().into());
        message.set_prev_log_index(self.store.latest_log_index().unwrap().into());
        message.set_prev_log_term(self.store.latest_log_term().unwrap().into());
        message.set_leader_commit(self.commit_index.into());
        message.init_entries(0);
        */
        // TODO: send an AppendEntries heartbeat
        unimplemented!()
    }

    /// Trigger an election timeout on the Raft replica.
    ///
    /// The provided RequestVoteRequest builder may be initialized with a message to send to each
    /// cluster peer.
    pub fn election_timeout(&mut self) -> Actions {
        debug!("{:?}: ElectionTimeout", self);
        let mut actions = Actions::empty();
        if !self.is_leader() {
            if self.peers.is_empty() {
                // Solitary replica special case; jump straight to leader status
                info!("{:?}: Transition to Leader", self);
                assert!(self.is_follower());
                assert!(self.store.voted_for().unwrap().is_none());
                self.store.inc_current_term().unwrap();
                self.store.set_voted_for(self.addr).unwrap();
                let latest_log_index = self.store.latest_log_index().unwrap();
                self.state = ReplicaState::Leader;
                self.leader_state.reinitialize(latest_log_index);
            } else {
                self.transition_to_candidate(&mut actions);
            }
        }
        actions
    }

    /// Transition this Replica to Leader state.
    ///
    /// The provided Actions instance will have an AppendEntriesRequest message
    /// added for each cluster peer.
    fn transition_to_leader(&mut self, actions: &mut Actions) {
        info!("{:?}: Transition to Leader", self);
        let current_term = self.current_term();
        let latest_log_index = self.store.latest_log_index().unwrap();
        let latest_log_term = self.store.latest_log_term().unwrap();
        self.state = ReplicaState::Leader;
        self.leader_state.reinitialize(latest_log_index);

        let mut message = MallocMessageBuilder::new_default();
        {
            let mut request = message.init_root::<message::Builder>()
                                     .init_append_entries_request();
            request.set_term(current_term.into());
            request.set_prev_log_index(latest_log_index.into());
            request.set_prev_log_term(latest_log_term.into());
            request.set_leader_commit(self.commit_index.into());
        }

        let message_rc = Rc::new(message);
        for peer in self.peers().iter().cloned() {
            actions.messages.push((peer, message_rc.clone()));
        }
    }

    /// Transition this Replica to Candidate state.
    ///
    /// The provided RequestVoteRequest message will be initialized with a message to send to each
    /// cluster peer.
    fn transition_to_candidate(&mut self, actions: &mut Actions) {
        info!("{:?}: Transition to Candidate", self);
        self.store.inc_current_term().unwrap();
        self.store.set_voted_for(self.addr).unwrap();
        self.state = ReplicaState::Candidate;
        self.candidate_state.clear();
        self.candidate_state.record_vote(self.addr.clone());

        let current_term = self.current_term();
        let latest_index = self.store.latest_log_index().unwrap();
        let latest_term = self.store.latest_log_term().unwrap();

        let mut message = MallocMessageBuilder::new_default();
        {
            let mut request = message.init_root::<message::Builder>()
                                     .init_request_vote_request();
            request.set_term(current_term.into());
            request.set_last_log_index(latest_index.into());
            request.set_last_log_term(latest_term.into());
        }

        let message_rc = Rc::new(message);
        for peer in self.peers().iter().cloned() {
            actions.messages.push((peer, message_rc.clone()));
        }
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
    fn transition_to_follower(&mut self,
                              term: Term,
                              leader: SocketAddr) {
        info!("{:?}: Transition to Follower", self);
        self.store.set_current_term(term).unwrap();
        self.state = ReplicaState::Follower;
        self.follower_state.set_leader(leader.clone());
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

    use std::collections::{HashSet, HashMap};
    use std::io::Cursor;
    use std::net::{SocketAddr};
    use std::rc::Rc;
    use std::str::FromStr;
    use std::sync::mpsc;

    use capnp::{MallocMessageBuilder, MessageBuilder, MessageReader, ReaderOptions};
    use capnp::serialize::{self, OwnedSpaceMessageReader};

    use messages_capnp::{
        append_entries_request,
        append_entries_response,
        request_vote_request,
        request_vote_response,
    };
    use replica::{Actions, Replica};
    use state_machine::NullStateMachine;
    use store::MemStore;
    use Term;

    type TestReplica = Replica<MemStore, NullStateMachine>;

    fn new_cluster(size: u16) -> HashMap<SocketAddr, TestReplica> {
        // the actual port does not matter here since they won't be bound
        let addrs: HashSet<SocketAddr> =
            (0..size).map(|port| FromStr::from_str(&format!("127.0.0.1:{}", port)).unwrap()).collect();

        addrs.iter().map(|addr| {
            let mut peers = addrs.clone();
            peers.remove(addr);
            let store = MemStore::new();
            (addr.clone(), Replica::new(addr.clone(), peers, store, NullStateMachine))
        }).collect()
    }

    fn into_reader<M>(message: &M) -> OwnedSpaceMessageReader where M: MessageBuilder {
        let mut buf = Cursor::new(Vec::new());

        serialize::write_message(&mut buf, message).unwrap();
        buf.set_position(0);
        serialize::read_message(&mut buf, ReaderOptions::new()).unwrap()
    }

    /// Applies the actions to the replicas (and recursively applies any resulting actions), and
    /// returns any client messages.
    fn apply_actions(from: SocketAddr,
                     actions: Actions,
                     replicas: &mut HashMap<SocketAddr, TestReplica>)
                     -> Vec<(SocketAddr, Rc<MallocMessageBuilder>)> {

        fn inner(from: SocketAddr,
                 actions: Actions,
                 replicas: &mut HashMap<SocketAddr, TestReplica>,
                 client_messages: &mut Vec<(SocketAddr, Rc<MallocMessageBuilder>)>) {
            for (destination, message) in actions.messages.into_iter() {
                // TODO: Figure out how to simplify this without tripping the borrow checker.
                let reader = into_reader(&*message);
                let mut further_actions = None;
                if let Some(replica) = replicas.get_mut(&destination) {
                    further_actions = Some(replica.apply_message(from.clone(), &reader));
                } else {
                    client_messages.push((destination, message));
                }
                if let Some(actions) = further_actions {
                    inner(destination, actions, replicas, client_messages);
                }
            }
        }

        let mut client_messages = vec![];
        inner(from, actions, replicas, &mut client_messages);
        client_messages
    }

    /// Elect `leader` as the leader of a cluster with the provided followers.
    /// The leader and the followers must be in the same term.
    fn elect_leader(leader: SocketAddr,
                    replicas: &mut HashMap<SocketAddr, TestReplica>) {
        let actions = replicas.get_mut(&leader).unwrap().election_timeout();
        let client_messages = apply_actions(leader, actions, replicas);
        assert!(client_messages.is_empty());
        assert!(replicas[&leader].is_leader());
    }

    /// Tests that a single-replica cluster will behave appropriately.
    ///
    /// The single replica should transition straight to the Leader state upon the first timeout.
    #[test]
    fn test_solitary_replica_transition_to_leader() {
        let (_, mut replica) = new_cluster(1).into_iter().next().unwrap();
        assert!(replica.is_follower());

        let actions = replica.election_timeout();
        assert!(replica.is_leader());
        assert!(actions.messages.is_empty());
        assert!(actions.timeouts.is_empty());
    }

    /// A simple election test of a two-replica cluster.
    #[test]
    fn test_election() {
        let mut replicas = new_cluster(2);
        let mut addresses: Vec<SocketAddr> = replicas.keys().cloned().collect();
        let leader = &addresses[0];
        let follower = &addresses[1];
        elect_leader(leader.clone(), &mut replicas);

        assert!(replicas[leader].is_leader());
        assert!(replicas[follower].is_follower());
    }

    /// Tests the Raft heartbeating mechanism. The leader receives a heartbeat
    /// timeout, and in response sends an AppendEntries message to the follower.
    /// The follower in turn resets its election timout, and replies to the
    /// leader.
    #[test]
    fn test_heartbeat() {
        // TODO
    }
}
