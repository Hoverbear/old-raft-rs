//! The `Replica` is a state-machine (not to be confused with the `StateMachine` trait) which
//!  implements the logic of the Raft Protocol. A `Replica` receives events from the local
//!  `Server`.  The set of possible events is specified by the Raft Protocol:
//!
//! ```text
//! Event = AppendEntriesRequest | AppendEntriesResponse
//!       | RequestVoteRequest | RequestVoteResponse
//!       | ElectionTimeout | HeartbeatTimeout
//!       | ClientCommand
//! ```
//! In response to receiving an event, the `Replica` may mutate its own state, apply a command to
//!  the local `StateMachine`, or return an event to be sent to one or more remote `Server` or
//!  `Client` instances.

use std::collections::HashSet;
use std::{cmp, fmt};
use std::rc::Rc;

use capnp::{
    MallocMessageBuilder,
    MessageBuilder,
    MessageReader,
};
use rand::{self, Rng};

use {LogIndex, Term, ServerId, ClientId, messages};
use messages_capnp::{
    append_entries_request,
    append_entries_response,
    client_request,
    proposal_request,
    message,
    request_vote_request,
    request_vote_response,
};
use state::{ReplicaState, LeaderState, CandidateState, FollowerState};
use state_machine::StateMachine;
use store::Store;

const ELECTION_MIN: u64 = 1500;
const ELECTION_MAX: u64 = 3000;
const HEARTBEAT_DURATION: u64 = 1000;

/// Timeout types for Raft.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ReplicaTimeout {
    // An election timeout. Randomized value.
    Election,
    // A heartbeat timeout. Stable value.
    Heartbeat(ServerId),
}

impl ReplicaTimeout {
    /// Returns how long the timeout period is.
    pub fn duration_ms(&self) -> u64 {
        match *self {
            ReplicaTimeout::Election => rand::thread_rng().gen_range::<u64>(ELECTION_MIN, ELECTION_MAX),
            ReplicaTimeout::Heartbeat(..) => HEARTBEAT_DURATION,
        }
    }
}

/// The set of actions for the server to carry out after applying requests to the replica.
pub struct Actions {
    /// Messages to be sent to peers.
    pub peer_messages: Vec<(ServerId, Rc<MallocMessageBuilder>)>,
    /// Messages to be send to clients.
    pub client_messages: Vec<(ClientId, Rc<MallocMessageBuilder>)>,
    /// Whether or not to clear timeouts associated.
    pub clear_timeouts: bool,
    /// Any new timeouts to create.
    pub timeouts: Vec<ReplicaTimeout>,
}

impl fmt::Debug for Actions {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let peer_messages: Vec<ServerId> = self.peer_messages
                                               .iter().map(|peer_message| peer_message.0)
                                               .collect();
        let client_messages: Vec<ClientId> = self.client_messages
                                                 .iter().map(|client_message| client_message.0)
                                                 .collect();
        write!(fmt, "Actions {{ peer_messages: {:?}, client_messages: {:?}, \
                     clear_timeouts: {:?}, timeouts: {:?} }}",
               peer_messages, client_messages, self.clear_timeouts, self.timeouts)
    }
}

impl Actions {
    /// Creates an empty `Actions` set.
    pub fn new() -> Actions {
        Actions {
            peer_messages: vec![],
            client_messages: vec![],
            clear_timeouts: false,
            timeouts: vec![],
        }
    }
}

/// A replica of a Raft distributed state machine. A Raft replica controls a client state machine,
/// to which it applies commands in a globally consistent order.
pub struct Replica<S, M> {
    /// The ID of this replica.
    id: ServerId,
    /// The IDs of the other replicas in the cluster.
    peers: HashSet<ServerId>,

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
    /// Creates a `Replica`.
    pub fn new(id: ServerId,
               peers: HashSet<ServerId>,
               store: S,
               state_machine: M)
               -> Replica<S, M> {
        let leader_state = LeaderState::new(store.latest_log_index().unwrap(), &peers);
        Replica {
            id: id,
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

    /// Returns the set of initial action which should be executed upon startup.
    pub fn init(&self) -> Actions {
        let mut actions = Actions::new();
        actions.timeouts.push(ReplicaTimeout::Election);
        actions
    }

    /// Returns the peers (Id's only) of the replica.
    pub fn peers(&self) -> &HashSet<ServerId> {
        &self.peers
    }

    /// Applies a peer message to the replica. This function dispatches a generic request to it's
    /// appropriate handler.
    pub fn apply_peer_message<R>(&mut self, from: ServerId, message: &R, actions: &mut Actions)
    where R: MessageReader {
        let reader = message.get_root::<message::Reader>().unwrap().which().unwrap();
        match reader {
            message::Which::AppendEntriesRequest(Ok(request)) =>
                self.append_entries_request(from, request, actions),
            message::Which::AppendEntriesResponse(Ok(response)) =>
                self.append_entries_response(from, response, actions),
            message::Which::RequestVoteRequest(Ok(request)) =>
                self.request_vote_request(from, request, actions),
            message::Which::RequestVoteResponse(Ok(response)) =>
                self.request_vote_response(from, response, actions),
            _ => panic!("cannot handle message"),
        };
    }

    /// Applies a client message to the replica. This function dispatches a generic request to it's
    /// appropriate handler. (Right now, only a `proposal_request` is valid.)
    pub fn apply_client_message<R>(&mut self,
                                   from: ClientId,
                                   message: &R,
                                   actions: &mut Actions)
    where R: MessageReader {
        let reader = message.get_root::<client_request::Reader>().unwrap().which().unwrap();
        match reader {
            client_request::Which::Proposal(Ok(request)) =>
                self.proposal_request(from, request, actions),
            _ => panic!("cannot handle message"),
        }
    }

    /// Applies a timeout's actions to the `Replica`.
    pub fn apply_timeout(&mut self, timeout: ReplicaTimeout, actions: &mut Actions) {
        push_log_scope!("{:?}", self);
        match timeout {
            ReplicaTimeout::Election => self.election_timeout(actions),
            ReplicaTimeout::Heartbeat(peer) => self.heartbeat_timeout(peer, actions),
        }
    }

    /// Apply an append entries request to the Raft replica.
    fn append_entries_request(&mut self,
                              from: ServerId,
                              request: append_entries_request::Reader,
                              actions: &mut Actions) {
        push_log_scope!("{:?}", self);
        scoped_debug!("AppendEntriesRequest from Replica({})", &from);

        let leader_term = Term(request.get_term());
        let current_term = self.current_term();

        if leader_term < current_term {
            let message = messages::append_entries_response_stale_term(current_term);
            actions.peer_messages.push((from, message));
            return;
        }

        let leader_commit_index = LogIndex::from(request.get_leader_commit());
        scoped_assert!(self.commit_index <= leader_commit_index);

        match self.state {
            ReplicaState::Follower => {
                let message = {
                    if current_term < leader_term {
                        self.store.set_current_term(leader_term).unwrap();
                        self.follower_state.set_leader(from);
                    }

                    let leader_prev_log_index = LogIndex(request.get_prev_log_index());
                    let leader_prev_log_term = Term(request.get_prev_log_term());

                    let latest_log_index = self.latest_log_index();
                    if latest_log_index < leader_prev_log_index {
                        messages::append_entries_response_inconsistent_prev_entry(self.current_term())
                    } else {
                        let existing_term = if leader_prev_log_index == LogIndex::from(0) {
                            Term::from(0)
                        } else {
                            self.store.entry(leader_prev_log_index).unwrap().0
                        };

                        if existing_term != leader_prev_log_term {
                            messages::append_entries_response_inconsistent_prev_entry(self.current_term())
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
                            self.commit_index = leader_commit_index;
                            self.apply_commits_until(latest_log_index);
                            messages::append_entries_response_success(self.current_term(), self.store.latest_log_index().unwrap())
                        }
                    }
                };
                actions.peer_messages.push((from, message));
                actions.timeouts.push(ReplicaTimeout::Election);
            },
            ReplicaState::Candidate => {
                // recognize the new leader, return to follower state, and apply the entries
                scoped_info!("received AppendEntriesRequest from Replica {{ id: {}, term: {} }} \
                             with newer term", from, leader_term);
                self.transition_to_follower(leader_term, from, actions);
                return self.append_entries_request(from, request, actions)
            },
            ReplicaState::Leader => {
                if leader_term == current_term {
                    // The single leader-per-term invariant is broken; there is a bug in the Raft
                    // implementation.
                    panic!("{:?}: peer leader {} with matching term {:?} detected.",
                           self, from, current_term);
                }

                // recognize the new leader, return to follower state, and apply the entries
                scoped_info!("received AppendEntriesRequest from Replica {{ id: {}, term: {} }} \
                             with newer term", from, leader_term);
                self.transition_to_follower(leader_term, from, actions);
                return self.append_entries_request(from, request, actions)
            },
        }
    }

    /// Apply an append entries response to the Raft replica.
    ///
    /// The provided message may be initialized with a new AppendEntries request to send back to
    /// the follower in the case that the follower's log is behind.
    fn append_entries_response(&mut self,
                               from: ServerId,
                               response: append_entries_response::Reader,
                               actions: &mut Actions) {
        push_log_scope!("{:?}", self);
        scoped_debug!("AppendEntriesResponse from Replica({})", from);

        let local_term = self.current_term();
        let responder_term = Term::from(response.get_term());
        let local_latest_log_index = self.latest_log_index();

        if local_term < responder_term {
            // Responder has a higher term number. Relinquish leader position (if it is held), and
            // return to follower status.

            // The responder is not necessarily the leader, but it is somewhat likely, so we will
            // use it as the leader hint.
            scoped_info!("received AppendEntriesResponse from Replica {{ id: {}, term: {} }} \
                         with newer term", from, responder_term);
            self.transition_to_follower(responder_term, from, actions);
            return;
        } else if local_term > responder_term {
            // Responder is responding to an AppendEntries request from a different term. Ignore
            // the response.
            return;
        }

        // At this point we must be the Leader, since we only send AppendEntries
        // requests while in the Leader state, so we should never receive AppendEntries
        // responses from the current term while not the Leader (It's impossible to go
        // from Leader to Candidate or Follower states without increasing the term, and
        // we have already checked that local_term == responder_term).
        scoped_assert!(self.is_leader(),
                      "received AppendEntries response from Replica({}) for the \
                      current term while in state {:?}.", from, self.state);

        match response.which() {
            Ok(append_entries_response::Which::Success(follower_latest_log_index)) => {
                let follower_latest_log_index = LogIndex::from(follower_latest_log_index);
                scoped_assert!(follower_latest_log_index <= local_latest_log_index);
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
                scoped_warn!("AppendEntries.InternalError response from Replica({}): {}",
                             from, error);
            }
            Err(error) => {
                scoped_warn!("Error decoding AppendEntriesResponse from Replica({}): {}",
                             from, error);
            }
        }

        // TODO: add client responses based on new commit index

        let next_index = self.leader_state.next_index(&from);
        if next_index <= local_latest_log_index {
            // If the replica is behind, send it entries to catch up.
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
            self.leader_state.set_next_index(from, local_latest_log_index + 1);
            actions.peer_messages.push((from, Rc::new(message)));
        } else {
            // If the replica is caught up, set a heartbeat timeout.
            let timeout = ReplicaTimeout::Heartbeat(from);
            actions.timeouts.push(timeout);
        }
    }

    /// Apply a request vote request to the Raft replica.
    fn request_vote_request(&mut self,
                            candidate: ServerId,
                            request: request_vote_request::Reader,
                            actions: &mut Actions) {
        push_log_scope!("{:?}", self);
        let candidate_term = Term(request.get_term());
        let candidate_log_term = Term(request.get_last_log_term());
        let candidate_log_index = LogIndex(request.get_last_log_index());
        scoped_debug!("RequestVoteRequest from Replica {{ id: {}, term: {}, latest_log_term: {}, \
                      latest_log_index: {} }}",
                      &candidate, candidate_term, candidate_log_term, candidate_log_index);
        let local_term = self.current_term();

        let new_local_term = if candidate_term > local_term {
            scoped_info!("received RequestVoteRequest from Replica {{ id: {}, term: {} }} \
                         with newer term", candidate, candidate_term);
            self.transition_to_follower(candidate_term, candidate, actions);
            candidate_term
        } else {
            local_term
        };

        let message = if candidate_term < local_term {
            messages::request_vote_response_stale_term(new_local_term)
        } else if candidate_log_term < self.latest_log_term()
               || candidate_log_index < self.latest_log_index() {
            messages::request_vote_response_inconsistent_log(new_local_term)
        } else {
            match self.store.voted_for().unwrap() {
                None => {
                    self.store.set_voted_for(candidate).unwrap();
                    messages::request_vote_response_granted(new_local_term)
                },
                Some(voted_for) if voted_for == candidate => {
                    messages::request_vote_response_granted(new_local_term)
                },
                _ => {
                    messages::request_vote_response_already_voted(new_local_term)
                },
            }
        };
        actions.peer_messages.push((candidate, message));
    }

    /// Apply a request vote response to the Raft replica.
    fn request_vote_response(&mut self,
                             from: ServerId,
                             response: request_vote_response::Reader,
                             actions: &mut Actions) {
        scoped_debug!("RequestVoteResponse from Replica({})", from);

        let local_term = self.current_term();
        let voter_term = Term::from(response.get_term());

        let majority = self.majority();
        if local_term < voter_term {
            // Responder has a higher term number. The election is compromised; abandon it and
            // revert to follower state with the updated term number. Any further responses we
            // receive from this election term will be ignored because the term will be outdated.

            // The responder is not necessarily the leader, but it is somewhat likely, so we will
            // use it as the leader hint.
            scoped_info!("received RequestVoteResponse from Replica {{ id: {}, term: {} }} \
                         with newer term", from, voter_term);
            self.transition_to_follower(voter_term, from, actions);
        } else if local_term > voter_term {
            // Ignore this message; it came from a previous election cycle.
        } else if self.is_candidate() {
            // A vote was received!
            if let Ok(request_vote_response::Granted(_)) = response.which() {
                self.candidate_state.record_vote(from.clone());
                if self.candidate_state.count_votes() >= majority {
                    self.transition_to_leader(actions);
                }
            }
        };
    }

    /// Apply a client proposal to the Raft replica.
    fn proposal_request(&mut self,
                        from: ClientId,
                        _request: proposal_request::Reader,
                        _actions: &mut Actions) {
        scoped_debug!("Proposal from Client({})", from);
        unimplemented!()
    }

    /// Trigger a heartbeat timeout on the Raft replica.
    /// `peer` is the ID of the peer which
    fn heartbeat_timeout(&mut self, peer: ServerId, actions: &mut Actions) {
        scoped_debug!("HeartbeatTimeout");
        scoped_assert!(self.is_leader());
        let mut message = MallocMessageBuilder::new_default();
        {
            let mut request = message.init_root::<message::Builder>()
                                     .init_append_entries_request();
            request.set_term(self.current_term().into());
            request.set_prev_log_index(self.latest_log_index().into());
            request.set_prev_log_term(self.store.latest_log_term().unwrap().into());
            request.set_leader_commit(self.commit_index.into());
            request.init_entries(0);
        }
        actions.peer_messages.push((peer, Rc::new(message)));
    }

    /// Trigger an election timeout on the Raft replica.
    ///
    /// The provided RequestVoteRequest builder may be initialized with a message to send to each
    /// cluster peer.
    fn election_timeout(&mut self, actions: &mut Actions) {
        scoped_info!("ElectionTimeout");
        scoped_assert!(!self.is_leader());
        if self.peers.is_empty() {
            // Solitary replica special case; jump straight to leader status
            scoped_info!("transitioning to Leader");
            scoped_assert!(self.is_follower());
            scoped_assert!(self.store.voted_for().unwrap().is_none());
            self.store.inc_current_term().unwrap();
            self.store.set_voted_for(self.id).unwrap();
            let latest_log_index = self.latest_log_index();
            self.state = ReplicaState::Leader;
            self.leader_state.reinitialize(latest_log_index);
        } else {
            self.transition_to_candidate(actions);
        }
    }

    /// Transition this Replica to Leader state.
    ///
    /// The provided Actions instance will have an AppendEntriesRequest message
    /// added for each cluster peer.
    fn transition_to_leader(&mut self, actions: &mut Actions) {
        scoped_info!("transitioning to Leader");
        let current_term = self.current_term();
        let latest_log_index = self.latest_log_index();
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
        for &peer in self.peers() {
            actions.peer_messages.push((peer, message_rc.clone()));
        }

        actions.clear_timeouts = true;
    }

    /// Transition this Replica to Candidate state.
    ///
    /// The provided RequestVoteRequest message will be initialized with a message to send to each
    /// cluster peer.
    fn transition_to_candidate(&mut self, actions: &mut Actions) {
        scoped_info!("transitioning to Candidate");
        self.store.inc_current_term().unwrap();
        self.store.set_voted_for(self.id).unwrap();
        self.state = ReplicaState::Candidate;
        self.candidate_state.clear();
        self.candidate_state.record_vote(self.id);

        let current_term = self.current_term();
        let latest_index = self.latest_log_index();
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
        for &peer in self.peers() {
            actions.peer_messages.push((peer, message_rc.clone()));
        }
        actions.timeouts.push(ReplicaTimeout::Election);
    }

    /// Advance the commit index and apply committed entries to the state machine, if possible.
    fn advance_commit_index(&mut self) {
        scoped_assert!(self.is_leader());
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
                              leader: ServerId,
                              actions: &mut Actions) {
        scoped_info!("transitioning to Follower");
        self.store.set_current_term(term).unwrap();
        self.state = ReplicaState::Follower;
        self.follower_state.set_leader(leader);
        actions.clear_timeouts = true;
        actions.timeouts.push(ReplicaTimeout::Election);
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

    /// Returns the term of the latest applied log entry.
    fn latest_log_term(&self) -> Term {
        self.store.latest_log_term().unwrap()
    }

    /// Returns the index of the latest applied log entry.
    fn latest_log_index(&self) -> LogIndex {
        self.store.latest_log_index().unwrap()
    }

    /// Get the cluster quorum majority size.
    fn majority(&self) -> usize {
        let peers = self.peers.len();
        let cluster_members = peers.checked_add(1).expect(&format!("unable to support {} cluster members", peers));
        (cluster_members >> 1) + 1
    }
}

impl <S, M> fmt::Debug for Replica<S, M> where S: Store, M: StateMachine {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(fmt, "Replica {{ id: "));
        try!(fmt::Display::fmt(&self.id, fmt));
        try!(write!(fmt, ", state: "));
        try!(fmt::Debug::fmt(&self.state, fmt));
        try!(write!(fmt, ", term: "));
        try!(fmt::Display::fmt(&self.current_term(), fmt));
        try!(write!(fmt, ", index: "));
        try!(fmt::Display::fmt(&self.latest_log_index(), fmt));
        write!(fmt, " }}")
    }
}

#[cfg(test)]
mod test {

    extern crate env_logger;

    use std::collections::{HashSet, HashMap};
    use std::io::Cursor;
    use std::rc::Rc;

    use capnp::{MallocMessageBuilder, MessageBuilder, ReaderOptions};
    use capnp::serialize::{self, OwnedSpaceMessageReader};

    use ClientId;
    use ServerId;
    use replica::{Actions, Replica, ReplicaTimeout};
    use state_machine::NullStateMachine;
    use store::MemStore;

    type TestReplica = Replica<MemStore, NullStateMachine>;

    fn new_cluster(size: u64) -> HashMap<ServerId, TestReplica> {
        let ids: HashSet<ServerId> = (0..size).map(Into::into).collect();
        ids.iter().map(|&id| {
            let mut peers = ids.clone();
            peers.remove(&id);
            let store = MemStore::new();
            (id, Replica::new(id, peers, store, NullStateMachine))
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
    fn apply_actions(from: ServerId,
                     actions: Actions,
                     replicas: &mut HashMap<ServerId, TestReplica>)
                     -> Vec<(ClientId, Rc<MallocMessageBuilder>)> {

        fn inner(from: ServerId,
                 actions: Actions,
                 replicas: &mut HashMap<ServerId, TestReplica>,
                 client_messages: &mut Vec<(ClientId, Rc<MallocMessageBuilder>)>) {

            for &(peer, ref message) in &actions.peer_messages {
                let reader = into_reader(&**message);
                let mut actions = Actions::new();
                replicas.get_mut(&peer).unwrap().apply_peer_message(from, &reader, &mut actions);
                inner(peer, actions, replicas, client_messages);
            }
            client_messages.extend(actions.client_messages.into_iter());
        }

        let mut client_messages = vec![];
        inner(from, actions, replicas, &mut client_messages);
        client_messages
    }

    /// Elect `leader` as the leader of a cluster with the provided followers.
    /// The leader and the followers must be in the same term.
    fn elect_leader(leader: ServerId,
                    replicas: &mut HashMap<ServerId, TestReplica>) {
        let mut actions = Actions::new();
        replicas.get_mut(&leader).unwrap().election_timeout(&mut actions);
        let client_messages = apply_actions(leader, actions, replicas);
        assert!(client_messages.is_empty());
        assert!(replicas[&leader].is_leader());
    }

    /// Tests that a single-replica cluster will behave appropriately.
    ///
    /// The single replica should transition straight to the Leader state upon the first timeout.
    #[test]
    fn test_solitary_replica_transition_to_leader() {
        setup_test!("test_solitary_replica_transition_to_leader");
        let (_, mut replica) = new_cluster(1).into_iter().next().unwrap();
        assert!(replica.is_follower());

        let mut actions = Actions::new();
        replica.election_timeout(&mut actions);
        assert!(replica.is_leader());
        assert!(actions.peer_messages.is_empty());
        assert!(actions.client_messages.is_empty());
        assert!(actions.timeouts.is_empty());
    }

    /// A simple election test of a two-replica cluster.
    #[test]
    fn test_election_2() {
        setup_test!("test_election_2");
        let mut replicas = new_cluster(2);
        let replica_ids: Vec<ServerId> = replicas.keys().cloned().collect();
        let leader = &replica_ids[0];
        let follower = &replica_ids[1];
        elect_leader(leader.clone(), &mut replicas);

        assert!(replicas[leader].is_leader());
        assert!(replicas[follower].is_follower());
    }

    /// A simple election test of a three-replica cluster.
    #[test]
    fn test_election_3() {
        setup_test!("test_election_3");
        let mut replicas = new_cluster(3);
        let replica_ids: Vec<ServerId> = replicas.keys().cloned().collect();
        let leader = &replica_ids[0];
        elect_leader(leader.clone(), &mut replicas);

        assert!(replicas[leader].is_leader());
        assert!(replicas[&replica_ids[1]].is_follower());
        assert!(replicas[&replica_ids[2]].is_follower());
    }

    /// A simple election test of a five-replica cluster.
    #[test]
    fn test_election_5() {
        setup_test!("test_election_5");
        let mut replicas = new_cluster(5);
        let replica_ids: Vec<ServerId> = replicas.keys().cloned().collect();
        let leader = &replica_ids[0];
        elect_leader(leader.clone(), &mut replicas);

        assert!(replicas[leader].is_leader());
        assert!(replicas[&replica_ids[1]].is_follower());
        assert!(replicas[&replica_ids[2]].is_follower());
        assert!(replicas[&replica_ids[3]].is_follower());
        assert!(replicas[&replica_ids[4]].is_follower());
    }

    /// A simple election test of a six-replica cluster.
    #[test]
    fn test_election_6() {
        setup_test!("test_election_6");
        let mut replicas = new_cluster(6);
        let replica_ids: Vec<ServerId> = replicas.keys().cloned().collect();
        let leader = &replica_ids[0];
        elect_leader(leader.clone(), &mut replicas);

        assert!(replicas[leader].is_leader());
        assert!(replicas[&replica_ids[1]].is_follower());
        assert!(replicas[&replica_ids[2]].is_follower());
        assert!(replicas[&replica_ids[3]].is_follower());
        assert!(replicas[&replica_ids[4]].is_follower());
        assert!(replicas[&replica_ids[5]].is_follower());
    }

    /// A simple election test of a seven-replica cluster.
    #[test]
    fn test_election_7() {
        setup_test!("test_election_7");
        let mut replicas = new_cluster(7);
        let replica_ids: Vec<ServerId> = replicas.keys().cloned().collect();
        let leader = &replica_ids[0];
        elect_leader(leader.clone(), &mut replicas);

        assert!(replicas[leader].is_leader());
        assert!(replicas[&replica_ids[1]].is_follower());
        assert!(replicas[&replica_ids[2]].is_follower());
        assert!(replicas[&replica_ids[3]].is_follower());
        assert!(replicas[&replica_ids[4]].is_follower());
        assert!(replicas[&replica_ids[5]].is_follower());
        assert!(replicas[&replica_ids[6]].is_follower());
    }

    /// Tests the Raft heartbeating mechanism. The leader receives a heartbeat
    /// timeout, and in response sends an AppendEntries message to the follower.
    /// The follower in turn resets its election timout, and replies to the
    /// leader.
    #[test]
    fn test_heartbeat() {
        setup_test!("test_heartbeat");
        let mut replicas = new_cluster(2);
        let replica_ids: Vec<ServerId> = replicas.keys().cloned().collect();
        let leader_id = &replica_ids[0];
        let follower_id = &replica_ids[1];
        elect_leader(leader_id.clone(), &mut replicas);

        // Leader pings with a heartbeat timeout.
        let leader_append_entries = {
            let mut actions = Actions::new();
            let leader = replicas.get_mut(&leader_id).unwrap();
            leader.heartbeat_timeout(follower_id.clone(), &mut actions);

            let peer_message = actions.peer_messages.iter().next().unwrap();
            assert_eq!(peer_message.0, follower_id.clone());
            peer_message.1.clone()
        };
        let reader = into_reader(&*leader_append_entries);

        // Follower responds.
        let follower_response = {
            let mut actions = Actions::new();
            let follower = replicas.get_mut(&follower_id).unwrap();
            follower.apply_peer_message(leader_id.clone(), &reader, &mut actions);

            let election_timeout = actions.timeouts.iter().next().unwrap();
            assert_eq!(election_timeout, &ReplicaTimeout::Election);

            let peer_message = actions.peer_messages.iter().next().unwrap();
            assert_eq!(peer_message.0, leader_id.clone());
            peer_message.1.clone()
        };
        let reader = into_reader(&*follower_response);

        // Leader applies and sends back a heartbeat to establish leadership.
        let leader = replicas.get_mut(&leader_id).unwrap();
        let mut actions = Actions::new();
        leader.apply_peer_message(follower_id.clone(), &reader, &mut actions);
        let heartbeat_timeout = actions.timeouts.iter().next().unwrap();
        assert_eq!(heartbeat_timeout, &ReplicaTimeout::Heartbeat(follower_id.clone()));
    }
}
