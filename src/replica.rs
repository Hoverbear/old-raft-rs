use std::collections::{hash_map, HashMap, HashSet};
use std::io;
use std::net::SocketAddr;

use capnp::{MallocMessageBuilder, MessageBuilder};

use {LogIndex, Term};
use messages_capnp::{
    append_entries_request,
    append_entries_response,
    client_request,
    request_vote_request,
    request_vote_response,
};
use state_machine::StateMachine;
use store::Store;

/// Replicas can be in one of three state:
///
/// * `Follower` - which replicates AppendEntries requests and votes for it's leader.
/// * `Leader` - which leads the cluster by serving incoming requests, ensuring
///              data is replicated, and issuing heartbeats.
/// * `Candidate` -  which campaigns in an election and may become a `Leader`
///                  (if it gets enough votes) or a `Follower`, if it hears from
///                  a `Leader`.
#[derive(Clone, Debug)]
enum ReplicaState {
    Follower,
    Candidate(CandidateState),
    Leader(LeaderState),
}

#[derive(Clone, Debug)]
struct LeaderState {
    last_index: LogIndex,
    next_index: HashMap<SocketAddr, LogIndex>,
    match_index: HashMap<SocketAddr, LogIndex>,
}

impl LeaderState {

    /// Returns a new `LeaderState` struct.
    ///
    /// # Arguments
    ///
    /// * `last_index` - The index of the leader's most recent log entry at the
    ///                  time of election.
    pub fn new(last_index: LogIndex) -> LeaderState {
        LeaderState {
            last_index: last_index,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
        }
    }

    /// Returns the next log entry index of the follower node.
    pub fn next_index(&mut self, node: SocketAddr) -> LogIndex {
        match self.next_index.entry(node) {
            hash_map::Entry::Occupied(entry) => *entry.get(),
            hash_map::Entry::Vacant(entry) => *entry.insert(self.last_index + 1),
        }
    }

    /// Sets the next log entry index of the follower node.
    pub fn set_next_index(&mut self, node: SocketAddr, index: LogIndex) {
        self.next_index.insert(node, index);
    }

    /// Returns the index of the highest log entry known to be replicated on
    /// the follower node.
    pub fn match_index(&self, node: SocketAddr) -> LogIndex {
        *self.match_index.get(&node).unwrap_or(&LogIndex(0))
    }

    /// Sets the index of the highest log entry known to be replicated on the
    /// follower node.
    pub fn set_match_index(&mut self, node: SocketAddr, index: LogIndex) {
        self.match_index.insert(node, index);
    }

    /// Counts the number of follower nodes containing the given log index.
    pub fn count_match_indexes(&self, index: LogIndex) -> usize {
        self.match_index.values().filter(|&&i| i >= index).count()
    }
}

#[derive(Clone, Debug)]
struct CandidateState {
    granted_votes: usize,
}

/// A replica of a Raft distributed state machine. A Raft replica controls a client state machine,
/// to which it applies commands in a globally consistent order.
pub struct Replica<S, M> {
    addr: SocketAddr,
    peers: HashSet<SocketAddr>,

    store: S,
    state_machine: M,

    state: ReplicaState,

    /// Volatile State
    commit_index: LogIndex,
    last_applied: LogIndex,
}

impl <S, M> Replica<S, M> where S: Store, M: StateMachine {

    pub fn new(addr: SocketAddr,
               peers: HashSet<SocketAddr>,
               store: S,
               state_machine: M)
               -> Replica<S, M> {
        Replica {
            addr: addr,
            peers: peers,
            state: ReplicaState::Follower,
            store: store,
            state_machine: state_machine,
            commit_index: LogIndex(0),
            last_applied: LogIndex(0),
        }
    }

    /// Apply an append entries request to the Raft replica.
    pub fn append_entries_request(&mut self,
                                  from: SocketAddr,
                                  request: append_entries_request::Reader,
                                  mut response: append_entries_response::Builder) {
        assert!(self.peers.contains(&from), "Received append entries request from unknown node {}.", from);
        debug!("ID {}: FROM {} HANDLE append_entries_request", self.addr, from);

        let leader_term = Term(request.get_term());
        let current_term = self.store.current_term().unwrap();

        if leader_term < current_term {
            response.set_stale_term(current_term.into());
            return;
        }

        match self.state {
            ReplicaState::Follower => {
                let prev_log_index = LogIndex(request.get_prev_log_index());
                let prev_log_term = Term(request.get_prev_log_term());

                let local_latest_index = self.store.latest_index().unwrap();
                if local_latest_index < prev_log_index {
                    response.set_inconsistent_prev_entry(());
                } else {
                    let (existing_term, _) = self.store.entry(prev_log_index).unwrap();
                    if existing_term != prev_log_term {
                        self.store.truncate_entries(prev_log_index).unwrap();
                        response.set_inconsistent_prev_entry(());
                    } else {
                        let entries = request.get_entries().unwrap();
                        let num_entries = entries.len();
                        if num_entries > 0 {
                            let mut entries_vec = Vec::with_capacity(num_entries as usize);
                            for i in 0..num_entries {
                                entries_vec.push((leader_term, entries.get(i).unwrap()));
                            }
                            self.store.append_entries(prev_log_index + 1, &entries_vec).unwrap();
                        }
                        //response.set_success(());
                    }
                }
            },
            ReplicaState::Candidate(..) | ReplicaState::Leader(..) => {
                // recognize the new leader, return to follower state, and apply the entries
                self.state = ReplicaState::Follower;
                return self.append_entries_request(from, request, response)
            },
        }
    }

    /// Apply an append entries response to the Raft replica.
    pub fn append_entries_response(&mut self, from: SocketAddr, response: append_entries_response::Reader) {
        assert!(self.peers.contains(&from), "Received append entries response from unknown node {}.", from);
        debug!("ID {}: FROM {} HANDLE append_entries_response", self.addr, from);

        match self.state {
            ReplicaState::Leader(ref leader_state) => {

            },
            ReplicaState::Follower | ReplicaState::Candidate(..) => {
            },
        }

        match response.which() {
            Ok(append_entries_response::Success(_)) => {
            },
            Ok(append_entries_response::StaleTerm(_)) => {
            },
            Ok(append_entries_response::InconsistentPrevEntry(_)) => {
            },
            Ok(append_entries_response::InternalError(_)) => {
            },
            Err(_) => error!("Append Entries Response type not recognized"),
        }
    }

    /// Apply a request vote request to the Raft replica.
    pub fn request_vote_request(&mut self,
                                candidate: SocketAddr,
                                request: request_vote_request::Reader,
                                mut response: request_vote_response::Builder) {
        assert!(self.peers.contains(&candidate), "Received request vote request from unknown node {}.", candidate);
        debug!("ID {}: FROM {} HANDLE request_vote_request", self.addr, candidate);
        let candidate_term = Term(request.get_term());
        let candidate_index = LogIndex(request.get_last_log_index());
        let local_term = self.store.current_term().unwrap();
        let local_index = self.store.latest_index().unwrap();

        if candidate_term < local_term {
            response.set_stale_term(local_term.into());
        } else if candidate_term == local_term && candidate_index < local_index {
            response.set_inconsistent_log(());
        } else if candidate_term == local_term && self.store.voted_for().unwrap().is_some() {
            response.set_already_voted(());
        } else {
            if candidate_term != local_term {
                self.store.set_current_term(candidate_term);
            }
            self.store.set_voted_for(Some(candidate));
            response.set_granted(candidate_term.into());
        }
    }

    /// Apply a request vote response to the Raft replica.
    pub fn request_vote_response(&mut self, from: SocketAddr, response: request_vote_response::Reader) {
        assert!(self.peers.contains(&from), "Received request vote response from unknown node {}.", from);
        debug!("ID {}: FROM {} HANDLE request_vote_response", self.addr, from);

    }

    /// Apply a client request to the Raft replica.
    pub fn client_request(&mut self, from: SocketAddr, request: client_request::Reader) {
        debug!("ID {}: FROM {} HANDLE client_request", self.addr, from);
    }

    /// Trigger a timeout on the Raft replica.
    pub fn timeout(&mut self) {
        debug!("ID {}: HANDLE timeout", self.addr);
    }

    /// Get the cluster quorum majority size.
    fn majority(&self) -> usize {
        let peers = self.peers.len();
        let cluster_members = peers.checked_add(1).expect(&format!("unable to support {} cluster members", peers));
        (cluster_members >> 1) + 1
    }
}
