use std::collections::{HashMap, HashSet};

use LogIndex;
use ServerId;

/// Replicas can be in one of three state:
///
/// * `Follower` - which replicates AppendEntries requests and votes for it's leader.
/// * `Leader` - which leads the cluster by serving incoming requests, ensuring
///              data is replicated, and issuing heartbeats.
/// * `Candidate` -  which campaigns in an election and may become a `Leader`
///                  (if it gets enough votes) or a `Follower`, if it hears from
///                  a `Leader`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplicaState {
    Follower,
    Candidate,
    Leader,
}

/// The state associated with a Raft replica in the `Leader` state.
#[derive(Clone, Debug)]
pub struct LeaderState {
    next_index: HashMap<ServerId, LogIndex>,
    match_index: HashMap<ServerId, LogIndex>,
}

impl LeaderState {

    /// Returns a new `LeaderState` struct.
    ///
    /// # Arguments
    ///
    /// * `latest_log_index` - The index of the leader's most recent log entry at the
    ///                        time of election.
    /// * `peers` - The set of peer cluster members.
    pub fn new(latest_log_index: LogIndex, peers: &HashSet<ServerId>) -> LeaderState {
        let next_index = peers.iter().cloned().map(|peer| (peer, latest_log_index + 1)).collect();
        let match_index = peers.iter().cloned().map(|peer| (peer, LogIndex::from(0))).collect();

        LeaderState {
            next_index: next_index,
            match_index: match_index,
        }
    }

    /// Returns the next log entry index of the follower.
    pub fn next_index(&mut self, follower: &ServerId) -> LogIndex {
        self.next_index[follower]
    }

    /// Sets the next log entry index of the follower.
    pub fn set_next_index(&mut self, follower: ServerId, index: LogIndex) {
        self.next_index.insert(follower, index);
    }

    /// Returns the index of the highest log entry known to be replicated on
    /// the follower.
    pub fn match_index(&self, follower: &ServerId) -> LogIndex {
        self.match_index[follower]
    }

    /// Sets the index of the highest log entry known to be replicated on the
    /// follower.
    pub fn set_match_index(&mut self, follower: ServerId, index: LogIndex) {
        self.match_index.insert(follower, index);
    }

    /// Counts the number of followers containing the given log index.
    pub fn count_match_indexes(&self, index: LogIndex) -> usize {
        self.match_index.values().filter(|&&i| i >= index).count()
    }

    /// Reinitializes the state following an election.
    pub fn reinitialize(&mut self, latest_log_index: LogIndex) {
        for (_, next_index) in self.next_index.iter_mut() {
            *next_index = latest_log_index + 1;
        }
        for (_, match_index) in self.match_index.iter_mut() {
            *match_index = LogIndex::from(0);
        }
    }
}

/// The state associated with a Raft replica in the `Candidate` state.
#[derive(Clone, Debug)]
pub struct CandidateState {
    granted_votes: HashSet<ServerId>,
}

impl CandidateState {

    /// Creates a new `CandidateState`.
    pub fn new() -> CandidateState {
        CandidateState { granted_votes: HashSet::new() }
    }

    /// Records a vote from `voter`.
    pub fn record_vote(&mut self, voter: ServerId) {
        self.granted_votes.insert(voter);
    }

    /// Returns the number of votes.
    pub fn count_votes(&self) -> usize {
        self.granted_votes.len()
    }

    /// Clears the vote count.
    pub fn clear(&mut self) {
        self.granted_votes.clear();
    }
}

/// The state associated with a Raft replica in the `Follower` state.
#[derive(Clone, Debug)]
pub struct FollowerState {
    /// The most recent leader of the follower. The leader is not guaranteed to be active, so this
    /// should only be used as a hint.
    pub leader: Option<ServerId>,
}

impl FollowerState {

    /// Returns a new `FollowerState`.
    pub fn new() -> FollowerState {
        FollowerState { leader: None }
    }

    /// Sets a new leader.
    pub fn set_leader(&mut self, leader: ServerId) {
        self.leader = Some(leader)
    }
}
