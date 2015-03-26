use std::collections::{hash_map, HashMap, VecDeque};
use std::net::SocketAddr;

use uuid::Uuid;

use state::NodeState::{Leader, Follower, Candidate};
use state::TransactionState::{Polling, Accepted, Rejected};
use LogIndex;

/// Volatile state
#[derive(Copy)]
pub struct VolatileState {
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
}

#[derive(Clone)]
pub struct LeaderState {
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

/// Nodes can either be:
///
///   * A `Follower`, which replicates AppendEntries requests and votes for it's leader.
///   * A `Leader`, which leads the cluster by serving incoming requests, ensuring data is
///     replicated, and issuing heartbeats..
///   * A `Candidate`, which campaigns in an election and may become a `Leader` (if it gets enough
///     votes) or a `Follower`, if it hears from a `Leader`.
#[derive(Clone)]
pub enum NodeState {
    Follower(VecDeque<Transaction>),
    Leader(LeaderState),
    Candidate(HashMap<SocketAddr, Transaction>),
}

#[derive(PartialEq, Eq, Clone)]
pub struct Transaction {
    pub uuid: Uuid,
    pub state: TransactionState,
}

/// Used to signify the state of of a Request/Response pair. This is only needed
/// on the original sender... not on the reciever.
#[derive(PartialEq, Eq, Copy, Clone)]
pub enum TransactionState {
    Polling,
    Accepted,
    Rejected,
}
