extern crate "rustc-serialize" as rustc_serialize;
extern crate uuid;

use uuid::Uuid;
use rustc_serialize::{json, Encodable, Decodable};
use types::NodeState::{Leader, Follower, Candidate};
use types::TransactionState::{Polling, Accepted, Rejected};
use std::fs::File;
use std::io;
use std::io::{ReadExt, Seek};
use std::old_io::IoError;

/// Persistent state
/// **Must be updated to stable storage before RPC response.**
pub struct PersistentState<T: Encodable + Decodable + Send + Clone> {
    pub current_term: u64,
    pub voted_for: Option<u64>, // request_vote cares if this is `None`
    log: File,
    pub last_index: u64,             // The last index of the file.
}

impl<T: Encodable + Decodable + Send + Clone> PersistentState<T> {
    pub fn new(current_term: u64, log_path: Path) -> PersistentState<T> {
        PersistentState {
            current_term: current_term,
            voted_for: None,
            log: File::create(&log_path).unwrap(),
            last_index: 0,
        }
    }
    pub fn append_entries(&mut self, prev_log_index: u64, prev_log_term: u64,
                      entries: Vec<(u64, T)>) -> io::Result<()> {
        try!(self.move_to(prev_log_index + 1));
        Ok(())
    }
    /// Returns the number of bytes containing `line` lines.
    /// TODO: Cache?
    fn move_to(&mut self, line: u64) -> io::Result<u64> {
        let mut lines_read = 0u64;
        // Go until we've reached `from` new lines.
        let _ = self.log.by_ref().chars().skip_while(|opt| {
            match *opt {
                Ok(val) => {
                    if val == '\n' {
                        lines_read += 1;
                        if lines_read == line {
                            false // At right location.
                        } else {
                            true // Not done yet, more lines to go.
                        }
                    } else {
                        true // Not a new line.
                    }
                },
                _ => false // At EOF. Nothing to purge.
            }
        }).next(); // Side effects.
        // We're at the first byte of a new line.
        self.log.seek(io::SeekFrom::Current(-1)) // Take the last byte.
    }
    /// Removes all entries from `from` to the last entry, inclusively.
    pub fn purge_from(&mut self, from: u64) -> io::Result<()> {
        let position = try!(self.move_to(from));
        self.log.set_len(position); // Chop off the file at the given position.
        Ok(())
    }
    pub fn retrieve_entries(&self, start: u64, end: u64) -> Result<Vec<(u64, T)>, IoError> {
        unimplemented!()
    }
    pub fn retrieve_entry(&self, entry: u64) -> Result<(u64, T), IoError> {
        unimplemented!()
    }
}

/// Volatile state
#[derive(Copy)]
pub struct VolatileState {
    pub commit_index: u64,
    pub last_applied: u64
}

/// Leader Only
/// **Reinitialized after election.**
#[derive(PartialEq, Eq, Clone)]
pub struct LeaderState {
    pub next_index: Vec<u64>,
    pub match_index: Vec<u64>
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
