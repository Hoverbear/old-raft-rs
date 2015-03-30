mod mem;

use std::error;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::result;

use LogIndex;
use Term;

pub use store::mem::{MemStore, Error};

/// A store of persistent Raft state.
pub trait Store: Clone + Debug + Send + 'static {

    type Error: error::Error + Debug + Sized + 'static;

    /// Returns the latest known term.
    fn current_term(&self) -> result::Result<Term, Self::Error>;

    /// Sets the current term to the provided value. The provided term must be greater than
    /// the current term. The `voted_for` value will be reset`.
    fn set_current_term(&mut self, term: Term) -> result::Result<(), Self::Error>;

    /// Increment the current term. The `voted_for` value will be reset.
    fn inc_current_term(&mut self) -> result::Result<Term, Self::Error>;

    /// Returns the candidate id of the candidate voted for in the current term (or none).
    fn voted_for(&self) -> result::Result<Option<SocketAddr>, Self::Error>;

    /// Sets the candidate id voted for in the current term.
    fn set_voted_for(&mut self, address: SocketAddr) -> result::Result<(), Self::Error>;

    /// Returns the index of the latest persisted log entry (0 if the log is empty).
    fn latest_log_index(&self) -> result::Result<LogIndex, Self::Error>;

    /// Returns the term of the latest persisted log entry (0 if the log is empty).
    fn latest_log_term(&self) -> result::Result<Term, Self::Error>;

    /// Returns the entry at the provided log index.
    ///
    /// # Panic
    ///
    /// This method will panic if the index greater than the largest index.
    fn entry(&self, index: LogIndex) -> result::Result<(Term, &[u8]), Self::Error>;

    /// Appends the provided entries to the log beginning at the given index.
    fn append_entries(&mut self, from: LogIndex, entries: &[(Term, &[u8])]) -> result::Result<(), Self::Error>;
}
