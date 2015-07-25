//! A `StateMachine` is a single instance of a distributed application. It is the `raft` libraries
//! responsibility to take commands from the `Client` and apply them to each `StateMachine`
//! instance in a globally consistent order.
//!
//! The `StateMachine` is interface is intentionally generic so that any distributed application
//! needing consistent state can be built on it.  For instance, a distributed hash table
//! application could implement `StateMachine`, with commands corresponding to `insert`, and
//! `remove`. The `raft` library would guarantee that the same order of `insert` and `remove`
//! commands would be seen by all consensus modules.
use std::{error, result};
use std::fmt::Debug;

mod channel;
mod null;
mod register;
#[cfg(feature="hashmap")] mod hashmap;

pub use state_machine::channel::ChannelStateMachine;
pub use state_machine::null::NullStateMachine;
pub use state_machine::register::RegisterStateMachine;

/// This trait is meant to be implemented such that the commands issued to it via `apply()` will
/// be reflected in your consuming application. Commands sent via `apply()` have been committed
/// in the cluser. Unlike `store`, your application should consume data produced by this and
/// accept it as truth.
pub trait StateMachine: Debug + Send + 'static {

    type Error: Debug + error::Error + Send + 'static;

    /// Applies a command to the state machine.
    /// Returns an application-specific result value.
    fn apply(&mut self, command: &[u8]) -> result::Result<Vec<u8>, Self::Error>;

    /// Queries a value of the state machine. Does not go through the durable log, or mutate the
    /// state machine.
    /// Returns an application-specific result value.
    fn query(&self, query: &[u8]) -> result::Result<Vec<u8>, Self::Error>;

    /// Take a snapshot of the state machine.
    fn snapshot(&self) -> result::Result<Vec<u8>, Self::Error>;

    /// Restore a snapshot of the state machine.
    fn restore_snapshot(&mut self, snapshot: Vec<u8>) -> result::Result<(), Self::Error>;
}
