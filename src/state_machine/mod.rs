//! The state machine which Raft applied commited entries too.
//!
//! This trait is meant to be implemented such that the commands issued to it via `apply()` will
//! be reflected in your consuming application. Commands send via `apply()` have been committed
//! in the cluser. Unlike `store`, your application should consume data produced by this and
//! accept it as truth.

mod channel;
mod null;

use std::{error, io, result};
use std::fmt::{self, Debug};
use std::sync::mpsc;

pub use state_machine::channel::ChannelStateMachine;
pub use state_machine::null::NullStateMachine;

pub trait StateMachine: Debug + Send + 'static {

    type Error: Debug + error::Error + Send + 'static;

    /// Applies a command to the state machine.
    fn apply(&mut self, command: &[u8]) -> result::Result<(), Self::Error>;

    /// Take a snapshot of the state machine.
    fn snapshot(&self) -> result::Result<Vec<u8>, Self::Error>;

    /// Restore a snapshot of the state machine.
    fn restore_snapshot(&mut self, snapshot: Vec<u8>) -> result::Result<(), Self::Error>;
}
