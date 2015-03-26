use std::{error, io, result};
use std::fmt::{self, Debug};
use std::sync::mpsc;

pub trait StateMachine: Debug + Send + 'static {

    type Error: Debug + error::Error + Send + 'static;

    /// Applies a command to the state machine.
    fn apply(&mut self, command: &[u8]) -> result::Result<(), Self::Error>;

    /// Take a snapshot of the state machine.
    fn snapshot(&self) -> result::Result<Vec<u8>, Self::Error>;

    /// Restore a snapshot of the state machine.
    fn restore_snapshot(&mut self, snapshot: Vec<u8>) -> result::Result<(), Self::Error>;
}

/// A state machine that simply redirects all commands to a channel.
///
/// This state machine is chiefly meant for testing.
pub struct ChannelStateMachine {
    tx: mpsc::Sender<Vec<u8>>
}
impl ChannelStateMachine {
    pub fn new() -> (ChannelStateMachine, mpsc::Receiver<Vec<u8>>) {
        let (tx, recv) = mpsc::channel();
        (ChannelStateMachine { tx: tx }, recv)
    }
}

impl StateMachine for ChannelStateMachine {

    type Error = mpsc::SendError<Vec<u8>>;

    fn apply(&mut self, command: &[u8]) -> result::Result<(), mpsc::SendError<Vec<u8>>> {
        self.tx.send(command.to_vec())
    }

    fn snapshot(&self) -> result::Result<Vec<u8>, mpsc::SendError<Vec<u8>>> {
        Ok(Vec::new())
    }

    fn restore_snapshot(&mut self, _snapshot: Vec<u8>) -> result::Result<(), mpsc::SendError<Vec<u8>>> {
        Ok(())
    }
}

impl Debug for ChannelStateMachine {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "ChannelStateMachine")
    }
}

/// A state machine with no states.
#[derive(Debug)]
pub struct NullStateMachine;

impl StateMachine for NullStateMachine {

    // The error type is not significant to this state machine
    type Error = io::Error;

    fn apply(&mut self, _command: &[u8]) -> result::Result<(), io::Error> {
        Ok(())
    }

    fn snapshot(&self) -> result::Result<Vec<u8>, io::Error> {
        Ok(Vec::new())
    }

    fn restore_snapshot(&mut self, _snapshot: Vec<u8>) -> result::Result<(), io::Error> {
        Ok(())
    }
}
