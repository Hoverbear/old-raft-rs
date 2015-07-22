use std::{io, result};

use state_machine::StateMachine;

/// A state machine with no states.
#[derive(Debug)]
pub struct NullStateMachine;

impl StateMachine for NullStateMachine {

    // The error type is not significant to this state machine
    type Error = io::Error;

    fn apply(&mut self, _command: &[u8]) -> result::Result<Vec<u8>, io::Error> {
        Ok(Vec::new())
    }

    fn query(&self, query: &[u8]) -> result::Result<Vec<u8>, io::Error> {
        Ok(Vec::new())
    }

    fn snapshot(&self) -> result::Result<Vec<u8>, io::Error> {
        Ok(Vec::new())
    }

    fn restore_snapshot(&mut self, _snapshot: Vec<u8>) -> result::Result<(), io::Error> {
        Ok(())
    }
}
