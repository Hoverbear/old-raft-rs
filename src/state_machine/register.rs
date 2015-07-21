use std::io::{Error, Result};

use state_machine::StateMachine;

/// A state machine that holds a single mutable value.
#[derive(Debug)]
pub struct RegisterStateMachine {
    value: Vec<u8>,
}

impl RegisterStateMachine {
    pub fn new() -> RegisterStateMachine {
        RegisterStateMachine { value: vec![] }
    }
}

impl StateMachine for RegisterStateMachine {

    type Error = Error;

    fn apply(&mut self, new_value: &[u8]) -> Result<()> {
        self.value.clear();
        self.value.extend(new_value);
        Ok(())
    }

    fn snapshot(&self) -> Result<Vec<u8>> {
        Ok(self.value.clone())
    }

    fn restore_snapshot(&mut self, snapshot_value: Vec<u8>) -> Result<()> {
        self.value = snapshot_value;
        Ok(())
    }
}
