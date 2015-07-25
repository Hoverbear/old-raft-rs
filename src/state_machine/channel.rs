use std::result;
use std::fmt::{self, Debug};
use std::sync::mpsc;

use state_machine::StateMachine;


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

    fn apply(&mut self, command: &[u8]) -> result::Result<Vec<u8>, mpsc::SendError<Vec<u8>>> {
        self.tx.send(command.to_vec()).map(|_| Vec::new())
    }

    fn query(&self, query: &[u8]) -> result::Result<Vec<u8>, Self::Error> {
        unimplemented!()
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
