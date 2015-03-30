extern crate raft;
mod new;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::mpsc;

use raft::Raft;
use raft::store::MemStore;
use raft::state_machine::ChannelStateMachine;

use new::new_cluster;

fn hello() {
    let mut nodes = new_cluster(3);
    let sent_command = b"Hello";
    // Push
    nodes[0].0.append(sent_command)
        .ok().expect("Couldn't append.");
    // Retrieve.
    let recieved_command = nodes[0].1.recv()
        .ok().expect("Couldn't recv.");
    assert_eq!(sent_command, recieved_command);
}
