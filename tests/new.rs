extern crate raft;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::mpsc;

use raft::Raft;
use raft::store::MemStore;
use raft::state_machine::ChannelStateMachine;

pub fn new_cluster(size: u16) -> Vec<(Raft, mpsc::Receiver<Vec<u8>>)> {
    // the actual port does not matter here since they won't be bound
    let addrs: HashSet<SocketAddr> =
        (0..size).map(|port| FromStr::from_str(&format!("127.0.0.1:200{}", port)).unwrap()).collect();

    addrs.iter().map(|addr| {
        let mut peers = addrs.clone();
        peers.remove(addr);
        let store = MemStore::new();
        let (state_machine, recv) = ChannelStateMachine::new();
        println!("Spawning new Raft on {}", addr);
        (Raft::new(addr.clone(), peers, store, state_machine), recv)
    }).collect()
}
