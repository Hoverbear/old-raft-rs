//! Creates a dummy Raft server with a no-op state machine.
//!
//! Usage:
//! ```bash
//! $ ./dummy <server-id> <server-address> [<peer-id> <peer-address>]*
//! ```

extern crate env_logger;
extern crate raft;

use std::env;
use std::net::SocketAddr;
use std::str::FromStr;
use raft::{state_machine, store, ServerId};

const USAGE: &'static str =
    "Usage:\n\t./dummy <server-id> <server-address> [<peer-id> <peer-address>]*";

fn main() {
    env_logger::init().unwrap();
    let args: Vec<String> = env::args().skip(1).collect();
    if args.len() < 2 || args.len() % 2 != 0 {
        panic!(USAGE);
    }

    let servers: Vec<(ServerId, SocketAddr)> = args.chunks(2).map(|chunk| {
        let id: u64 = FromStr::from_str(&chunk[0]).unwrap();
        let addr: SocketAddr = FromStr::from_str(&chunk[1]).unwrap();
        (ServerId::from(id), addr)
    }).collect();

    let (id, addr) = servers[0];

    let store = store::MemStore::new();
    let state_machine = state_machine::NullStateMachine;

    raft::Server::run(id, addr,
                      servers.into_iter().skip(1).collect(),
                      store,
                      state_machine).unwrap();
}
