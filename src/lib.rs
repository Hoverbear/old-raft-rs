#![crate_name = "raft"]
#![crate_type="lib"]
#![doc(html_logo_url = "https://raw.githubusercontent.com/Hoverbear/raft/master/raft.png")]
#![doc(html_root_url = "https://hoverbear.github.io/raft/raft/")]

#![feature(convert)]

extern crate capnp;
extern crate mio;
extern crate rand;
extern crate rustc_serialize;
extern crate uuid;
#[macro_use] extern crate log;

pub mod state_machine;
pub mod store;

mod node;
mod replica;
mod state;

mod messages_capnp {
    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/messages_capnp.rs"));
}

use std::{io, ops};
use std::collections::HashSet;
use std::fmt::Debug;
use std::net::SocketAddr;

// Data structures.
use store::Store;
use node::RaftNode;
use state_machine::StateMachine;

/// This is the primary interface with a `RaftNode` in the cluster. Creating a new `Raft` client
/// will, for now, automatically spawn a `RaftNode` with the relevant parameters. This may be
/// changed in the future. This is based on the assumption that any consuming appliaction
/// interacting with a Raft cluster will also be a participant.
pub struct Raft {
    current_leader: Option<SocketAddr>,
    related_raftnode: SocketAddr, // Not RaftNode because we move that to another thread.
    cluster_members: HashSet<SocketAddr>,
}

impl Raft {
    /// Create a new `Raft` client that has a cooresponding `RaftNode` attached. Note that this
    /// `Raft` may not necessarily interact with the cooreponding `RaftNode`, it will interact with
    /// the `Leader` of a cluster in almost all cases.
    pub fn new<S, M>(addr: SocketAddr,
                     cluster_members: HashSet<SocketAddr>,
                     store: S,
                     state_machine: M)
                     -> Raft
    where S: Store, M: StateMachine {
        let mut peers = cluster_members.clone();
        peers.remove(&addr);
        RaftNode::<S, M>::spawn(addr, peers, store, state_machine);
        // Store relevant information.
        Raft {
            current_leader: None,
            related_raftnode: addr,
            cluster_members: cluster_members,
        }
    }
    pub fn append(entry: &[u8]) -> io::Result<()> {
        unimplemented!();
    }
}

/// The term of a log entry.
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, RustcEncodable, RustcDecodable)]
pub struct Term(u64);
impl From<u64> for Term {
    fn from(val: u64) -> Term {
        Term(val)
    }
}
impl Into<u64> for Term {
    fn into(self) -> u64 {
        self.0
    }
}
impl ops::Add<u64> for Term {
    type Output = Term;
    fn add(self, rhs: u64) -> Term {
        Term(self.0.checked_add(rhs).expect("overflow while incrementing Term"))
    }
}
impl ops::Sub<u64> for Term {
    type Output = Term;
    fn sub(self, rhs: u64) -> Term {
        Term(self.0.checked_sub(rhs).expect("underflow while decrementing Term"))
    }
}

/// The index of a log entry.
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, RustcEncodable, RustcDecodable)]
pub struct LogIndex(u64);
impl From<u64> for LogIndex {
    fn from(val: u64) -> LogIndex {
        LogIndex(val)
    }
}
impl Into<u64> for LogIndex {
    fn into(self) -> u64 {
        self.0
    }
}
impl ops::Add<u64> for LogIndex {
    type Output = LogIndex;
    fn add(self, rhs: u64) -> LogIndex {
        LogIndex(self.0.checked_add(rhs).expect("overflow while incrementing LogIndex"))
    }
}
impl ops::Sub<u64> for LogIndex {
    type Output = LogIndex;
    fn sub(self, rhs: u64) -> LogIndex {
        LogIndex(self.0.checked_sub(rhs).expect("underflow while decrementing LogIndex"))
    }
}
