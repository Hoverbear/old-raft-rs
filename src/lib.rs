#![crate_name = "raft"]
#![crate_type="lib"]
#![doc(html_logo_url = "https://raw.githubusercontent.com/Hoverbear/raft/master/raft.png")]
#![doc(html_root_url = "https://hoverbear.github.io/raft/raft/")]

#![feature(
    collections,
    convert,
    io,
)]

extern crate rustc_serialize;
extern crate uuid;
extern crate rand;
#[macro_use] extern crate log;
extern crate mio;
pub mod state;
pub mod state_machine;
pub mod store;
pub mod node;
pub mod interchange;

use std::{io, str, thread};
use std::collections::{HashMap, HashSet, VecDeque, BitSet};
use std::fmt::Debug;
use std::num::Int;
use std::net::SocketAddr;
use std::ops;
use std::marker::PhantomData;

use rand::{thread_rng, Rng, ThreadRng};
use rustc_serialize::{json, Encodable, Decodable};

// MIO
use mio::udp::UdpSocket;
use mio::{Token, EventLoop, Handler, ReadHint};

// Data structures.
use state::{LeaderState, VolatileState};
use state::NodeState::{Leader, Follower, Candidate};
use state::{NodeState, TransactionState, Transaction};
use store::Store;
use node::RaftNode;

/// This is the primary interface with a `RaftNode` in the cluster. Creating a new `Raft` client
/// will, for now, automatically spawn a `RaftNode` with the relevant parameters. This may be
/// changed in the future. This is based on the assumption that any consuming appliaction
/// interacting with a Raft cluster will also be a participant.
pub struct Raft<T, S>
where T: Encodable + Decodable + Clone + Debug + Send + 'static,
      S: Store + Debug
{
    current_leader: Option<SocketAddr>,
    related_raftnode: SocketAddr, // Not RaftNode because we move that to another thread.
    cluster_members: HashSet<SocketAddr>,
    // TODO: Can we get rid of these?
    phantom_store: PhantomData<S>,
    phantom_type: PhantomData<T>,
}

impl<T, S> Raft<T,S>
where T: Encodable + Decodable + Clone + Debug + Send + 'static,
      S: Store + Debug {
    /// Create a new `Raft` client that has a cooresponding `RaftNode` attached. Note that this
    /// `Raft` may not necessarily interact with the cooreponding `RaftNode`, it will interact with
    /// the `Leader` of a cluster in almost all cases.
    pub fn new(address: SocketAddr, cluster_members: HashSet<SocketAddr>, store: S) -> Raft<T, S> {
        // TODO: How do we know if this panics?
        RaftNode::<T, S>::spawn(address, cluster_members.clone(), store);
        // Store relevant information.
        Raft {
            current_leader: None,
            related_raftnode: address,
            cluster_members: cluster_members,
            phantom_store: PhantomData,
            phantom_type: PhantomData,
        }
    }
    pub fn append(entries: Vec<T>) -> io::Result<()> {
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
