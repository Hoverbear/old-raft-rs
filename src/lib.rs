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
use std::net::TcpStream;
use std::str::FromStr;

use rustc_serialize::{Encodable, Decodable};
// Data structures.
use store::Store;
use node::RaftNode;
use state_machine::StateMachine;

// Cap'n Proto
use capnp::serialize_packed;
use capnp::{MessageBuilder, MessageReader, ReaderOptions, MallocMessageBuilder};
use messages_capnp::{
    client_request,
    client_response,
};

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
    /// *Note:* All requests are blocking, by design from the Raft paper.
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

    /// Appends an entry to the replicated log. This will only return once it's properly replicated
    /// to a majority of nodes.
    pub fn append(&mut self, entry: &[u8]) -> io::Result<()> {
        let mut message = MallocMessageBuilder::new_default();
        if self.current_leader.is_none() { try!(self.refresh_leader()); }
        {
            let mut client_req = message.init_root::<client_request::Builder>();
            client_req.set_append(entry)
        }
        // We know current leader `is_some()` because `refresh_leader()` didn't fail.
        let mut socket = try!(TcpStream::connect(self.current_leader.unwrap())); // TODO: Handle Leader
        serialize_packed::write_packed_message_unbuffered(&mut socket, &mut message);

        // Wait for a response.
        let mut response = serialize_packed::new_reader_unbuffered(socket, ReaderOptions::new()).unwrap();
        let client_res = response.get_root::<client_response::Reader>().unwrap();
        // Set the current leader.
        match client_res.which().unwrap() {
            client_response::Which::Success(()) => {
                // It worked!
                Ok(())
            },
            client_response::Which::NotLeader(Ok(leader_bytes)) => {
                let socket = SocketAddr::from_str(leader_bytes).unwrap();
                self.current_leader = Some(socket);
                // Try again.
                self.append(entry)
            }
            _ => unimplemented!(),
        }
    }

    /// This function will force the `Raft` interface to refresh it's knowledge of the leader from
    /// The cooresponding `RaftNode` running alongside it.
    pub fn refresh_leader(&mut self) -> io::Result<()> {
        let mut message = MallocMessageBuilder::new_default();
        {
            let mut client_req = message.init_root::<client_request::Builder>();
            client_req.set_leader_refresh(());
        }
        let mut socket = try!(TcpStream::connect(self.related_raftnode));
        try!(serialize_packed::write_packed_message_unbuffered(&mut socket, &mut message));

        // Wait for a response.
        let mut response = serialize_packed::new_reader_unbuffered(socket, ReaderOptions::new()).unwrap();
        let client_res = response.get_root::<client_response::Reader>().unwrap();
        // Set the current leader.
        self.current_leader = match client_res.which().unwrap() {
            client_response::Which::NotLeader(Ok(leader_bytes)) => {
                let socket = SocketAddr::from_str(leader_bytes).unwrap();
                Some(socket)
            },
            _ => unimplemented!(),
        };
        Ok(())
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
