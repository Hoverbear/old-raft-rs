#![crate_name = "raft"]
#![crate_type="lib"]
#![doc(html_logo_url = "https://raw.githubusercontent.com/Hoverbear/raft/master/raft.png")]
#![doc(html_root_url = "https://hoverbear.github.io/raft/raft/")]

#![feature(convert)]

//! This is the Raft Distributed Consensus Protocol implemented for Rust.
//! [Raft](http://raftconsensus.github.io/) is described as:
//!
//! > Raft is a consensus algorithm that is designed to be easy to understand. It's equivalent to
//! > Paxos in fault-tolerance and performance. The difference is that it's decomposed into
//! > relatively independent subproblems, and it cleanly addresses all major pieces needed for
//! > practical systems.
//!
//! This implementation utilizes [Cap'n Proto](https://kentonv.github.io/capnproto/) for its RPC,
//! [`mio`](https://github.com/carllerche/mio) for it's async event loop.
//!
//! If this package fails to build for you it is possibly because you do not have the
//! [`capnp`](https://capnproto.org/capnp-tool.html) utility installed. You should be able to find
//! appropriate packages for most popular distributions.
//!
//! # Consuming this library
//!
//! Consuming this library works in a few parts:
//!
//! 1. Implement `Store` and `StateMachine` such that they will hook into your application.
//! 2. Create a `Raft` with those impls which will spawn it's own `RaftNode` and join with a cluster.
//! 3. Interact with the cluster by issuing `append()` calls.
//! 4. React to calls to `apply()` from the implemented `StateMachine`
//!
//! It's important to note that issuing an `append()` call to `Raft` does not (at this time)
//! necessarily gaurantee that the entry has been applied to the `StateMachine`, since it must be
//! successfully replicated across a majority of nodes before it is applied.
//!
//! ## State
//!
//!     // TODO
//!
//! ## StateMachine
//!
//!     // TODO
//!
//! ## Client Requests
//!
//!     // TODO
//!

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
use std::result::Result;
use std::collections::HashSet;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::net::TcpStream;
use std::str::FromStr;
use std::error::FromError;

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

/// This is the primary interface with a `RaftNode` in the cluster.
///
/// Note: Creating a new `Raft` client will, for now, automatically spawn a `RaftNode` with the
/// relevant parameters. This may be changed in the future. This is based on the assumption that
/// any consuming application interacting with a Raft cluster will also be a participant.
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
    pub fn append(&mut self, entry: &[u8]) -> Result<(), RaftError> {
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
        let mut response = try!(serialize_packed::new_reader_unbuffered(socket, ReaderOptions::new()));
        let client_res = try!(response.get_root::<client_response::Reader>());
        // Set the current leader.
        match try!(client_res.which()) {
            client_response::Which::Success(()) => {
                // It worked!
                Ok(())
            },
            client_response::Which::NotLeader(Ok(leader_bytes)) => {
                let current_leader = match SocketAddr::from_str(leader_bytes) {
                    Ok(socket) => Some(socket),
                    Err(_) => return Err(RaftError::Raft(RaftErrorKind::BadResponse))
                };
                // Try again.
                self.append(entry)
            },
            _ => unimplemented!(),
        }
    }

    /// Kills the node. Should only really be used for testing purposes.
    /// Accepts a `SocketAddr` because if you're going to kill a node you should be able to pick
    /// your victim.
    pub fn die(&mut self, target: SocketAddr, reason: String) -> Result<(), RaftError> {
        if !self.cluster_members.contains(&target) {
            return Err(RaftError::Raft(RaftErrorKind::NotInCluster))
        }
        let mut message = MallocMessageBuilder::new_default();
        {
            let mut client_req = message.init_root::<client_request::Builder>();
            client_req.set_die(&reason)
        }
        // We know current leader `is_some()` because `refresh_leader()` didn't fail.
        let mut socket = try!(TcpStream::connect(self.current_leader.unwrap())); // TODO: Handle Leader
        serialize_packed::write_packed_message_unbuffered(&mut socket, &mut message);

        // Wait for a response.
        let mut response = try!(serialize_packed::new_reader_unbuffered(socket, ReaderOptions::new()));
        let client_res = try!(response.get_root::<client_response::Reader>());
        // Set the current leader.
        match try!(client_res.which()) {
            client_response::Which::Success(()) => Ok(()),
            _ => unimplemented!(),
        }
    }

    /// This function will force the `Raft` interface to refresh it's knowledge of the leader from
    /// The cooresponding `RaftNode` running alongside it.
    pub fn refresh_leader(&mut self) -> Result<(), RaftError> {
        let mut message = MallocMessageBuilder::new_default();
        {
            let mut client_req = message.init_root::<client_request::Builder>();
            client_req.set_leader_refresh(());
        }
        let mut socket = try!(TcpStream::connect(self.related_raftnode));
        try!(serialize_packed::write_packed_message_unbuffered(&mut socket, &mut message));

        // Wait for a response.
        let mut response = serialize_packed::new_reader_unbuffered(socket, ReaderOptions::new()).unwrap();
        let client_res = try!(response.get_root::<client_response::Reader>());
        // Set the current leader.
        self.current_leader = match try!(client_res.which()) {
            client_response::Which::NotLeader(Ok(leader_bytes)) => {
                match SocketAddr::from_str(leader_bytes) {
                    Ok(socket) => Some(socket),
                    Err(_) => return Err(RaftError::Raft(RaftErrorKind::BadResponse))
                }
            },
            _ => unimplemented!(),
        };
        Ok(())
    }
}

/// RaftErrors are the composed variety of errors that can originate from the various libraries.
/// With the exception of the `Raft` variant these are generated from `try!()` macros invoking
/// on `io::Error` or `capnp::Error` by using
/// [`FromError`](https://doc.rust-lang.org/std/error/#the-fromerror-trait).
#[derive(Debug)]
pub enum RaftError {
    CapnProto(capnp::Error),
    SchemaError(capnp::NotInSchema),
    Io(io::Error),
    Raft(RaftErrorKind),
}

/// Currently, this can only be:
///
/// * `RelatedNodeDown` - When the related RaftNode is known to be down.
/// * `CannotProceed` - When the related RaftNode cannot proceed due to more than a majority of
///                     nodes being unavailable.
/// TODO: Hook these up.
#[derive(Debug)]
pub enum RaftErrorKind {
    RelatedNodeDown,
    CannotProceed,
    NotInCluster,
    BadResponse,
}

impl FromError<io::Error> for RaftError {
    fn from_error(err: io::Error) -> RaftError {
        RaftError::Io(err)
    }
}

impl FromError<capnp::Error> for RaftError {
    fn from_error(err: capnp::Error) -> RaftError {
        RaftError::CapnProto(err)
    }
}

impl FromError<capnp::NotInSchema> for RaftError {
    fn from_error(err: capnp::NotInSchema) -> RaftError {
        RaftError::SchemaError(err)
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
