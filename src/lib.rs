#![crate_name = "raft"]
#![crate_type="lib"]
#![doc(html_logo_url = "https://raw.githubusercontent.com/Hoverbear/raft/master/raft.png")]
#![doc(html_root_url = "https://hoverbear.github.io/raft/raft/")]

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
//! 2. Create a `Raft` with those impls which will spawn it's own `Server` and join with a cluster.
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
#![feature(buf_stream)]

extern crate capnp;
extern crate mio;
extern crate rand;
extern crate rustc_serialize;
extern crate uuid;
#[macro_use] extern crate log;

pub mod state_machine;
pub mod store;

pub mod server;
mod replica;
mod state;

mod messages_capnp {
    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/messages_capnp.rs"));
}

use std::collections::HashSet;
use std::io::{self, BufStream, Write};
use std::net::SocketAddr;
use std::net::TcpStream;
use std::ops;
use std::str::FromStr;

use capnp::{serialize, MallocMessageBuilder, MessageBuilder, MessageReader, ReaderOptions};
use rustc_serialize::Encodable;

use messages_capnp::{message, client_append_response};
use server::Server;
use state_machine::StateMachine;
use store::Store;

/// This is the primary interface with a `Server` in the cluster.
///
/// Note: Creating a new `Raft` client will, for now, automatically spawn a `Server` with the
/// relevant parameters. This may be changed in the future. This is based on the assumption that
/// any consuming application interacting with a Raft cluster will also be a participant.
pub struct Raft {
    current_leader: Option<SocketAddr>,
    cluster_members: HashSet<SocketAddr>,
}

impl Raft {
    /// Create a new `Raft` client that has a cooresponding `Server` attached. Note that this
    /// `Raft` may not necessarily interact with the cooreponding `Server`, it will interact with
    /// the `Leader` of a cluster in almost all cases.
    /// *Note:* All requests are blocking, by design from the Raft paper.
    pub fn new<S, M>(addr: SocketAddr,
                     cluster_members: HashSet<SocketAddr>,
                     store: S,
                     state_machine: M)
                     -> Raft
    where S: Store, M: StateMachine {
        debug!("Starting Raft on {}", addr);
        assert!(cluster_members.contains(&addr));
        let mut peers = cluster_members.clone();
        peers.remove(&addr);
        Server::<S, M>::spawn(addr, peers, store, state_machine);
        // Store relevant information.
        Raft {
            current_leader: None,
            cluster_members: cluster_members,
        }
    }

    /// Appends an entry to the replicated log. This will only return once it's properly replicated
    /// to a majority of nodes.
    pub fn append(&mut self, entry: &[u8]) -> Result<()> {
        let mut message = MallocMessageBuilder::new_default();
        {
            message.init_root::<message::Builder>()
                   .init_client_append_request()
                   .set_entry(entry);
        }

        let mut members = self.cluster_members.iter().cloned().cycle();

        loop {
            let leader: SocketAddr = self.current_leader
                                         .take()
                                         .unwrap_or_else(|| members.next().unwrap());
            debug!("connecting to potential leader {}", leader);

            let mut socket = BufStream::new(try!(TcpStream::connect(leader)));
            try!(serialize::write_message(&mut socket, &mut message));
            try!(socket.flush());
            let response = try!(serialize::read_message(&mut socket, ReaderOptions::new()));

            match try!(response.get_root::<message::Reader>()).which().unwrap() {
                message::Which::ClientAppendResponse(Ok(response)) => {
                    match response.which().unwrap() {
                        client_append_response::Which::Success(()) => {
                            self.current_leader = Some(leader);
                            return Ok(())
                        },
                        client_append_response::Which::UnknownLeader(()) => (),
                        client_append_response::Which::NotLeader(leader) => {
                            let leader_addr: SocketAddr = SocketAddr::from_str(try!(leader)).unwrap();
                            assert!(self.cluster_members.contains(&leader_addr));
                            self.current_leader = Some(leader_addr);
                        }
                    }
                },
                _ => panic!("Unexpected message type"), // TODO: return a proper error
            }
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// raft::Errors are the composed variety of errors that can originate from the various libraries.
/// With the exception of the `Raft` variant these are generated from `try!()` macros invoking
/// on `io::Error` or `capnp::Error` by using
/// [`FromError`](https://doc.rust-lang.org/std/error/#the-fromerror-trait).
#[derive(Debug)]
pub enum Error {
    CapnProto(capnp::Error),
    SchemaError(capnp::NotInSchema),
    Io(io::Error),
    Raft(ErrorKind),
}

/// Currently, this can only be:
///
/// * `RelatedNodeDown` - When the related Server is known to be down.
/// * `CannotProceed` - When the related Server cannot proceed due to more than a majority of
///                     nodes being unavailable.
/// TODO: Hook these up.
#[derive(Debug)]
pub enum ErrorKind {
    RelatedNodeDown,
    CannotProceed,
    NotInCluster,
    BadResponse,
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<capnp::Error> for Error {
    fn from(err: capnp::Error) -> Error {
        Error::CapnProto(err)
    }
}

impl From<capnp::NotInSchema> for Error {
    fn from(err: capnp::NotInSchema) -> Error {
        Error::SchemaError(err)
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
