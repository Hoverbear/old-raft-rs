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
extern crate uuid;
#[macro_use] extern crate log;

/// Unwraps the Result value, or logs a warning and returns early if the value
/// is an `Err`.
macro_rules! try_warn {
    ($expr:expr, $($arg:tt)*) => (match $expr {
        ::std::result::Result::Ok(val) => val,
        ::std::result::Result::Err(err) => {
            warn!($($arg)*, err);
            return ::std::result::Result::Err(::std::convert::From::from(err));
        },
    })
}

pub mod state_machine;
pub mod store;

mod client;
mod messages;
mod replica;
mod server;
mod state;

pub mod messages_capnp {
    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/messages_capnp.rs"));
}

pub use server::Server;
pub use state_machine::StateMachine;
pub use store::Store;
pub use client::Client;

use std::{io, ops, fmt};

use uuid::Uuid;

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
    InvalidClientId,
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
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
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
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
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

/// The id of a Raft server. Must be unique among the participants in a
/// consensus group.
#[derive(Copy, Clone, Hash, PartialEq, Eq)]
pub struct ServerId(u64);
impl From<u64> for ServerId {
    fn from(val: u64) -> ServerId {
        ServerId(val)
    }
}
impl Into<u64> for ServerId {
    fn into(self) -> u64 {
        self.0
    }
}
impl fmt::Debug for ServerId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ServerId({})", self.0)
    }
}
impl fmt::Display for ServerId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// The ID of a Raft client.
#[derive(Copy, Clone, Hash, PartialEq, Eq)]
pub struct ClientId(Uuid);
impl ClientId {
    fn new() -> ClientId {
        ClientId(Uuid::new_v4())
    }
    fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
    fn from_bytes(bytes: &[u8]) -> Result<ClientId> {
        match Uuid::from_bytes(bytes) {
            Some(uuid) => Ok(ClientId(uuid)),
            None => Err(Error::Raft(ErrorKind::InvalidClientId)),
        }
    }
}
impl fmt::Debug for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ClientId({})", self.0)
    }
}
impl fmt::Display for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}
