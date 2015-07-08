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

extern crate bufstream;
extern crate capnp;
extern crate mio;
extern crate rand;
extern crate uuid;
#[macro_use] extern crate log;
#[macro_use] extern crate wrapped_enum;

pub mod state_machine;
pub mod store;

mod backoff;
mod client;
mod connection;
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

use std::{io, fmt};

use uuid::Uuid;

pub type Result<T> = std::result::Result<T, Error>;

/// raft::Errors are the composed variety of errors that can originate from the various libraries.
/// With the exception of the `Raft` variant these are generated from `try!()` macros invoking
/// on `io::Error` or `capnp::Error` by using
/// [`FromError`](https://doc.rust-lang.org/std/error/#the-fromerror-trait).
wrapped_enum!{
    #[derive(Debug)]
    ///
    pub enum Error {
        ///
        : CapnProto(capnp::Error),
        ///
        : SchemaError(capnp::NotInSchema),
        ///
        : Io(io::Error),
        ///
        : Raft(ErrorKind),
    }
}

#[derive(Debug)]
///
pub enum ErrorKind {
    /// The server ran out of slots in the slab for new connections
    ConnectionLimitReached,
    /// A client reported an invalid client id
    InvalidClientId,
    /// A remote connection attemtped to use an unknown connection type in the connection preamble
    UnknownConnectionType,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::CapnProto(ref error) => fmt::Display::fmt(error, f),
            Error::SchemaError(ref error) => fmt::Display::fmt(error, f),
            Error::Io(ref error) => fmt::Display::fmt(error, f),
            Error::Raft(ref error) => fmt::Debug::fmt(error, f),
        }
    }
}

/// The Term for the log entry.
pub type Term = u64;

/// The index for the log entry.
pub type LogIndex = u64;

/// The id of a Raft server. Must be unique among the participants in a
/// consensus group.
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub struct ServerId(u64);

impl From<u64> for ServerId {
    fn from(val: u64) -> ServerId {
        ServerId(val)
    }
}

impl From<ServerId> for u64 {
    fn from(server_id: ServerId) -> Self {
        server_id.0
    }
}

impl fmt::Display for ServerId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// The ID of a Raft client.
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub struct ClientId(Uuid);

impl ClientId {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        match Uuid::from_bytes(bytes) {
            Some(uuid) => Ok(ClientId(uuid)),
            None => Err(Error::Raft(ErrorKind::InvalidClientId)),
        }
    }

    fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Display for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}
