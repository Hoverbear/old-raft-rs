#![crate_name = "raft"]
#![crate_type="lib"]

#![allow(unstable)]
extern crate "rustc-serialize" as rustc_serialize;
pub mod interchange;

use std::old_io::net::ip::SocketAddr;
use std::old_io::net::udp::UdpSocket;
use std::old_io::timer::Timer;
use std::time::Duration;
use std::thread::Thread;
use std::rand::{thread_rng, Rng};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::str;
use std::collections::HashMap;
use rustc_serialize::{json, Encodable, Decodable};


use interchange::{RemoteProcedureCall, RemoteProcedureResponse, ClientRequest, ClientResponse};
use NodeState::{Leader, Follower, Candidate};

// The maximum size of the read buffer.
const BUFFER_SIZE: usize = 4096;
const HEARTBEAT_MIN: i64 = 150;
const HEARTBEAT_MAX: i64 = 300;

/// The Raft Distributed Consensus Algorithm requires two RPC calls to be available:
///
///   * `append_entries` which is used as both a heartbeat (with no payload) and the primary
///     interface for requests.
///   * `request_vote` which is used by candidates during campaigns to obtain a vote.
///
pub trait Raft<T: Encodable + Decodable + Send + Clone> {
    /// Returns (term, success)
    fn append_entries(term: u64, leader_id: u64, prev_log_index: u64,
                      prev_log_term: u64, entries: Vec<T>,
                      leader_commit: u64) -> (u64, bool);

    /// Returns (term, voteGranted)
    fn request_vote(term: u64, candidate_id: u64, last_log_index: u64,
                    last_log_term: u64) -> (u64, bool);
}

/// A `RaftNode` acts as a replicated state machine. The server's role in the cluster depends on it's
/// own status. It will maintain both volatile state (which can be safely lost) and persistent
/// state (which must be carefully stored and kept safe).
pub struct RaftNode<T: Encodable + Decodable + Send + Clone> {
    // Raft related.
    state: NodeState,
    persistent_state: PersistentState<T>,
    volatile_state: VolatileState,
    // Auxilary Data.
    // TODO: This should probably be split off.
    // All nodes need to know this otherwise they can't effectively lead or hold elections.
    leader_node: Option<SocketAddr>,
    own_id: u64,
    nodes: HashMap<u64, SocketAddr>,
    heartbeat: Receiver<()>,
    socket: UdpSocket,
    req_recv: Receiver<ClientRequest<T>>,
    res_send: Sender<ClientResponse<T>>
}

/// The implementation of the RaftNode. In most use cases, creating a `RaftNode` should just be
/// done via `::new()`.
///
/// ```
/// use raft::RaftNode;
/// use std::old_io::net::ip::SocketAddr;
/// use std::old_io::net::ip::IpAddr::Ipv4Addr;
/// use std::collections::HashMap;
///
/// let mut nodes = HashMap::new();
/// nodes.insert(1, SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11111 });
/// nodes.insert(2, SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11112 });
/// // Create the nodes.
/// let node = RaftNode::<String>::start(1, nodes.clone());
/// ```
impl<T: Encodable + Decodable + Send + Clone> RaftNode<T> {
    /// Creates a new RaftNode with the neighbors specified. `id` should be a valid index into
    /// `nodes`. The idea is that you can use the same `nodes` on all of the clients and only vary
    /// `id`.
    pub fn start(id: u64, nodes: HashMap<u64, SocketAddr>) -> (Sender<ClientRequest<T>>, Receiver<ClientResponse<T>>) {
        // TODO: Check index.
        // I'd rather not have to create these, but c'este la vie
        let mut rng = thread_rng();
        let mut timer = Timer::new().unwrap();
        // Setup the socket, make it not block.
        let own_socket_addr = nodes.get(&id)
            .unwrap().clone(); // TODO: Can we do better?
        let mut socket = UdpSocket::bind(own_socket_addr)
            .unwrap(); // TODO: Can we do better?
        socket.set_read_timeout(Some(0));
        // Communication channels.
        let (req_send, req_recv) = channel::<ClientRequest<T>>();
        let (res_send, res_recv) = channel::<ClientResponse<T>>();
        // Create the struct.
        let mut raft_node = RaftNode {
            state: Follower,
            persistent_state: PersistentState {
                current_term: 0, // TODO: Double Check.
                voted_for: 0,    // TODO: Better type?
                log: Vec::<T>::new(),
            },
            volatile_state: VolatileState {
                commit_index: 0,
                last_applied: 0,
            },
            leader_node: None,
            own_id: id,
            nodes: nodes,
            // Blank timer for now.
            heartbeat: timer.oneshot(Duration::milliseconds(rng.gen_range::<i64>(HEARTBEAT_MIN, HEARTBEAT_MAX))), // If this fails we're in trouble.
            socket: socket,
            req_recv: req_recv,
            res_send: res_send,
        };
        Thread::spawn(move || {
            // Start up a RNG and Timer
            let mut rng = thread_rng();
            let mut timer = Timer::new().unwrap();
            // We need a read buffer.
            let mut read_buffer = [0; BUFFER_SIZE];
            // This is the main, strongly typed state machine. It loops indefinitely for now. It
            // would be nice if this was event based.
            loop {
                match raft_node.state {
                    Leader(_) => {
                        raft_node.leader_tick();
                    },
                    Follower => {
                        raft_node.follower_tick();
                    },
                    Candidate => {
                        raft_node.candidate_tick();
                    },
                }
            }
        });
        (req_send, res_recv)
    }
    /// This is the main tick for a leader node.
    fn leader_tick(&mut self) {
        // We need a read buffer.
        let mut read_buffer = [0; BUFFER_SIZE];
        // If socket has data.
        match self.socket.recv_from(&mut read_buffer) {
            Ok((num_read, source)) => { // Something on the socket.
                // This is possibly an RPC from another node. Try to parse it out
                // and determine what to do based on it's variant.
                let data = str::from_utf8(&mut read_buffer[.. num_read])
                    .unwrap();
                if let Ok(rpc) = json::decode::<RemoteProcedureCall<T>>(data) {
                    match rpc {
                        RemoteProcedureCall::RequestVote {
                            .. // We don't care what they're sending. We're leading.
                        } => unimplemented!(),
                        RemoteProcedureCall::AppendEntries {
                            ..
                        } => unimplemented!(),
                    }
                } else if let Ok(rpr) = json::decode::<RemoteProcedureResponse>(data) {
                    match rpr {
                        RemoteProcedureResponse::Accepted {
                            ..
                        } => unimplemented!(),
                        RemoteProcedureResponse::Rejected {
                            ..
                        } => unimplemented!(),
                    }
                }
            },
            Err(_) => (),               // Nothing on the socket.
        }
        // If channel has data.
        match self.req_recv.try_recv() {
            Ok(entry) => {              // Something in channel.
                // This is an entry that the client wants commited.
                // Announce this change to the cluster as soon as possible and
                // return the same value on the channel back to them.
                // TODO
                unimplemented!()
            },
            Err(_) => (),               // Nothing in channel.
        }
        // If timer has fired.
        match self.heartbeat.try_recv() {
            Ok(_) => {                  // Timer has fired.
                // It's time to announce a heartbeat. Fire off UDP packets to the
                // cluster to maintain authority and update logs. Use an
                // `RemoteProcedureCall::AppendEntries` variant.
                // TODO
                unimplemented!()
            },
            Err(_) => (),               // Timer hasn't fired.
        }
    }
    /// This is the main tick for a follower node.
    fn follower_tick(&mut self) {
        // We need a read buffer.
        let mut read_buffer = [0; BUFFER_SIZE];
        // If socket has data.
        match self.socket.recv_from(&mut read_buffer) {
            Ok((num_read, source)) => { // Something on the socket.
                // Sweet! We got a packet. This is probably a `RemoteProcedureCall`
                // variant. If it's a `RequestVote` respond either with the current
                // leader, or a vote. If it's an `AppendEntries` we need to work
                // with the log and **update the heartbeat timer**.
                // TODO
                let data = str::from_utf8(&mut read_buffer[.. num_read])
                    .unwrap();
                if let Ok(rpc) = json::decode::<RemoteProcedureCall<T>>(data) {
                    match rpc {
                        RemoteProcedureCall::RequestVote {
                            ..
                        } => unimplemented!(),
                        RemoteProcedureCall::AppendEntries {
                            entries: mut entries,
                            prev_log_index: mut idx,
                            ..
                        } => {
                            // TODO: Log data properly.
                            self.res_send.send(ClientResponse::Accepted {
                                entries: entries.clone(),
                                last_index: idx,
                            });
                        },
                    }
                } else if let Ok(rpr) = json::decode::<RemoteProcedureResponse>(data) {
                    match rpr {
                        RemoteProcedureResponse::Accepted {
                            ..
                        } => unimplemented!(),
                        RemoteProcedureResponse::Rejected {
                            ..
                        } => unimplemented!(),
                    }
                }
            },
            Err(_) => (),               // Nothing on the socket.
        }
        // If channel has data.
        match self.req_recv.try_recv() {
            Ok(entry) => {              // Something in channel.
                // The client program is asking for something to be done. This will
                // be a `ClientRequest` which needs to be appropriately acted upon.
                match entry {
                    ClientRequest::IndexRange { .. } => {
                    },
                    ClientRequest::AppendEntries { entries: mut entries, .. } => {
                        println!("Sending things!");
                        let rpc = RemoteProcedureCall::AppendEntries {
                            term: 0,
                            leader_id: 0,
                            prev_log_index: 0,
                            prev_log_term: entries.get(0).unwrap().clone(),
                            entries: entries,
                            leader_commit: 0,
                        };
                        let encoded = json::encode::<RemoteProcedureCall<T>>(&rpc)
                            .unwrap();
                        for (id, node) in self.nodes.iter() {
                            self.socket.send_to(encoded.as_bytes(), *node)
                                .unwrap(); // TODO: Can we do better?
                        }

                    },
                }
            },
            Err(_) => (),               // Nothing in channel.
        }
        // If timer has fired.
        match self.heartbeat.try_recv() {
            Ok(_) => {                  // Timer has fired.
                // This means we haven't heard from the Leader! It's probably time
                // to start an campaign and become a `Candidate`. We'll either
                // hear back that there is a`Leader`, or get enough votes to become one.
                unimplemented!()

            },
            Err(_) => (),               // Timer hasn't fired.
        }
    }
    // This is the main tick for a cadidate node.
    fn candidate_tick(&mut self) {
        // We need a read buffer.
        let mut read_buffer = [0; BUFFER_SIZE];
        // If socket has data.
        match self.socket.recv_from(&mut read_buffer) {
            Ok((num_read, source)) => { // Something on the socket.
                // As a candidate, we're going to be listening for responses to
                // our `RemoteProcedureCall::RequestVote` requests. If a node
                // suggests there is a `Leader` and tells us the ID, become of a
                // `Follower` of that node.
                //
                // If we recieve a `RemoteProcedureCall::AppendEntries` it means
                // someone thinks we're the `Leader`. (Because in this
                // implementation we're allowing Followers to relay AppendEntries
                // from their client programs.)
                //
                // If we recieve a `RemoteProcedureCall::RequestVote` it means
                // there is another `Candidate` campaigning.
                // TODO
                let data = str::from_utf8(&mut read_buffer[.. num_read])
                    .unwrap();
                if let Ok(rpc) = json::decode::<RemoteProcedureCall<T>>(data) {
                    match rpc {
                        RemoteProcedureCall::RequestVote {
                            ..
                        } => unimplemented!(),
                        RemoteProcedureCall::AppendEntries {
                            ..
                        } => unimplemented!(),
                    }
                } else if let Ok(rpr) = json::decode::<RemoteProcedureResponse>(data) {
                    // TODO: Is it possible we'll recieve something we didn't
                    // expect? Might an `Accepted` come when we ask the leader to
                    // append something?
                    match rpr {
                        RemoteProcedureResponse::Accepted {
                            ..
                        } => unimplemented!(),
                        RemoteProcedureResponse::Rejected {
                            ..
                        } => unimplemented!(),
                    }
                }
            },
            Err(_) => (),               // Nothing on the socket.
        }
        // If channel has data.
        match self.req_recv.try_recv() {
            Ok(entry) => {              // Something in channel.
                // The client program wants something to be done. Since we are a
                // candidate right now there isn't much we can do as the cluster
                // does not have a leader as far as we can tell.
                unimplemented!()
            },
            Err(_) => (),               // Nothing in channel.
        }
        // If timer has fired.
        match self.heartbeat.try_recv() {
            Ok(_) => {                  // Timer has fired.
                // We don't really care about the heartbeat as a `Candidate` as our
                // heartbeat already timed out, this is how we became a `Candidate`
                // in the first place.
                panic!("Should not get a timer fire while a Candidate.")
            },
            Err(_) => (),               // Timer hasn't fired.
        }
    }

    /// A lookup for index -> SocketAddr
    fn lookup(&self, index: u64) -> Option<&SocketAddr> {
        self.nodes.get(&index)
    }
    /// When a `Follower`'s heartbeat times out it's time to start a campaign for election and
    /// become a `Candidate`. If successful, the `RaftNode` will transistion state into a `Leader`,
    /// otherwise it will become `Follower` again.
    /// This function accepts a `Follower` and transforms it into a `Candidate` then attempts to
    /// issue `RequestVote` remote procedure calls to other known nodes. If a majority come back
    /// accepted, it will become the leader.
    fn campaign(&mut self) {
        self.state = match self.state {
            Follower => Candidate,
            _ => panic!("Should not campaign while not a follower!")
        };
        // TODO: Issue `RequestVote` to known nodes.
        unimplemented!()
            // We rely on the loop to handle incoming responses regarding `RequestVote`, don't worry
            // about that here.
    }


}

/// The RPC calls required by the Raft protocol.
impl<T: Encodable + Decodable + Send + Clone> Raft<T> for RaftNode<T> {
    /// Returns (term, success)
    fn append_entries(term: u64, leader_id: u64, prev_log_index: u64,
                      prev_log_term: u64, entries: Vec<T>,
                      leader_commit: u64) -> (u64, bool) {
        (0, false) // TODO: Implement
    }
    /// Returns (term, voteGranted)
    fn request_vote(term: u64, candidate_id: u64, last_log_index: u64,
                    last_log_term: u64) -> (u64, bool) {
        (0, false) // TODO: Implement
    }
}

/// Nodes can either be:
///
///   * A `Follower`, which replicates AppendEntries requests and votes for it's leader.
///   * A `Leader`, which leads the cluster by serving incoming requests, ensuring data is
///     replicated, and issuing heartbeats..
///   * A `Candidate`, which campaigns in an election and may become a `Leader` (if it gets enough
///     votes) or a `Follower`, if it hears from a `Leader`.
#[derive(PartialEq, Eq)]
pub enum NodeState {
    Follower,
    Leader(LeaderState),
    Candidate,
}

/// Persistent state
/// **Must be updated to stable storage before RPC response.**
pub struct PersistentState<T: Encodable + Decodable + Send + Clone> {
    current_term: u64,
    voted_for: u64, // Better way? Can we use a IpAddr?
    log: Vec<T>,
}

/// Volatile state
#[derive(Copy)]
pub struct VolatileState {
    commit_index: u64,
    last_applied: u64
}

/// Leader Only
/// **Reinitialized after election.**
#[derive(PartialEq, Eq)]
pub struct LeaderState {
    next_index: Vec<u64>,
    match_index: Vec<u64>
}
