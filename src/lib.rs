#![crate_name = "raft"]
#![crate_type="lib"]

#![allow(unstable)]
extern crate "rustc-serialize" as rustc_serialize;

use std::io::net::ip::SocketAddr;
use std::io::net::udp::UdpSocket;
use std::io::timer::Timer;
use std::time::Duration;
use std::thread::Thread;
use std::rand::{thread_rng, Rng};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::str;
use rustc_serialize::{json, Encodable, Decodable};
use std::collections::HashMap;

use RemoteProcedureCall::{AppendEntries, RequestVote};
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
}

/// The implementation of the RaftNode. In most use cases, creating a `RaftNode` should just be
/// done via `::new()`.
///
/// ```
/// use raft::RaftNode;
/// use std::io::net::ip::SocketAddr;
/// use std::io::net::ip::IpAddr::Ipv4Addr;
/// use std::collections::HashMap;
///
/// let mut nodes = HashMap::new();
/// nodes.insert(1, SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11111 });
/// nodes.insert(2, SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11112 });
/// // Create the nodes.
/// let send_node = RaftNode::<String>::new(1, nodes.clone());
/// ```
impl<T: Encodable + Decodable + Send + Clone> RaftNode<T> {
    /// Creates a new RaftNode with the neighbors specified. `id` should be a valid index into
    /// `nodes`. The idea is that you can use the same `nodes` on all of the clients and only vary
    /// `id`.
    pub fn new(id: u64, nodes: HashMap<u64, SocketAddr>) -> RaftNode<T> {
        // TODO: Check index.
        // I'd rather not have to create these, but c'este la vie
        let mut rng = thread_rng();
        let mut timer = Timer::new().unwrap();
        RaftNode {
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
        }
    }
    /// Spins up a thread that listens and handles RPCs.
    pub fn spinup(mut self) -> (Sender<T>, Receiver<T>) {
        let (client_in, node_out) = channel::<T>();
        let (node_in, client_out) = channel::<T>();
        // Need to clone the SocketAddr for lifetime reasons.
        Thread::spawn(move || {
            // Setup the socket, make it not block.
            let own_socket_addr = self.nodes.get(&mut self.own_id)
                .unwrap(); // TODO: Can we do better?
            let mut socket = UdpSocket::bind(*own_socket_addr)
                .unwrap(); // TODO: Can we do better?
            socket.set_read_timeout(Some(0));
            // Start up a RNG and Timer
            let mut rng = thread_rng();
            let mut timer = Timer::new().unwrap();
            // We need a read buffer.
            let mut read_buffer = [0; BUFFER_SIZE];
            // This is the main, strongly typed state machine. It loops indefinitely for now. It
            // would be nice if this was event based.
            loop {
                match &self.state {
                    &Leader(ref leader_state) => {
                        // If socket has data.
                        match socket.recv_from(&mut read_buffer) {
                            Ok((num_read, source)) => { // Something on the socket.
                                // This is possibly an RPC from another node. Try to parse it out
                                // and determine what to do based on it's variant.
                                // Possible Variants: 
                                //   * `RemoteProcedureCall::RequestVote`,
                                //   * `RemoteProcedureCall::AppendEntries`
                                // TODO
                            },
                            Err(_) => (),               // Nothing on the socket.
                        }
                        // If channel has data.
                        match node_out.try_recv() {
                            Ok(entry) => {              // Something in channel.
                                // This is an entry that the client wants commited.
                                // Announce this change to the cluster as soon as possible and
                                // return the same value on the channel back to them.
                                // TODO
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
                            },
                            Err(_) => (),               // Timer hasn't fired.
                        }
                    },
                    &Follower => {
                        // If socket has data.
                        match socket.recv_from(&mut read_buffer) {
                            Ok((num_read, source)) => { // Something on the socket.
                                // Sweet! We got a packet. This is probably a `RemoteProcedureCall`
                                // variant. If it's a `RequestVote` respond either with the current
                                // leader, or a vote. If it's an `AppendEntries` we need to work
                                // with the log and **update the heartbeat timer**.
                                // TODO
                                println!("Got something from socket");
                                let string_slice = str::from_utf8(read_buffer.slice_to_mut(num_read))
                                    .unwrap(); // TODO: Can we do better?
                                let mut rpc = json::decode::<RemoteProcedureCall<T>>(string_slice)
                                    .unwrap(); // TODO: Can we do better?
                                match rpc {
                                    AppendEntries { entries: mut entries, .. } => {
                                        // TODO: Log data properly.
                                        for entry in entries.iter_mut() {
                                            node_in.send(entry.clone());
                                        }
                                    },
                                    RequestVote { .. } => {
                                        // TODO: Do something.
                                    },
                                }
                            },
                            Err(_) => (),               // Nothing on the socket.
                        }
                        // If channel has data.
                        match node_out.try_recv() {
                            Ok(entry) => {              // Something in channel.
                                // The client program is asking for something to be done. For now,
                                // this is only really possibly a new entry for the log. It might
                                // be better to provide other variants like `ShowIndexes(x,y)` or
                                // something then handle those.
                                println!("Sending things!");
                                let rpc = AppendEntries {
                                    term: 0,
                                    leader_id: 0,
                                    prev_log_index: 0,
                                    entries: vec![entry],
                                    leader_commit: 0,
                                };
                                let encoded = json::encode::<RemoteProcedureCall<T>>(&rpc)
                                    .unwrap();
                                for (id, node) in self.nodes.iter() {
                                    socket.send_to(encoded.as_bytes(), *node)
                                        .unwrap(); // TODO: Can we do better?
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

                            },
                            Err(_) => (),               // Timer hasn't fired.
                        }
                    },
                    &Candidate => {
                        // If socket has data.
                        match socket.recv_from(&mut read_buffer) {
                            Ok((num_read, source)) => { // Something on the socket.
                                // As a candidate, we're going to be listening for responses to
                                // our `RemoteProcedureCall::RequestVote` requests. If a node
                                // suggests there is a `Leader` and tells us the ID, become of a
                                // `Follower` of that node.
                                //
                                // If we recieve a `RemoteProcedureCall::AppendEntries` it means
                                // someone thinks we're the `Leader`.
                                // 
                                // If we recieve a `RemoteProcedureCall::RequestVote` it means
                                // there is another `Candidate` campaigning.
                                // TODO
                            },
                            Err(_) => (),               // Nothing on the socket.
                        }
                        // If channel has data.
                        match node_out.try_recv() {
                            Ok(entry) => {              // Something in channel.
                                // The client program wants something to be done. Possible
                                // improvements need to be made here as they have no way to
                                // interface with the log.

                            },
                            Err(_) => (),               // Nothing in channel.
                        }
                        // If timer has fired.
                        match self.heartbeat.try_recv() {
                            Ok(_) => {                  // Timer has fired.
                                // We don't really care about the heartbeat as a `Candidate` as our
                                // heartbeat already timed out, this is how we became a `Candidate`
                                // in the first place.

                            },
                            Err(_) => (),               // Timer hasn't fired.
                        }
                    },
                }
            }
        });
        (client_in, client_out)
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

/// Data interchange format for RPC calls.
#[derive(RustcEncodable, RustcDecodable, Show, Clone)]
pub enum RemoteProcedureCall<T> {
    AppendEntries {
        term: u64,
        leader_id: u64,
        prev_log_index: u64,
        entries: Vec<T>,
        leader_commit: u64,
    },
    RequestVote {
        term: u64,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64,
    },
}

/// Nodes can either be:
///
///   * A `Follower`, which replicates AppendEntries requests and votes for it's leader.
///   * A `Leader`, which leads the cluster by serving incoming requests, ensuring data is
///     replicated, and issuing heartbeats..
///   * A `Candidate`, which campaigns in an election and may become a `Leader` (if it gets enough
///     votes) or a `Follower`, if it hears from a `Leader`.
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
pub struct VolatileState {
    commit_index: u64,
    last_applied: u64
}

/// Leader Only
/// **Reinitialized after election.**
pub struct LeaderState {
    next_index: Vec<u64>,
    match_index: Vec<u64>
}

#[cfg(test)]
mod tests {
    use std::io::net::ip::SocketAddr;
    use std::io::net::udp::UdpSocket;
    use std::io::net::ip::IpAddr::Ipv4Addr;
    use std::thread::Thread;
    use std::collections::HashMap;
    use rustc_serialize::json;
    use std::sync::mpsc::{channel, Sender, Receiver};
    use std::str;
    use super::*;
    
    #[test]
    fn basic_test() {
        let mut nodes = HashMap::new();
        nodes.insert(1, SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11111 });
        nodes.insert(2, SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11112 });
        // Create the nodes.
        let send_node = RaftNode::<String>::new(1, nodes.clone());
        let recieve_node = RaftNode::<String>::new(2, nodes.clone());
        // Start up the node.
        let (_, log_reciever) = recieve_node.spinup();
        let (log_sender, _) = send_node.spinup();
        // Make a test send to that port.
        let test_value = "foo".to_string();
        log_sender.send(test_value.clone()).unwrap();
        // Get the result.
        let event = log_reciever.recv().unwrap();
        assert_eq!(event, "foo".to_string());
    }

}
