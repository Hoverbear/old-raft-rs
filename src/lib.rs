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
use RemoteProcedureCall::{AppendEntries, RequestVote};

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
    fn append_entries(term: usize, leader_id: usize, prev_log_index: usize,
                      prev_log_term: usize, entries: Vec<T>,
                      leader_commit: usize) -> (usize, bool);

    /// Returns (term, voteGranted)
    fn request_vote(term: usize, candidate_id: usize, last_log_index: usize,
                    last_log_term: usize) -> (usize, bool);
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
    known_nodes: Vec<SocketAddr>,
    leader_node: Option<SocketAddr>,
    own_socket: SocketAddr,
    heartbeat: Timer,
}

/// The implementation of the RaftNode. In most use cases, creating a `RaftNode` should just be
/// done via `::new()`.
///
/// ```
/// use raft::RaftNode;
/// use std::io::net::ip::SocketAddr;
/// use std::io::net::ip::IpAddr::Ipv4Addr;
///
/// let my_node = RaftNode::<String>::new(
///     // This node's address.
///     SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11111 },
///     // The list of nodes.
///     vec![
///     SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11112 },
///     SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11113 }
///     ]);
/// ```
impl<T: Encodable + Decodable + Send + Clone> RaftNode<T> {
    /// Creates a new RaftNode with the neighbors specified.
    pub fn new(own_socket: SocketAddr, node_list: Vec<SocketAddr>) -> RaftNode<T> {
        RaftNode {
            state: NodeState::Follower,
            persistent_state: PersistentState {
                current_term: 0, // TODO: Double Check.
                voted_for: 0,    // TODO: Better type?
                log: Vec::<T>::new(),
            },
            volatile_state: VolatileState {
                commit_index: 0,
                last_applied: 0,
            },
            known_nodes: node_list,
            leader_node: None,
            own_socket: own_socket,
            // Blank timer for now.
            heartbeat: Timer::new().unwrap(), // If this fails we're in trouble.
        }
    }
    /// Spins up a thread that listens and handles RPCs.
    pub fn spinup(mut self) -> (Sender<T>, Receiver<T>) {
        let (client_in, node_out) = channel::<T>();
        let (node_in, client_out) = channel::<T>();
        // Need to clone the SocketAddr for lifetime reasons.
        Thread::spawn(move || {
            // Setup the socket, make it not block.
            let mut socket = UdpSocket::bind(self.own_socket)
                .unwrap(); // TODO: Can we do better?
            socket.set_read_timeout(Some(0));
            // Start up a RNG.
            let mut rng = thread_rng();
            self.heartbeat.oneshot(Duration::milliseconds(rng.gen_range::<i64>(HEARTBEAT_MIN, HEARTBEAT_MAX)));
            // We need a read buffer.
            let mut read_buffer = [0; BUFFER_SIZE];
            loop {
                // Read anything from the socket and handle it.
                match socket.recv_from(&mut read_buffer) {
                    Ok((num_read, source)) => {
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
                    // TODO: Maybe need to handle this better.
                    Err(_) => (),
                }
                // If there is something in the reciever we need to handle it.
                match (node_out.try_recv(), &self.state) {
                    (Ok(entry), &NodeState::Leader(_)) => {
                        println!("Sending things!");
                        let rpc = AppendEntries {
                            term: 0,
                            leader_id: 0,
                            prev_log_index: 0,
                            entries: vec![entry],
                            leader_commit: 0,
                        };
                        let encoded = json::encode::<RemoteProcedureCall<T>>(&rpc);
                        for node in self.known_nodes.iter() {
                            socket.send_to(encoded.as_bytes(), *node)
                                .unwrap(); // TODO: Can we do better?
                        }
                    },
                    // Need to deal with election issues.
                    (Ok(entry), _) => {
                        println!("Got entry, not leader");
                    },
                    // TODO: Maybe need to handle this better.
                    (Err(_), _) => (),
                }
            }
        });
        (client_in, client_out)
    }
}

/// The RPC calls required by the Raft protocol.
impl<T: Encodable + Decodable + Send + Clone> Raft<T> for RaftNode<T> {
    /// Returns (term, success)
    fn append_entries(term: usize, leader_id: usize, prev_log_index: usize,
                      prev_log_term: usize, entries: Vec<T>,
                      leader_commit: usize) -> (usize, bool) {
        (0, false) // TODO: Implement
    }
    /// Returns (term, voteGranted)
    fn request_vote(term: usize, candidate_id: usize, last_log_index: usize,
                    last_log_term: usize) -> (usize, bool) {
        (0, false) // TODO: Implement
    }
}

/// Data interchange format for RPC calls.
#[derive(RustcEncodable, RustcDecodable, Show, Clone)]
pub enum RemoteProcedureCall<T> {
    AppendEntries {
        term: usize,
        leader_id: usize,
        prev_log_index: usize,
        entries: Vec<T>,
        leader_commit: usize,
    },
    RequestVote {
        term: usize,
        candidate_id: usize,
        last_log_index: usize,
        last_log_term: usize,
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
    current_term: usize,
    voted_for: usize, // Better way? Can we use a IpAddr?
    log: Vec<T>,
}

/// Volatile state
pub struct VolatileState {
    commit_index: usize,
    last_applied: usize
}

/// Leader Only
/// **Reinitialized after election.**
pub struct LeaderState {
    next_index: Vec<usize>,
    match_index: Vec<usize>
}

#[cfg(test)]
mod tests {
    use std::io::net::ip::SocketAddr;
    use std::io::net::udp::UdpSocket;
    use std::io::net::ip::IpAddr::Ipv4Addr;
    use std::thread::Thread;
    use rustc_serialize::json;
    use std::sync::mpsc::{channel, Sender, Receiver};
    use std::str;
    use super::*;

    #[test]
    fn basic_test() {
        let send_addr = SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11111 };
        let recieve_addr = SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11112 };
        // Create the node.
        let send_node = RaftNode::<String>::new(
            send_addr, // The node's address.
            vec![
            recieve_addr, // The neighbor.
            ]);
        let recieve_node = RaftNode::<String>::new(
            recieve_addr, // The node's address.
            vec![
            send_addr, // The neighbor.
            ]);
        // Start up the node.
        let (_, log_reciever) = recieve_node.spinup();
        let (log_sender, _) = send_node.spinup();
        // Make a test send to that port.
        let test_value = "foo".to_string();
        println!("About to send");
        log_sender.send(test_value.clone()).unwrap();
        println!("After send!");
        // Get the result.
        let event = log_reciever.recv().unwrap();
        assert_eq!(event, "foo".to_string());
    }

}
