#![crate_name = "raft"]
#![crate_type="lib"]

#![allow(unstable)]
extern crate "rustc-serialize" as rustc_serialize;

use std::io::net::ip::SocketAddr;
use std::io::net::udp::UdpSocket;
use std::thread::Thread;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::str;
use rustc_serialize::json;

// The maximum size of the read buffer.
const BUFFER_SIZE: usize = 4096;

/// The Raft Distributed Consensus Algorithm requires two RPC calls to be available:
///
///   * `append_entries` which is used as both a heartbeat (with no payload) and the primary
///     interface for requests.
///   * `request_vote` which is used by candidates during campaigns to obtain a vote.
///
pub trait Raft {
    /// Returns (term, success)
    fn append_entries(term: usize, leader_id: usize, prev_log_index: usize,
                      prev_log_term: usize, entries: Vec<String>,
                      leader_commit: usize) -> (usize, bool);

    /// Returns (term, voteGranted)
    fn request_vote(term: usize, candidate_id: usize, last_log_index: usize,
                    last_log_term: usize) -> (usize, bool);
}

/// A `RaftNode` acts as a replicated state machine. The server's role in the cluster depends on it's
/// own status. It will maintain both volatile state (which can be safely lost) and persistent
/// state (which must be carefully stored and kept safe).
pub struct RaftNode {
    // Raft related.
    state: NodeState,
    persistent_state: PersistentState,
    volatile_state: VolatileState,
    // Auxilary Data.
    // TODO: This should probably be split off.
    // All nodes need to know this otherwise they can't effectively lead or hold elections.
    known_nodes: Vec<SocketAddr>,
    own_socket: SocketAddr,
}

/// The implementation of the RaftNode. In most use cases, creating a `RaftNode` should just be
/// done via `::new()`.
///
/// ```
/// use raft::RaftNode;
/// use std::io::net::ip::SocketAddr;
/// use std::io::net::ip::IpAddr::Ipv4Addr;
///
/// let my_node = RaftNode::new(
///     // This node's address.
///     SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11111 },
///     // The list of nodes.
///     vec![
///     SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11112 },
///     SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11113 }
///     ]);
/// ```
impl RaftNode {
    /// Creates a new RaftNode with the neighbors specified.
    pub fn new(own_socket: SocketAddr, node_list: Vec<SocketAddr>) -> RaftNode {
        RaftNode {
            state: NodeState::Follower,
            persistent_state: PersistentState {
                current_term: 0, // TODO: Double Check.
                voted_for: 0,    // TODO: Better type?
                log: Vec::<String>::new(),
            },
            volatile_state: VolatileState {
                commit_index: 0,
                last_applied: 0,
            },
            known_nodes: node_list,
            own_socket: own_socket,
        }
    }
    /// Spins up a thread that listens and handles RPCs.
    pub fn spinup(&self) -> (Sender<(RemoteProcedureCall, SocketAddr)>, Receiver<String>) {
        let (entry_sender, entry_reciever) = channel::<String>();
        let (rpc_sender, rpc_reciever) = channel::<(RemoteProcedureCall, SocketAddr)>();
        // Need to clone the SocketAddr for lifetime reasons.
        let mut socket = UdpSocket::bind(self.own_socket)
            .unwrap(); // TODO: Can we do better?
        socket.set_read_timeout(Some(0));
        Thread::spawn(move || {
            let mut read_buffer = [0; BUFFER_SIZE];
            loop {
                match socket.recv_from(&mut read_buffer) {
                    Ok((num_read, source)) => {
                        let string_slice = str::from_utf8(read_buffer.slice_to_mut(num_read))
                            .unwrap(); // TODO: Can we do better?
                        let rpc = json::decode::<RemoteProcedureCall>(string_slice)
                            .unwrap(); // TODO: Can we do better?
                        entry_sender.send(format!("{:?}", rpc));
                    },
                    // TODO: Maybe need to handle this better.
                    Err(_) => (),
                }
                // Write anything in the channel out to the network.
                match rpc_reciever.try_recv() {
                    Ok((rpc, target)) => {
                        println!("Sending things!");
                        let encoded = json::encode::<RemoteProcedureCall>(&rpc);
                        socket.send_to(encoded.as_bytes(), target)
                            .unwrap(); // TODO: Can we do better?
                    },
                    // TODO: Maybe need to handle this better.
                    Err(_) => (),
                }
            }
        });
        (rpc_sender, entry_reciever)
    }
}

/// The RPC calls required by the Raft protocol.
impl Raft for RaftNode {
    /// Returns (term, success)
    fn append_entries(term: usize, leader_id: usize, prev_log_index: usize,
                      prev_log_term: usize, entries: Vec<String>,
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
pub enum RemoteProcedureCall {
    AppendEntries {
        term: usize,
        leader_id: usize,
        prev_log_index: usize,
        entries: Vec<String>,
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
pub struct PersistentState {
    current_term: usize,
    voted_for: usize, // Better way? Can we use a IpAddr?
    log: Vec<String>,
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
    fn accepts_request_vote() {
        let send_addr = SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11111 };
        let recieve_addr = SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11112 };
        // Create the node.
        let send_node = RaftNode::new(
            send_addr, // The node's address.
            vec![
            recieve_addr, // The neighbor.
            ]);
        let recieve_node = RaftNode::new(
            recieve_addr, // The node's address.
            vec![
            send_addr, // The neighbor.
            ]);
        // Start up the node.
        let (_, log_reciever) = recieve_node.spinup();
        let (rpc_sender, _) = send_node.spinup();
        // Make a test send to that port.
        let test_value = RemoteProcedureCall::RequestVote {
            term: 0,
            candidate_id: 0,
            last_log_index: 0,
            last_log_term: 0,
        };
        rpc_sender.send((test_value.clone(), recieve_addr)).unwrap();
        // Get the result.
        let event = log_reciever.recv().unwrap();
        assert_eq!(event, format!("{:?}", test_value));
    }

}
