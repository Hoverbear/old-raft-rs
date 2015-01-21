#![crate_name = "raft"]
#![crate_type="lib"]

use std::io::net::ip::SocketAddr;
use std::io::net::udp::UdpSocket;
use std::thread::Thread;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::str;

// The maximum size of the read buffer.
const BUFFER_SIZE: usize = 2048;

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
            own_socket: own_socket
        }
    }
    /// Spins up a thread that listens and handles RPCs.
    pub fn spinup(&self) -> Receiver<String> {
        let (sender, reciever) = channel::<String>();
        // Need to clone the SocketAddr for lifetime reasons.
        let mut socket = UdpSocket::bind(self.own_socket)
                .unwrap(); // TODO: Can we do better?
        Thread::spawn(move || {
            let mut read_buffer = [0; BUFFER_SIZE];
            let (num_read, source) = socket.recv_from(&mut read_buffer)
                .unwrap(); // TODO: Can we do better?
            let message = str::from_utf8(&read_buffer)
                .map(|val| String::from_str(val))
                .unwrap_or("FAIL".to_string()); // TODO: Can we do better?
            sender.send(message).unwrap();
        });
        reciever
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
    use std::sync::mpsc::{channel, Sender, Receiver};
    use std::str;
    use super::*;

    #[test]
    fn basic_functionality() {
        // Create the node.
        let my_node = RaftNode::new(
            // This node's address.
            SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11111 },
            // The list of nodes.
            vec![
            SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11112 },
            SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11113 }
            ]);
        // Start up the node.
        let my_channel = my_node.spinup();
        // Make a test send to that port.
        let sender = SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 1112 };
        let mut out = UdpSocket::bind(sender).unwrap();
        out.send_to("foo".as_bytes(), SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11111}).unwrap();
        // Get the result.
        let event = my_channel.recv().unwrap();
        println!("{}", event);
    }

}
