extern crate "rustc-serialize" as rustc_serialize;
extern crate raft;

use raft::interchange::{RemoteProcedureCall, AppendEntries, RequestVote};
use raft::interchange::{ClientRequest, AppendRequest, IndexRange};
use raft::interchange::{RemoteProcedureResponse};
use raft::NodeState::{Leader, Follower, Candidate};
use raft::RaftNode;

use std::old_io::net::ip::SocketAddr;
use std::old_io::net::udp::UdpSocket;
use std::old_io::net::ip::IpAddr::Ipv4Addr;
use std::thread::Thread;
use std::collections::HashMap;
use rustc_serialize::json;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::str;

#[test]
fn basic_test() {
    let mut nodes = HashMap::new();
    nodes.insert(1, SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11111 });
    nodes.insert(2, SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11112 });
    // Create the nodes.
    let (log_sender, _) = RaftNode::<String>::start(1, nodes.clone());
    let (_, log_reciever) = RaftNode::<String>::start(2, nodes.clone());
    // Make a test send to that port.
    let test_command = ClientRequest::AppendRequest(AppendRequest {
        entries: vec!["foo".to_string()],
        prev_log_index: 0,
        prev_log_term: "foo".to_string(),
    });
    log_sender.send(test_command.clone()).unwrap();
    // Get the result.
    let event = log_reciever.recv().unwrap();
    match event {
        Ok(entries) => {
            assert_eq!(entries, vec!["foo".to_string()]);
        },
        Err(err) => panic!(err),
    }
}
