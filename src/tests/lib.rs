extern crate raft;
use raft::RaftNode;
use raft::interchange::{ClientRequest, ClientResponse};

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
    let send_node = RaftNode::<String>::new(1, nodes.clone());
    let recieve_node = RaftNode::<String>::new(2, nodes.clone());
    // Start up the node.
    let (_, log_reciever) = recieve_node.spinup();
    let (log_sender, _) = send_node.spinup();
    // Make a test send to that port.
    let test_command = ClientRequest::AppendEntries {
        entries: vec!["foo".to_string()],
        prev_log_index: 0,
        prev_log_term: "foo".to_string(),
    };
    log_sender.send(test_command.clone()).unwrap();
    // Get the result.
    let event = log_reciever.recv().unwrap();
    match event {
        ClientResponse::Accepted { entries: entries, .. } => {
            assert_eq!(entries, vec!["foo".to_string()]);
        },
        _ => panic!("Didn't return the right thing"),
    }
}

