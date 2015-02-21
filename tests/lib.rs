extern crate "rustc-serialize" as rustc_serialize;
extern crate raft;

use raft::interchange::{RemoteProcedureCall, AppendEntries, RequestVote};
use raft::interchange::{ClientRequest, AppendRequest, IndexRange};
use raft::interchange::{RemoteProcedureResponse};
use raft::RaftNode;

use std::old_io::timer::Timer;
use std::time::Duration;
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
    let mut nodes = vec![
        (0, SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11110 }),
        (1, SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11111 }),
        (2, SocketAddr { ip: Ipv4Addr(127, 0, 0, 1), port: 11112 }),
    ];
    // Create the nodes.
    let (log_0_sender, log_0_reciever) = RaftNode::<String>::start(
        0,
        nodes.clone(),
        Path::new("/tmp/test0")
    );
    let (log_1_sender, log_1_reciever) = RaftNode::<String>::start(
        1,
        nodes.clone(),
        Path::new("/tmp/test1")
    );
    let (log_2_sender, log_2_reciever) = RaftNode::<String>::start(
        2,
        nodes.clone(),
        Path::new("/tmp/test2")
    );
    println!("Foo");
    // Make a test send to that port.
    let test_command = ClientRequest::AppendRequest(AppendRequest {
        entries: vec!["foo".to_string()],
        prev_log_index: 0,
        prev_log_term: 0,
    });
    log_0_sender.send(test_command.clone()).unwrap();
    // Get the result.
    let event = log_0_reciever.recv().unwrap();
    let mut timer = Timer::new().unwrap();
    let clock = timer.oneshot(Duration::milliseconds(5000)); // If this fails we're in trouble.
    let _ = clock.recv();


    assert!(event.is_ok());
}
