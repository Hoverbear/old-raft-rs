#![feature(old_path)]
#![feature(old_io)]
#![feature(fs)]
#![feature(std_misc)]

extern crate "rustc-serialize" as rustc_serialize;
extern crate raft;
extern crate env_logger;
extern crate log;

use raft::interchange::{ClientRequest, AppendRequest, IndexRange};
use raft::RaftNode;

use std::old_io::timer::Timer;
use std::time::Duration;
use std::old_io::net::ip::SocketAddr;
use std::old_io::net::ip::IpAddr::Ipv4Addr;
use std::fs;

fn wait_a_second() {
    let mut timer = Timer::new().unwrap();
    let clock = timer.oneshot(Duration::milliseconds(1000)); // If this fails we're in trouble.
    let _ = clock.recv();
}

#[test]
#[allow(unused_variables)]
fn basic_test() {
    // Start the logger.
    env_logger::init().unwrap();

    fs::remove_file(&Path::new("/tmp/test0")).ok();
    fs::remove_file(&Path::new("/tmp/test1")).ok();
    fs::remove_file(&Path::new("/tmp/test2")).ok();
    let nodes = vec![
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


    // Make a test send to that port.
    let test_command = ClientRequest::AppendRequest(AppendRequest {
        entries: vec!["foo".to_string()],
        prev_log_index: 0,
        prev_log_term: 0,
    });
    log_0_sender.send(test_command.clone()).unwrap();
    // Get the result.
    wait_a_second();
    let event = log_0_reciever.try_recv()
        .ok().expect("Didn't recieve in a reasonable time.");
    assert!(event.is_ok()); // Workaround until we build a proper stream.


    // Test Index.
    let test_index = ClientRequest::IndexRange(IndexRange {
            start_index: 0,
            end_index: 5,
    });
    log_0_sender.send(test_index.clone()).unwrap();
    wait_a_second();
    let result = log_0_reciever.try_recv()
        .ok().expect("Didn't recieve in a reasonable time.").unwrap();
    // We don't know what the term will be.
    assert_eq!(result, vec![(result[0].0, "foo".to_string())]);


    // Add something else.
    let test_command = ClientRequest::AppendRequest(AppendRequest {
        entries: vec!["bar".to_string(), "baz".to_string()],
        prev_log_index: 1,
        prev_log_term: result[0].0,
    });
    log_0_sender.send(test_command.clone()).unwrap();
    // Get the result.
    wait_a_second();
    let event = log_0_reciever.try_recv()
        .ok().expect("Didn't recieve in a reasonable time.");
    assert!(event.is_ok()); // Workaround until we build a proper stream.

    // Test Index.
    let test_index = ClientRequest::IndexRange(IndexRange {
            start_index: 0,
            end_index: 5,
    });
    log_0_sender.send(test_index.clone()).unwrap();
    wait_a_second();
    let result = log_0_reciever.try_recv()
        .ok().expect("Didn't recieve in a reasonable time.").unwrap();
    // We don't know what the term will be.
    assert_eq!(result, vec![
        (result[0].0, "foo".to_string()),
        (result[1].0, "bar".to_string()),
        (result[2].0, "baz".to_string()),
    ]);
}
