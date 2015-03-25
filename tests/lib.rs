#![feature(convert)]

extern crate rustc_serialize;
extern crate raft;
extern crate env_logger;
extern crate log;

use std::fs;
use std::path::PathBuf;
use std::str::FromStr;

use raft::{LogIndex, RaftNode, Term};
use raft::interchange::{ClientRequest, AppendRequest, IndexRange};

#[test]
#[allow(unused_variables)]
fn basic_test() {
    use std::net::SocketAddr;
    // Start the logger.
    env_logger::init().unwrap();

    fs::remove_file(&PathBuf::from("/tmp/test0")).ok();
    fs::remove_file(&PathBuf::from("/tmp/test1")).ok();
    fs::remove_file(&PathBuf::from("/tmp/test2")).ok();

    let nodes = vec![
        SocketAddr::from_str("127.0.0.1:11110").unwrap(),
        SocketAddr::from_str("127.0.0.1:11111").unwrap(),
        SocketAddr::from_str("127.0.0.1:11112").unwrap(),
    ];

    // Create the nodes.
    let (log_0_sender, log_0_reciever) = RaftNode::<String>::start(
        nodes[0].clone(),
        nodes.clone().into_iter().collect(),
        PathBuf::from("/tmp/test0")
    );
    let (log_1_sender, log_1_reciever) = RaftNode::<String>::start(
        nodes[1].clone(),
        nodes.clone().into_iter().collect(),
        PathBuf::from("/tmp/test1")
    );
    let (log_2_sender, log_2_reciever) = RaftNode::<String>::start(
        nodes[2].clone(),
        nodes.clone().into_iter().collect(),
        PathBuf::from("/tmp/test2")
    );

    // Make a test send to that port.
    let test_command = ClientRequest::AppendRequest(AppendRequest {
        entries: vec!["foo".to_string()],
        prev_log_index: LogIndex(0),
        prev_log_term: Term(0),
    });
    log_0_sender.send(test_command.clone()).unwrap();
    // Get the result.
    let event = log_0_reciever.recv()
        .ok().expect("Didn't recieve in a reasonable time.");
    assert!(event.is_ok()); // Workaround until we build a proper stream.


    // Test Index.
    let test_index = ClientRequest::IndexRange(IndexRange {
            start_index: LogIndex(0),
            end_index: LogIndex(5),
    });
    log_0_sender.send(test_index.clone()).unwrap();
    // wait_a_second();
    let result = log_0_reciever.recv()
        .ok().expect("Didn't recieve in a reasonable time.").unwrap();
    // We don't know what the term will be.
    assert_eq!(result, vec![(result.get(0).expect("Unable to get term.").0, "foo".to_string())]);


    // Add something else.
    let test_command = ClientRequest::AppendRequest(AppendRequest {
        entries: vec!["bar".to_string(), "baz".to_string()],
        prev_log_index: LogIndex(1),
        prev_log_term: result.get(0).expect("Unable to get term.").0,
    });
    log_0_sender.send(test_command.clone()).unwrap();
    // Get the result.
    let event = log_0_reciever.recv()
        .ok().expect("Didn't recieve in a reasonable time.");
    assert!(event.is_ok()); // Workaround until we build a proper stream.

    // Test Index.
    let test_index = ClientRequest::IndexRange(IndexRange {
            start_index: LogIndex(0),
            end_index: LogIndex(5),
    });
    log_0_sender.send(test_index.clone()).unwrap();
    // wait_a_second();
    let result = log_0_reciever.recv()
        .ok().expect("Didn't recieve in a reasonable time.").unwrap();
    // We don't know what the term will be.
    assert_eq!(result, vec![
        (result.get(0).expect("Unable to get term.").0, "foo".to_string()),
        (result.get(1).expect("Unable to get term.").0, "bar".to_string()),
        (result.get(1).expect("Unable to get term.").0, "baz".to_string()),
    ]);
}
