extern crate raft;
extern crate env_logger;
mod new;

use new::new_cluster;


#[test]
fn hello() {
    env_logger::init().unwrap();
    let mut nodes = new_cluster(3);
    let sent_command = b"Hello";
    // Push
    nodes[0].0.append(sent_command)
        .ok().expect("Couldn't append.");
    // Retrieve.
    let received_command = nodes[0].1.recv()
        .ok().expect("Couldn't recv.");
    assert_eq!(received_command, sent_command);
}
