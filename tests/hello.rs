extern crate raft;
mod new;

use new::new_cluster;

#[test]
fn hello() {
    let mut nodes = new_cluster(3);
    let sent_command = b"Hello";
    // Push
    nodes[0].0.append(sent_command)
        .ok().expect("Couldn't append.");
    // Retrieve.
    let recieved_command = nodes[0].1.recv()
        .ok().expect("Couldn't recv.");
    assert_eq!(recieved_command, sent_command);
}
