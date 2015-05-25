extern crate raft;
extern crate env_logger;
mod new;

use new::new_cluster;

#[test]
fn hello() {
    env_logger::init().unwrap();
    let mut nodes = new_cluster(3);
    let sent_command = b"Hello";
    nodes[0].0.append(sent_command).unwrap();
    let recieved_command = nodes[0].1.recv().unwrap();
    assert_eq!(recieved_command, sent_command);
}
