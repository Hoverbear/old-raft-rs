//! The `Client` allows users of the `raft` library to connect to remote `Server` instances and
//! issue commands to be applied to the `StateMachine`.

use std::collections::HashSet;
use std::fmt;
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpStream;
use std::str::FromStr;

use bufstream::BufStream;
use capnp::{serialize, MessageReader, ReaderOptions, MallocMessageBuilder};

use messages_capnp::{client_response, command_response};
use messages;
use ClientId;
use Result;
use RaftError;

/// The representation of a Client connection to the cluster.
pub struct Client {
    /// The `Uuid` of the client, should be unique in the cluster.
    pub id: ClientId,
    /// The current connection to the current leader.
    /// If it is none it may mean that there is no estabished leader or that there has been
    /// a disconnection.
    leader_connection: Option<BufStream<TcpStream>>,
    /// A lookup for the cluster's nodes.
    cluster: HashSet<SocketAddr>,
}

impl Client {

    /// Creates a new client.
    pub fn new(cluster: HashSet<SocketAddr>) -> Client {
        Client {
            id: ClientId::new(),
            leader_connection: None,
            cluster: cluster,
        }
    }

    /// Proposes an entry to be appended to the replicated log. This will only
    /// return once the entry has been durably committed.
    /// Returns `Error` when the entire cluster has an unknown leader. Try proposing again later.
    pub fn propose(&mut self, entry: &[u8]) -> Result<Vec<u8>> {
        scoped_trace!("{:?}: propose", self);
        let mut message = messages::proposal_request(entry);
        self.send_message(&mut message)
    }

    /// Queries an entry from the state machine. This is non-mutating and doesn't go through the
    /// durable log. Like `.propose()` this will only communicate with the leader of the cluster.
    pub fn query(&mut self, query: &[u8]) -> Result<Vec<u8>> {
        scoped_trace!("{:?}: query", self);
        let mut message = messages::query_request(query);
        self.send_message(&mut message)
    }

    fn send_message(&mut self, message: &mut MallocMessageBuilder) -> Result<Vec<u8>> {
        let mut members = self.cluster.iter().cloned();

        loop {
            let mut connection = match self.leader_connection.take() {
                Some(cxn) => {
                    scoped_debug!("had existing connection {:?}", cxn.get_ref().peer_addr());
                    cxn
                },
                None => {
                    let leader = try!(members.next().ok_or(RaftError::LeaderSearchExhausted));
                    scoped_debug!("connecting to potential leader {}", leader);
                    // Send the preamble.
                    let preamble = messages::client_connection_preamble(self.id);
                    let mut stream = BufStream::new(try!(TcpStream::connect(leader)));
                    scoped_debug!("connected");
                    try!(serialize::write_message(&mut stream, &*preamble));
                    stream
                }
            };
            try!(serialize::write_message(&mut connection, message));
            try!(connection.flush());
            scoped_debug!("awaiting response from connection");
            let response = try!(serialize::read_message(&mut connection, ReaderOptions::new()));
            match try!(response.get_root::<client_response::Reader>()).which().unwrap() {
                client_response::Which::Proposal(Ok(response)) => {
                    match response.which().unwrap() {
                        command_response::Which::Success(data) => {
                            scoped_debug!("received response Success");
                            self.leader_connection = Some(connection);
                            return data
                                .map(|v| Vec::from(v))
                                .map_err(|e| e.into()) // Exit the function.
                        },
                        command_response::Which::UnknownLeader(()) => {
                            scoped_debug!("received response UnknownLeader");
                            () // Keep looping.
                        },
                        command_response::Which::NotLeader(leader) => {
                            scoped_debug!("received response NotLeader");
                            let leader_str = try!(leader);
                            if !self.cluster.contains(&try!(SocketAddr::from_str(leader_str))) {
                                scoped_debug!("cluster violation detected");
                                return Err(RaftError::ClusterViolation.into()) // Exit the function.
                            }
                            let mut connection: TcpStream = try!(TcpStream::connect(leader_str));
                            let preamble = messages::client_connection_preamble(self.id);
                            try!(serialize::write_message(&mut connection, &*preamble));
                            self.leader_connection = Some(BufStream::new(connection));
                        }
                    }
                },
                _ => panic!("Unexpected message type"), // TODO: return a proper error
            }
        }
    }
}

impl fmt::Debug for Client {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Client({})", self.id)
    }
}


#[cfg(test)]
mod tests {
    extern crate env_logger;

    use std::collections::HashSet;
    use std::io::Write;
    use std::net::{TcpStream, TcpListener};
    use std::thread;

    use uuid::Uuid;
    use capnp::{serialize, ReaderOptions};
    use capnp::message::MessageReader;
    use bufstream::BufStream;

    use {Client, messages, Result};
    use messages_capnp::{connection_preamble, client_request};

    fn expect_preamble(connection: &mut TcpStream, client_id: Uuid) -> Result<bool> {
        let message = try!(serialize::read_message(connection, ReaderOptions::new()));
        let preamble = try!(message.get_root::<connection_preamble::Reader>());
        // Test to make sure preamble has the right id.
        if let connection_preamble::id::Which::Client(Ok(id)) = try!(preamble.get_id().which()) {
            Ok(Uuid::from_bytes(id).unwrap() == client_id)
        } else {
            Ok(false)
        }
    }

    fn expect_proposal(connection: &mut TcpStream, value: &[u8]) -> Result<bool> {
        let message = try!(serialize::read_message(connection, ReaderOptions::new()));
        let request = try!(message.get_root::<client_request::Reader>());
        // Test to make sure request has the right value.
        if let client_request::Which::Proposal(Ok(proposal)) = try!(request.which()) {
            Ok(proposal.get_entry().unwrap() == value)
        } else {
            Ok(false)
        }
    }

    #[test]
    fn test_proposal_success() {
        setup_test!("test_proposal_success");
        // Start up the cluster and get what we need.
        let mut cluster = HashSet::new();
        let test_server = TcpListener::bind("127.0.0.1:0").unwrap();
        let test_addr = test_server.local_addr().unwrap();
        cluster.insert(test_addr);

        let mut client = Client::new(cluster);
        let client_id = client.id.0.clone();
        let to_propose = b"Bears";

        // The client connects on the proposal.
        // Wait for it.
        let child = thread::spawn(move || {
            let (mut connection, _)  = test_server.accept().unwrap();

            // Proposal should be fine, no errors.
            scoped_debug!("Should get preamble and proposal. Responds Success");
            expect_preamble(&mut connection, client_id).unwrap();
            expect_proposal(&mut connection, to_propose).unwrap();
            // Send response! (success!)
            let response = messages::command_response_success(b"Foxes");
            serialize::write_message(&mut connection, &*response).unwrap();
            connection.flush().unwrap();
        });

        // Propose. It's a marriage made in heaven! :)
        // Should be ok
        assert_eq!(client.propose(to_propose).unwrap(), b"Foxes");
        assert!(client.leader_connection.is_some());

        child.join().unwrap();
    }

    #[test]
    fn test_proposal_unknown_leader() {
        setup_test!("test_proposal_unknown_leader");
        // Start up the cluster and get what we need.
        let mut cluster = HashSet::new();
        let test_server = TcpListener::bind("127.0.0.1:0").unwrap();
        let test_addr = test_server.local_addr().unwrap();
        cluster.insert(test_addr);

        let mut client = Client::new(cluster);
        let to_propose = b"Bears";

        // The client connects on the proposal.
        // Wait for it.
        let child = thread::spawn(move || {
            let (mut connection, _)  = test_server.accept().unwrap();

            // Proposal should report unknown leader, and have the client return error.
            scoped_debug!("Should get proposal. Responds UnknownLeader");
            expect_proposal(&mut connection, to_propose).unwrap();
            // Send response! (unknown leader!) Client should drop connection.
            let response = messages::command_response_unknown_leader();
            serialize::write_message(&mut connection, &*response).unwrap();
            connection.flush().unwrap();
        });

        // Propose. It's a marriage made in heaven! :)
        assert!(client.propose(to_propose).is_err());

        child.join().unwrap();
    }

    #[test]
    fn test_proposal_not_leader() {
        setup_test!("test_proposal_not_leader");
        let mut cluster = HashSet::new();
        let test_server = TcpListener::bind("127.0.0.1:0").unwrap();
        let test_addr = test_server.local_addr().unwrap();
        cluster.insert(test_addr);

        let second_server = TcpListener::bind("127.0.0.1:0").unwrap();
        let second_addr = second_server.local_addr().unwrap();
        cluster.insert(second_addr);

        let mut client = Client::new(cluster);
        let client_id = client.id.0.clone();
        let to_propose = b"Bears";

        // The client connects on the first proposal.
        // Wait for it.
        let child = thread::spawn(move || {
            // Proposal should report NotLeader. Client should choose the server we direct it to.
            scoped_debug!("Should get preamble and proposal. Responds NotLeader.");
            let (mut connection, _)  = test_server.accept().unwrap();
            expect_preamble(&mut connection, client_id).unwrap();
            expect_proposal(&mut connection, to_propose).unwrap();

            // Send response! (not leader!)
            let response = messages::command_response_not_leader(&second_addr);
            serialize::write_message(&mut connection, &*response).unwrap();
            connection.flush().unwrap();

            // Test that it seeks out other server and proposes.
            scoped_debug!("Second server should get preamble and proposal. Responds Success.");
            let (mut connection, _)  = second_server.accept().unwrap();
            expect_preamble(&mut connection, client_id).unwrap();
            expect_proposal(&mut connection, to_propose).unwrap();

            // Send final response! (Success!)
            let response = messages::command_response_success(b"Foxes");
            serialize::write_message(&mut connection, &*response).unwrap();
        });

        // Workaround to set up rigged selection of servers.
        client.leader_connection = {
            let preamble = messages::client_connection_preamble(client.id);
            let mut stream = BufStream::new(TcpStream::connect(test_addr).unwrap());
            serialize::write_message(&mut stream, &*preamble).unwrap();
            Some(stream)
        };

        // Should be ok, change leader connection.
        assert_eq!(client.propose(to_propose).unwrap(), b"Foxes");
        assert!(client.leader_connection.is_some());

        child.join().unwrap();
    }

    /// This test makes sure that the client cannot be redirected to a leader which exists outside
    /// the cluster. This is a necessary test since it would introduce error into the cluster.
    #[test]
    fn test_proposal_leader_not_in_cluster() {
        setup_test!("test_proposal_leader_not_in_cluster");
        let mut cluster = HashSet::new();
        let test_server = TcpListener::bind("127.0.0.1:0").unwrap();
        let test_addr = test_server.local_addr().unwrap();
        cluster.insert(test_addr);

        let second_server = TcpListener::bind("127.0.0.1:0").unwrap();
        let second_addr = second_server.local_addr().unwrap();
        // cluster.insert(second_addr); <--- NOT in cluster.

        let mut client = Client::new(cluster);
        let client_id = client.id.0.clone();
        let to_propose = b"Bears";

        // The client connects on the first proposal.
        // Wait for it.
        let child = thread::spawn(move || {
            // Proposal should report NotLeader. Client should choose the server we direct it to.
            scoped_debug!("Should get preamble and proposal. Responds NotLeader.");
            let (mut connection, _)  = test_server.accept().unwrap();
            expect_preamble(&mut connection, client_id).unwrap();
            expect_proposal(&mut connection, to_propose).unwrap();

            // Send response! (not leader!)
            let response = messages::command_response_not_leader(&second_addr);
            serialize::write_message(&mut connection, &*response).unwrap();
            connection.flush().unwrap();

            // No more...
        });

        // Workaround to set up rigged selection of servers.
        client.leader_connection = {
            let preamble = messages::client_connection_preamble(client.id);
            let mut stream = BufStream::new(TcpStream::connect(test_addr).unwrap());
            serialize::write_message(&mut stream, &*preamble).unwrap();
            Some(stream)
        };

        // Should be err, change leader connectio but to wrong ip..
        assert!(client.propose(to_propose).is_err());
        assert!(client.leader_connection.is_none());

        child.join().unwrap();
    }
}
