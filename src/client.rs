//! The `Client` allows users of the `raft` library to connect to remote `Server` instances and
//! issue commands to be applied to the `StateMachine`.

use std::collections::HashSet;
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpStream;

use bufstream::BufStream;
use capnp::{serialize, MessageReader, ReaderOptions};

use messages_capnp::{client_response, proposal_response};
use messages;
use ClientId;
use Result;

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
    pub fn propose(&mut self, entry: &[u8]) -> Result<()> {
        let mut message = messages::proposal_request(entry);

        let mut members = self.cluster.iter().cloned().cycle();

        loop {
            let mut connection = match self.leader_connection.take() {
                Some(cxn) => cxn,
                None => {
                    let leader = members.next().unwrap();
                    debug!("connecting to potential leader {}", leader);
                    // Send the preamble.
                    let preamble = messages::client_connection_preamble(self.id);
                    let mut stream = BufStream::new(try!(TcpStream::connect(leader)));
                    try!(serialize::write_message(&mut stream, &*preamble));
                    stream
                }
            };
            try!(serialize::write_message(&mut connection, &mut message));
            try!(connection.flush());
            let response = try!(serialize::read_message(&mut connection, ReaderOptions::new()));
            match try!(response.get_root::<client_response::Reader>()).which().unwrap() {
                client_response::Which::Proposal(Ok(response)) => {
                    match response.which().unwrap() {
                        proposal_response::Which::Success(()) => {
                            self.leader_connection = Some(connection);
                            return Ok(())
                        },
                        proposal_response::Which::UnknownLeader(()) => (),
                        proposal_response::Which::NotLeader(leader) => {
                            let connection: TcpStream = try!(TcpStream::connect(try!(leader)));
                            self.leader_connection = Some(BufStream::new(connection));
                        }
                    }
                },
                _ => panic!("Unexpected message type"), // TODO: return a proper error
            }
        }
    }
}


#[cfg(test)]
mod test {
    use Client;
    use uuid::Uuid;
    use std::net::{SocketAddr, TcpListener};
    use std::collections::HashSet;
    use std::str::FromStr;
    use std::thread;
    use std::io::Read;
    use capnp::{serialize, ReaderOptions};
    use capnp::message::MessageReader;
    use messages;
    use messages_capnp::{connection_preamble, client_request};

    #[test]
    fn test_proposal_standalone() {
        let mut cluster = HashSet::new();
        let test_server = TcpListener::bind(SocketAddr::from_str("127.0.0.1:0").unwrap()).unwrap();
        let test_addr = test_server.local_addr().unwrap();
        cluster.insert(test_addr);

        let mut client = Client::new(cluster);
        let client_id = client.id.0.clone();
        let to_propose = b"Bears";

        // The client connects on the first proposal.
        // Wait for it.
        let child = thread::spawn(move || {
            let (mut connection, _)  = test_server.accept().unwrap();

            // Expect Preamble.
            let message = serialize::read_message(&mut connection, ReaderOptions::new()).unwrap();
            let preamble = message.get_root::<connection_preamble::Reader>().unwrap();
            // Test to make sure preamble has the right id.
            if let connection_preamble::id::Which::Client(Ok(id)) = preamble.get_id().which().unwrap() {
                assert_eq!(Uuid::from_bytes(id).unwrap(), client_id);
            } else { panic!("Invalid preamble."); }

            // Expect proposal.
            let message = serialize::read_message(&mut connection, ReaderOptions::new()).unwrap();
            let request = message.get_root::<client_request::Reader>().unwrap();
            // Test to make sure request has the right value.
            if let client_request::Which::Proposal(Ok(proposal)) = request.which().unwrap() {
                assert_eq!(proposal.get_entry().unwrap(), to_propose);
            } else { panic!("Invalid request."); }

            // Send a response.
            let response = messages::proposal_response_success();
            serialize::write_message(&mut connection, &*response).unwrap();
        });
        // Propose. It's a marriage made in heaven! :)
        client.propose(to_propose).unwrap();
        child.join().unwrap();
    }
}
