use std::collections::HashSet;
use std::io::{BufStream, Write};
use std::net::SocketAddr;
use std::net::TcpStream;

use capnp::{serialize, MessageReader, ReaderOptions};

use messages_capnp::{message, propose_response};
use messages;
use Result;

pub struct Client {
    leader_connection: Option<BufStream<TcpStream>>,
    cluster: HashSet<SocketAddr>,
}

impl Client {

    /// Creates a new client.
    pub fn new(cluster: HashSet<SocketAddr>) -> Client {
        Client {
            leader_connection: None,
            cluster: cluster,
        }
    }

    /// Proposes an entry to be appended to the replicated log. This will only
    /// return once the entry has been durably committed.
    pub fn propose(&mut self, entry: &[u8]) -> Result<()> {
        let mut message = messages::propose_request(entry);

        let mut members = self.cluster.iter().cloned().cycle();

        loop {
            let mut connection = match self.leader_connection.take() {
                Some(cxn) => cxn,
                None => {
                    let leader = members.next().unwrap();
                    debug!("connecting to potential leader {}", leader);
                    BufStream::new(try!(TcpStream::connect(leader)))
                }
            };

            try!(serialize::write_message(&mut connection, &mut message));
            try!(connection.flush());
            let response = try!(serialize::read_message(&mut connection, ReaderOptions::new()));

            match try!(response.get_root::<message::Reader>()).which().unwrap() {
                message::Which::ProposeResponse(Ok(response)) => {
                    match response.which().unwrap() {
                        propose_response::Which::Success(()) => {
                            self.leader_connection = Some(connection);
                            return Ok(())
                        },
                        propose_response::Which::UnknownLeader(()) => (),
                        propose_response::Which::NotLeader(leader) => {
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
