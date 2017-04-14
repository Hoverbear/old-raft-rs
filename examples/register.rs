extern crate bincode;
extern crate docopt;
extern crate env_logger;
extern crate raft;
extern crate rustc_serialize;
extern crate serde;
#[macro_use] extern crate serde_derive;

use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::process;

use docopt::Docopt;

use raft::{
    state_machine,
    persistent_log,
    ServerId,
    Server,
    Client,
};

/// Proposal operations supported by the distributed register. Proposals may
/// mutate the register, and will be durably replicated to a quorum of peers
/// before completing.
#[derive(Serialize, Deserialize)]
enum Proposal {
    Put(String),
    Cas(String, String),
}

/// Query operations supported by the distributed register. Queries may
/// not mutate the register, and are serviced by the the current master replica.
#[derive(Serialize, Deserialize)]
enum Query {
    Get,
}

/// A response to a get, put or cas operation.
#[derive(Serialize, Deserialize)]
enum Response {
    /// The operation succeeded.
    Ok(String),
    /// The operation failed.
    Err(String),
}

static USAGE: &'static str = "
A replicated mutable value. Operations on the register have serializable
consistency, but no durability (once all register servers are terminated the
value is lost).

Each register server holds a replica of the register, and coordinates with its
peers to update the register's value according to client commands. The register
is available for reading and writing only if a majority of register servers are
available.

Commands:

  get     Returns the current value of the register.

  put     Sets the current value of the register, and returns the previous
          value.

  cas     (compare and set) Conditionally sets the value of the register if the
          current value matches an expected value, and returns the previous
          value.

  server  Starts a register server. Servers must be provided a unique ID and
          address (ip:port) at startup, along with the ID and address of all
          peer servers.

Usage:
  register get (<node-address>)...
  register put <new-value> (<node-address>)...
  register cas <expected-value> <new-value> (<node-address>)...
  register server <id> [<node-id> <node-address>]...
  register (-h | --help)

Options:
  -h --help   Show a help message.
";

#[derive(Debug, RustcDecodable)]
struct Args {
    cmd_server: bool,
    cmd_get: bool,
    cmd_put: bool,
    cmd_cas: bool,

    arg_id: Option<u64>,
    arg_node_id: Vec<u64>,
    arg_node_address: Vec<String>,
    arg_server_id: Option<u64>,

    arg_new_value: String,
    arg_expected_value: String,
}

fn main() {
    let _ = env_logger::init();
    let args: Args = Docopt::new(USAGE)
                            .and_then(|d| d.decode())
                            .unwrap_or_else(|e| e.exit());
    if args.cmd_server {
        server(&args);
    } else if args.cmd_get {
        get(&args);
    } else if args.cmd_put {
        put(&args);
    } else if args.cmd_cas {
        cas(&args);
    }
}

/// Parses a socket address from a string, or panics with an error message.
fn parse_addr(addr: &str) -> SocketAddr {
    addr.to_socket_addrs()
        .ok()
        .expect(&format!("unable to parse socket address: {}", addr))
        .next()
        .unwrap()
}

/// Creates a new client connection to the raft servers specified in the arguments.
fn create_client(args: &Args) -> Client {
    // Parse raft server addresses from arguments.
    let cluster = args.arg_node_address.iter()
        .map(|v| parse_addr(&v))
        .collect();

    Client::new(cluster)
}

/// Handles a response message by printing the value on success, or printing the
/// error and exiting on failure.
fn handle_response(response: Vec<u8>) {
    match bincode::deserialize(&response).unwrap() {
        Response::Ok(val) => println!("{}", val),
        Response::Err(err) => {
            println!("{}", err);
            process::exit(1);
        }
    }
}

/// Creates a raft server running on the current thread with options provided by `args`.
fn server(args: &Args) {
    // Creating a raft server requires several things:

    // A log implementation, which manages the persistent, replicated log.
    let log = persistent_log::MemLog::new();

    // A state machine implementation. The state machine type must be the same
    // on all nodes.
    let state_machine = RegisterStateMachine::new();

    // A unique server id.
    let id = ServerId::from(args.arg_id.unwrap());

    // A list of peers.
    let mut peers = args.arg_node_id
                    .iter()
                    .zip(args.arg_node_address.iter())
                    .map(|(&id, addr)| (ServerId::from(id), parse_addr(&addr)))
                    .collect::<HashMap<_,_>>();

    // The peer set must not include the local server's ID.
    let addr = peers.remove(&id).unwrap();

    // Run the raft server.
    Server::run(id, addr, peers, log, state_machine).unwrap();
}

/// Retrieves the value of the register from the provided raft cluster.
///
/// Panics if the get fails.
fn get(args: &Args) {
    let mut client = create_client(args);
    let request = bincode::serialize(&Query::Get, bincode::Infinite).unwrap();
    handle_response(client.query(&request).unwrap());
}

/// Sets a value for a given key in the provided raft cluster.
fn put(args: &Args) {
    let mut client = create_client(args);
    let proposal = Proposal::Put(args.arg_new_value.clone());
    let request = bincode::serialize(&proposal, bincode::Infinite).unwrap();
    handle_response(client.propose(&request).unwrap());
}

/// Atomically sets the register value if the current value equals the expected
/// value.
fn cas(args: &Args) {
    let mut client = create_client(args);
    let proposal = Proposal::Cas(args.arg_expected_value.clone(),
                                 args.arg_new_value.clone());
    let request = bincode::serialize(&proposal, bincode::Infinite).unwrap();
    handle_response(client.propose(&request).unwrap());
}

/// A state machine that holds a single mutable string value.
#[derive(Debug)]
pub struct RegisterStateMachine {
    value: String,
}

impl RegisterStateMachine {

    /// Creates a new register state machine with empty state.
    pub fn new() -> RegisterStateMachine {
        RegisterStateMachine { value: String::new() }
    }
}

/// `StateMachine` implementation that provides register semantics.
///
/// The register is mutated by calls to `apply`, and queried by calls to
/// `query`.
impl state_machine::StateMachine for RegisterStateMachine {

    fn apply(&mut self, proposal: &[u8]) -> Vec<u8> {

        let message = match bincode::deserialize::<Proposal>(&proposal) {
            Ok(proposal) => proposal,
            Err(err) => return format!("{}", err).into_bytes(),
        };

        // Encoding the current value should never fail.
        let response = bincode::serialize(&Response::Ok(self.value.clone()),
                                                 bincode::Infinite).unwrap();
        match message {
            Proposal::Put(val) => self.value = val,
            Proposal::Cas(test, new) => {
                if test == self.value {
                    self.value = new;
                }
            },
        }

        response
    }

    fn query(&self, query: &[u8]) -> Vec<u8> {
        if let Err(err) = bincode::deserialize::<Query>(&query) {
            return format!("{}", err).into_bytes();
        }

        // Encoding the current value should never fail.
        bincode::serialize(&Response::Ok(self.value.clone()),
                                  bincode::Infinite).unwrap()
    }

    fn snapshot(&self) -> Vec<u8> {
        self.value.clone().into_bytes()
    }

    fn restore_snapshot(&mut self, value: Vec<u8>) {
        self.value = String::from_utf8(value).unwrap();
    }
}
