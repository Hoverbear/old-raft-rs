extern crate docopt;
extern crate env_logger;
extern crate raft;
extern crate rustc_serialize;
extern crate bincode;

use std::net::SocketAddr;
use std::str::FromStr;
use std::collections::HashMap;
use std::io::{Error, Result};

use docopt::Docopt;
use bincode::SizeLimit;

use raft::{
    state_machine,
    persistent_log,
    ServerId,
    Server,
    Client,
};


static USAGE: &'static str = "
A replicated mutable hashmap. Operations on the register have serializable
consistency, but no durability (once all register servers are terminated the
map is lost).

Each register server holds a replica of the map, and coordinates with its
peers to update the maps values according to client commands. The register
is available for reading and writing only if a majority of register servers are
available.

Commands:

  get     Returns the current value of the register.

  put     Sets the current value of the register, and returns the previous
          value.

  cas     (compare and set) Conditionally sets the value of the register if the
          current value matches an expected value, returning true if the
          register was set.

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

fn parse_addr(addr: &str) -> SocketAddr {
    SocketAddr::from_str(addr)
               .ok()
               .expect(&format!("unable to parse socket address: {}", addr))
}

fn server(args: &Args) {
    let persistent_log = persistent_log::MemLog::new();
    let state_machine = RegisterStateMachine::new();

    let id = ServerId::from(args.arg_id.unwrap());
    let mut peers = args.arg_node_id
                    .iter()
                    .zip(args.arg_node_address.iter())
                    .map(|(&id, addr)| (ServerId::from(id), parse_addr(&addr)))
                    .collect::<HashMap<_,_>>();

    let addr = peers.remove(&id).unwrap();
    Server::run(id, addr, peers, persistent_log, state_machine).unwrap();
}

fn get(args: &Args) {
    let cluster = args.arg_node_address.iter()
        .map(|v| parse_addr(&v))
        .collect();
    let mut client = Client::new(cluster);
    let payload = Message::Get;
    let encoded = bincode::encode(&payload, SizeLimit::Infinite).unwrap();
    let response = client.query(&encoded).unwrap();
    let val = bincode::decode::<String>(&response).unwrap();
    println!("{}", val)
}

fn put(args: &Args) {
    let cluster = args.arg_node_address.iter()
        .map(|v| parse_addr(&v))
        .collect();
    let mut client = Client::new(cluster);
    let payload = Message::Put(args.arg_new_value.clone().into_bytes());
    let encoded = bincode::encode(&payload, SizeLimit::Infinite).unwrap();
    let response = client.propose(&encoded).unwrap();
    let val = bincode::decode::<String>(&response).unwrap();
    println!("{}", val)
}

fn cas(_args: &Args) {
    panic!("unimplemented: waiting on changes to the Raft Client and StateMachine APIs");
}

#[derive(RustcEncodable, RustcDecodable, PartialEq)]
enum Message {
    Get,
    Put(Vec<u8>),
    Cas(Vec<u8>, Vec<u8>),
}

/// A state machine that holds a single mutable value.
#[derive(Debug)]
pub struct RegisterStateMachine {
    value: Vec<u8>,
}

impl RegisterStateMachine {
    pub fn new() -> RegisterStateMachine {
        RegisterStateMachine { value: vec![] }
    }
}


impl state_machine::StateMachine for RegisterStateMachine {

    type Error = Error;

    fn apply(&mut self, proposal: &[u8]) -> Result<Vec<u8>> {
        let old_value = self.value.clone();
        let message = bincode::decode::<Message>(&proposal).unwrap();
        let response = (match message {
            Message::Put(val) => {
                self.value.clear();
                self.value.extend(val);
                bincode::encode(&old_value, SizeLimit::Infinite)
            },
            Message::Get => bincode::encode(&old_value, SizeLimit::Infinite),
            Message::Cas(test, new) => {
                if test == old_value {
                    self.value.clear();
                    self.value.extend(new);
                    bincode::encode(&true, SizeLimit::Infinite)
                } else {
                    bincode::encode(&false, SizeLimit::Infinite)
                }
            },
        }).unwrap();
        Ok(response)
    }

    fn query(&self, query: &[u8]) -> Result<Vec<u8>> {
        let old_value = self.value.clone();
        let message = bincode::decode::<Message>(&query).unwrap();
        let response = (match message {
            Message::Get => bincode::encode(&old_value, SizeLimit::Infinite),
            _ => panic!("Cannot mutate from query!"),
        }).unwrap();
        Ok(response)
    }

    fn snapshot(&self) -> Result<Vec<u8>> {
        Ok(self.value.clone())
    }

    fn restore_snapshot(&mut self, snapshot_value: Vec<u8>) -> Result<()> {
        self.value = snapshot_value;
        Ok(())
    }
}
