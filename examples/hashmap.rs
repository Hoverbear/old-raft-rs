#![feature(plugin)]
#![feature(custom_derive)]
#![plugin(serde_macros)]

extern crate docopt;
extern crate env_logger;
extern crate raft;
extern crate serde;
extern crate rustc_serialize;

use std::net::SocketAddr;
use std::str::FromStr;
use std::io::{Error, Result};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

use serde::json::{self, Value};
use serde::{Serialize, Deserialize};
use docopt::Docopt;

use raft::{
    state_machine,
    persistent_log,
    ServerId,
    Server,
    Client,
};
use Message::*;

static USAGE: &'static str = "
A replicated mutable value. Operations on the register have serializable
consistency, but no durability (once all register servers are terminated the
value is lost).

Each register server holds a replica of the register, and coordinates with its
peers to update the register's value according to client commands. The register
is available for reading and writing only if a majority of register servers are
available.

Commands:

  get     Returns the current value of the key.

  put     Sets the current value of the key, and returns the previous
          value.

  cas     (compare and set) Conditionally sets the value of the key if the
          current value matches an expected value, returning true if the
          key was set.

  server  Starts a key server. Servers must be provided a unique ID and
          address (ip:port) at startup, along with the ID and address of all
          peer servers.register

Usage:
  hashmap get <key> (<node-address>)...
  hashmap put <key> <new-value> (<node-address>)...
  hashmap cas <key> <expected-value> <new-value> (<node-address>)...
  hashmap server <id> [<node-id> <node-address>]...
  hashmap (-h | --help)

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

    arg_key: String,
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
    let state_machine = HashmapStateMachine::new();

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
    let payload = json::to_string(&Message::Get(args.arg_key.clone())).unwrap();
    let response = client.query(payload.as_bytes()).unwrap();
    println!("{}", String::from_utf8(response).unwrap())
}

fn put(args: &Args) {
    let cluster = args.arg_node_address.iter()
        .map(|v| parse_addr(&v))
        .collect();
    let mut client = Client::new(cluster);
    let new_value = json::to_value(&args.arg_new_value);
    let payload = json::to_string(&Message::Put(args.arg_key.clone(), new_value)).unwrap();
    let response = client.propose(payload.as_bytes()).unwrap();
    println!("{}", String::from_utf8(response).unwrap())
}

fn cas(_args: &Args) {
    panic!("unimplemented: waiting on changes to the Raft Client and StateMachine APIs");
}

/// A state machine that holds a hashmap.
#[derive(Debug)]
pub struct HashmapStateMachine {
    map: HashMap<String, Value>,
}

#[derive(Serialize, Deserialize)]
pub enum Message {
    Get(String),
    Put(String, Value),
    Cas(String, Value, Value),
}

impl HashmapStateMachine {
    pub fn new() -> HashmapStateMachine {
        HashmapStateMachine {
            map: HashMap::new(),
        }
    }
}

impl state_machine::StateMachine for HashmapStateMachine {

    type Error = Error;

    fn apply(&mut self, new_value: &[u8]) -> Result<Vec<u8>> {
        let string = String::from_utf8_lossy(new_value);
        let message = json::from_str(&string).unwrap();

        let response = match message {
            Get(key) => {
                let old_value = &self.map.get(&key).map(|v| v.clone());
                json::to_string(old_value)
            },
            Put(key, value) => {
                let old_value = &self.map.insert(key, value);
                json::to_string(old_value)
            },
            Cas(key, old_check, new) => {
                if *self.map.get(&key).unwrap() == old_check {
                    let _ = self.map.insert(key, new);
                    json::to_string(&true)
                } else {
                    json::to_string(&false)
                }
            },
        };

        Ok(response.unwrap().into_bytes())
    }

    fn query(&self, query: &[u8]) -> Result<Vec<u8>> {
        let string = String::from_utf8_lossy(query);
        let message = json::from_str(&string).unwrap();

        let response = match message {
            Get(key) => {
                let old_value = &self.map.get(&key).map(|v| v.clone());
                json::to_string(old_value)
            },
            _ => panic!("Can't do mutating requests in query"),
        };

        Ok(response.unwrap().into_bytes())
    }

    fn snapshot(&self) -> Result<Vec<u8>> {
        Ok(json::to_string(&self.map)
            .unwrap()
            .into_bytes())
    }

    fn restore_snapshot(&mut self, snapshot_value: Vec<u8>) -> Result<()> {
        self.map = json::from_str(&String::from_utf8_lossy(&snapshot_value)).unwrap();
        Ok(())
    }
}
