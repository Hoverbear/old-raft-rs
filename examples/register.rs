extern crate docopt;
extern crate env_logger;
extern crate raft;
extern crate rustc_serialize;

use std::net::SocketAddr;
use std::str::FromStr;

use docopt::Docopt;

use raft::{
    state_machine,
    store,
    ServerId,
    Server
};

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
          current value matches an expected value, returning true if the
          register was set.

  server  Starts a register server. Servers must be provided a unique ID and
          address (ip:port) at startup, along with the ID and address of all
          peer servers.

Usage:
  register get (<server-address>)...
  register put new-value (<server-address>)...
  register cas <expected-value> <new-value> (<server-address>)...
  register server <id> <address> [<peer-id> <peer-address>]...
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

    arg_id: u64,
    arg_address: String,
    arg_peer_id: Vec<u64>,
    arg_peer_address: Vec<String>,
    arg_server_address: Vec<String>,

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
    let store = store::MemStore::new();
    let state_machine = state_machine::RegisterStateMachine::new();

    let id = ServerId::from(args.arg_id);
    let addr = parse_addr(&args.arg_address);
    let peers = args.arg_peer_id
                    .iter()
                    .zip(args.arg_peer_address.iter())
                    .map(|(&id, addr)| (ServerId::from(id), parse_addr(&addr)))
                    .collect();

    Server::run(id, addr, peers, store, state_machine).unwrap();
}

fn get(_args: &Args) {
    panic!("unimplemented: waiting on changes to the Raft Client and StateMachine APIs");
}

fn put(_args: &Args) {
    panic!("unimplemented: waiting on changes to the Raft Client and StateMachine APIs");
}

fn cas(_args: &Args) {
    panic!("unimplemented: waiting on changes to the Raft Client and StateMachine APIs");
}
