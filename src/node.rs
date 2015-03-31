use std::thread;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::ops::Deref;
use std::io;
use std::error::FromError;

// MIO
use mio::tcp::{listen, TcpListener, TcpStream};
use mio::util::Slab;
use mio::Socket;
use mio::buf::{RingBuf};
use mio::{Interest, PollOpt, NonBlock, Token, EventLoop, Handler, ReadHint};
use mio::buf::Buf;
use mio::{TryRead, TryWrite};

use rand::{self, Rng};

// Data structures.
use store::Store;
use replica::{Replica, Emit, Broadcast};
use state_machine::StateMachine;

// Cap'n Proto
use capnp::serialize_packed;
use capnp::{
    MessageBuilder,
    MessageReader,
    ReaderOptions,
    MallocMessageBuilder,
    Word,
    OwnedSpaceMessageReader,
};
use messages_capnp::{
    rpc_request,
    rpc_response,
    client_request,
    client_response,
    request_vote_response,
    append_entries_response,
    append_entries_request,
};
use super::{Error, Result};

// MIO Tokens
const ELECTION_TIMEOUT: Token = Token(0);
const HEARTBEAT_TIMEOUT: Token = Token(1);
const LISTENER:  Token = Token(2);

const ELECTION_MIN: u64 = 150;
const ELECTION_MAX: u64 = 300;
const HEARTBEAT_DURATION: u64 = 50;

/// The Raft Distributed Consensus Algorithm requires two RPC calls to be available:
///
///   * `append_entries` which is used as both a heartbeat (with no payload) and the primary
///     interface for requests.
///   * `request_vote` which is used by candidates during campaigns to obtain a vote.
///
/// A `RaftServer` acts as a replicated state machine. The server's role in the cluster depends on it's
/// own status. It will maintain both volatile state (which can be safely lost) and persistent
/// state (which must be carefully stored and kept safe).
///
/// Currently, the `RaftServer` API is not well defined. **We are looking for feedback and suggestions.**
pub struct RaftServer<S, M> where S: Store, M: StateMachine {
    replica: Replica<S, M>,
    // Channels and Sockets
    listener: NonBlock<TcpListener>,
    connections: Slab<Connection>,
}

/// The implementation of the RaftServer. In most use cases, creating a `RaftServer` should just be
/// done via `::new()`.
impl<S, M> RaftServer<S, M> where S: Store, M: StateMachine {

    /// Creates a new Raft node with the cluster members specified.
    ///
    /// # Arguments
    ///
    /// * `addr` - The address of the new node.
    /// * `peers` - The address of all peers in the Raft cluster.
    /// * `store` - The persitent log store.
    /// * `state_machine` - The client state machine to which client commands will be applied.
    pub fn spawn(addr: SocketAddr,
                 peers: HashSet<SocketAddr>,
                 store: S,
                 state_machine: M) {
        // Create an event loop
        let mut event_loop = EventLoop::<RaftServer<S, M>>::new().unwrap();
        // Setup the socket, make it not block.
        let listener = listen(&addr).unwrap();
        listener.set_reuseaddr(true);
        event_loop.register(&listener, LISTENER).unwrap();
        let timeout = rand::thread_rng().gen_range::<u64>(ELECTION_MIN, ELECTION_MAX);
        event_loop.timeout_ms(ELECTION_TIMEOUT, timeout).unwrap();
        event_loop.timeout_ms(HEARTBEAT_TIMEOUT, HEARTBEAT_DURATION);
        let replica = Replica::new(addr, peers, store, state_machine);
        // Fire up the thread.
        thread::Builder::new().name(format!("RaftServer {}", addr)).spawn(move || {
            let mut raft_node = RaftServer {
                listener: listener,
                replica: replica,
                connections: Slab::new_starting_at(Token(2), 128),
            };
            event_loop.run(&mut raft_node).unwrap();
        }).unwrap();
    }
}

impl<S, M> Handler for RaftServer<S, M> where S: Store, M: StateMachine {

    type Message = ();
    type Timeout = Token;

    /// A registered IoHandle has available writing space.
    fn writable(&mut self, reactor: &mut EventLoop<RaftServer<S, M>>, token: Token) {
        match token {
            ELECTION_TIMEOUT => unreachable!(),
            HEARTBEAT_TIMEOUT => unreachable!(),
            LISTENER => unreachable!(),
            tok => {
                self.connections[tok].writable(reactor, &mut self.replica);
            }
        }
    }

    /// A registered IoHandle has available data to read
    fn readable(&mut self, reactor: &mut EventLoop<RaftServer<S, M>>, token: Token, hint: ReadHint) {
        match token {
            ELECTION_TIMEOUT => unreachable!(),
            HEARTBEAT_TIMEOUT => unreachable!(),
            LISTENER => {
                let stream = self.listener.accept().unwrap().unwrap(); // Result<Option<_>,_>
                let conn = Connection::new(stream);
                let tok = self.connections.insert(conn)
                    .ok().expect("Could not add connection to slab.");

                // Register the connection
                self.connections[tok].token = tok;
                reactor.register_opt(&self.connections[tok].stream, tok, Interest::readable(), PollOpt::edge() | PollOpt::oneshot())
                    .ok().expect("Could not register socket with event loop.");
            },
            tok => {
                self.connections[tok].readable(reactor, &mut self.replica);
            }
        }
    }

    /// A registered timer has expired
    fn timeout(&mut self, reactor: &mut EventLoop<RaftServer<S, M>>, token: Token) {
        let mut message = MallocMessageBuilder::new_default();
        let request = message.init_root::<rpc_request::Builder>();

        match token {
            ELECTION_TIMEOUT => {
                let timeout = rand::thread_rng().gen_range::<u64>(ELECTION_MIN, ELECTION_MAX);
                let _send_message = self.replica.election_timeout(request.init_request_vote());
                reactor.timeout_ms(ELECTION_TIMEOUT, timeout).unwrap();
                // TODO: send messages if necessary
            },
            HEARTBEAT_TIMEOUT => {
                let _send_message = self.replica.heartbeat_timeout(request.init_append_entries());
                reactor.timeout_ms(HEARTBEAT_TIMEOUT, HEARTBEAT_DURATION).unwrap();
                // TODO: send messages if necessary
            },
            _ => unreachable!(),
        }
    }
}

struct Connection {
    stream: NonBlock<TcpStream>,
    token: Token,
    interest: Interest,
    current_read: RingBuf,
    current_write: RingBuf,
    next_write: Option<MallocMessageBuilder>,
}

impl Connection {
    fn new(sock: NonBlock<TcpStream>) -> Connection {
        Connection {
            stream: sock,
            token: Token(-1),
            interest: Interest::hup(),
            current_read: RingBuf::new(4096),
            current_write: RingBuf::new(4096),
            next_write: Some(MallocMessageBuilder::new_default()),
        }
    }

    fn writable<S, M>(&mut self, event_loop: &mut EventLoop<RaftServer<S, M>>, replica: &mut Replica<S,M>)
                      -> Result<()>
    where S: Store, M: StateMachine {
        // Attempt to write data.
        // The `current_write` buffer will be advanced based on how much we wrote.
        match self.stream.write(&mut self.current_write) {
            Ok(None) => {
                // This is a buffer flush. WOULDBLOCK
                self.interest.insert(Interest::writable());
            },
            Ok(Some(r)) => {
                // We managed to write data!
                // TODO: We're *probably* not ready to shoot data again?
                match (self.current_write.has_remaining(), self.next_write.is_some()) {
                    // Need to write more of what we have.
                    (true, _) => (),
                    // Need to roll over.
                    (false, true) => try!(
                        serialize_packed::write_packed_message_unbuffered(&mut self.current_write,
                                                                          self.next_write.as_mut().unwrap())
                    ),
                    // We're done writing for now.
                    (false, false) => self.interest.remove(Interest::writable()),

                }
            },
            Err(e) => panic!("Couldn't write!"),
        }

        match event_loop.reregister(&self.stream, self.token, self.interest, PollOpt::edge() | PollOpt::oneshot()) {
            Ok(()) => Ok(()),
            Err(e) => Err(Error::from_error(e)),
        }
    }

    fn readable<S, M>(&mut self, event_loop: &mut EventLoop<RaftServer<S, M>>, replica: &mut Replica<S,M>)
                      -> Result<()>
    where S: Store, M: StateMachine {
        let mut read = 0;
        let from = self.stream.peer_addr().unwrap();
        match self.stream.read(&mut self.current_read) {
            Ok(Some(r)) => {
                // Just read `r` bytes.
                read = r;
            },
            Ok(None) => panic!("We just got readable, but were unable to read from the socket?"),
            Err(e) => panic!("Error on reading."),
        };
        if read > 0 {
            match serialize_packed::new_reader_unbuffered(&mut self.current_read, ReaderOptions::new()) {
                // We have something reasonably interesting in the buffer!
                Ok(reader) => {
                    self.handle_reader(from, reader, event_loop, replica);
                },
                // It's not read entirely yet.
                Err(_) => (),
            }
        }
        match event_loop.reregister(&self.stream, self.token, self.interest, PollOpt::edge()) {
            Ok(()) => Ok(()),
            Err(e) => Err(Error::from_error(e)),
        }
    }

    fn handle_reader<S, M>(&mut self, from: SocketAddr, reader: OwnedSpaceMessageReader,
                           event_loop: &mut EventLoop<RaftServer<S, M>>, replica: &mut Replica<S,M>)
    where S: Store, M: StateMachine {
        let mut builder_message = MallocMessageBuilder::new_default();
        let from = self.stream.peer_addr().unwrap();
        if let Ok(request) = reader.get_root::<rpc_request::Reader>() {
            // We will be responding.
            let response = match request.which().unwrap() {
                // TODO: Move these into replica?
                rpc_request::Which::AppendEntries(Ok(call)) => {
                    let builder = builder_message.init_root::<append_entries_response::Builder>();
                    replica.append_entries_request(from, call, builder)
                },
                rpc_request::Which::RequestVote(Ok(call)) => {
                    let builder = builder_message.init_root::<request_vote_response::Builder>();
                    replica.request_vote_request(from, call, builder)
                },
                _ => unimplemented!(),
            };
            match response {
                Some(Emit) => {
                    // TODO
                    unimplemented!();
                    self.interest.insert(Interest::writable());
                },
                None            => (),
            }
        } else if let Ok(response) = reader.get_root::<rpc_response::Reader>() {
            // We won't be responding. This is already a response.
            match response.which().unwrap() {
                rpc_response::Which::AppendEntries(Ok(call)) => {
                    let res = builder_message.init_root::<append_entries_request::Builder>();
                    match replica.append_entries_response(from, call, res) {
                        Some(Emit) => {
                            // TODO
                            unimplemented!();
                            self.interest.insert(Interest::writable());
                        },
                        None => (),
                    }
                },
                rpc_response::Which::RequestVote(Ok(call)) => {
                    let res = builder_message.init_root::<append_entries_request::Builder>();
                    match replica.request_vote_response(from, call, res) {
                        Some(Broadcast) => {
                            // Won an election!
                            unimplemented!();
                            self.interest.insert(Interest::writable());
                        },
                        None => (),
                    }

                },
                _ => unimplemented!(),
            }
        } else if let Ok(client_req) = reader.get_root::<client_request::Reader>() {
            let mut should_die = false;
            // We will be responding.
            match client_req.which().unwrap() {
                client_request::Which::Append(Ok(call)) => {
                    let mut res = builder_message.init_root::<client_response::Builder>();
                    match replica.client_append(from, call, res) {
                        Some(emit) => {
                            unimplemented!();
                            self.interest.insert(Interest::writable());
                        },
                        None => (),
                    }

                },
                client_request::Which::Die(Ok(call)) => {
                    should_die = true;
                    let mut res = builder_message.init_root::<client_response::Builder>();
                    res.set_success(());
                    self.interest.insert(Interest::writable());
                    debug!("Got a Die request from Client({}). Reason: {}", from, call);
                },
                client_request::Which::LeaderRefresh(()) => {
                    let mut res = builder_message.init_root::<client_response::Builder>();
                    match replica.client_leader_refresh(from, res) {
                        Some(emit) => {
                            unimplemented!();
                            self.interest.insert(Interest::writable())
                        },
                        None => (),
                    }
                },
                _ => unimplemented!(),
            };

            // Do this here so that we can send the response.
            if should_die {
                panic!("Got a Die request.");
            }

        } else {
            // It's something we don't understand.
            unimplemented!();
        }
    }
}
