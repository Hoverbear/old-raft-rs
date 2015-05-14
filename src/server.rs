use std::thread;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::collections::VecDeque;
use std::io::BufReader;

// MIO
use mio::tcp::{TcpListener, TcpStream};
use mio::util::Slab;
use mio::Socket;
use mio::buf::{RingBuf};
use mio::{Interest, PollOpt, Token, EventLoop, Handler, ReadHint};
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
const RINGBUF_SIZE: usize = 4096;

/// The Raft Distributed Consensus Algorithm requires two RPC calls to be available:
///
///   * `append_entries` which is used as both a heartbeat (with no payload) and the primary
///     interface for requests.
///   * `request_vote` which is used by candidates during campaigns to obtain a vote.
///
/// A `Server` acts as a replicated state machine. The server's role in the cluster depends on it's
/// own status. It will maintain both volatile state (which can be safely lost) and persistent
/// state (which must be carefully stored and kept safe).
///
/// Currently, the `Server` API is not well defined. **We are looking for feedback and suggestions.**
pub struct Server<S, M> where S: Store, M: StateMachine {
    replica: Replica<S, M>,
    // Channels and Sockets
    listener: TcpListener,
    connections: Slab<Connection>,
}

/// The implementation of the Server. In most use cases, creating a `Server` should just be
/// done via `::new()`.
impl<S, M> Server<S, M> where S: Store, M: StateMachine {

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
        debug!("Spawning Server");
        // Create an event loop
        let mut event_loop = EventLoop::<Server<S, M>>::new().unwrap();
        // Setup the socket, make it not block.
        let listener = TcpListener::bind(&addr).unwrap();
        listener.set_reuseaddr(true).unwrap();
        event_loop.register(&listener, LISTENER).unwrap();
        let timeout = rand::thread_rng().gen_range::<u64>(ELECTION_MIN, ELECTION_MAX);
        event_loop.timeout_ms(ELECTION_TIMEOUT, timeout).unwrap();
        event_loop.timeout_ms(HEARTBEAT_TIMEOUT, HEARTBEAT_DURATION).unwrap();
        let replica = Replica::new(addr, peers, store, state_machine);
        // Fire up the thread.
        thread::Builder::new().name(format!("Server {}", addr)).spawn(move || {
            let mut raft_node = Server {
                listener: listener,
                replica: replica,
                connections: Slab::new_starting_at(Token(2), 128),
            };
            event_loop.run(&mut raft_node).unwrap();
        }).unwrap();
    }
}

impl<S, M> Handler for Server<S, M> where S: Store, M: StateMachine {

    type Message = RingBuf;
    type Timeout = Token;

    /// A registered IoHandle has available writing space.
    fn writable(&mut self, reactor: &mut EventLoop<Server<S, M>>, token: Token) {
        debug!("Writeable");
        match token {
            ELECTION_TIMEOUT => unreachable!(),
            HEARTBEAT_TIMEOUT => unreachable!(),
            LISTENER => unreachable!(),
            tok => {
                self.connections[tok].writable(reactor, &mut self.replica).unwrap();
            }
        }
    }

    /// A registered IoHandle has available data to read
    fn readable(&mut self, reactor: &mut EventLoop<Server<S, M>>, token: Token, _hint: ReadHint) {
        debug!("Readable");
        match token {
            ELECTION_TIMEOUT => unreachable!(),
            HEARTBEAT_TIMEOUT => unreachable!(),
            LISTENER => {
                let stream = match self.listener.accept().unwrap() {
                    Some(s) => s,
                    None => return, // Socket isn't quite ready.
                }; // Result<Option<_>,_>
                let conn = Connection::new(stream);
                let tok = self.connections.insert(conn)
                    .ok().expect("Could not add connection to slab.");

                // Register the connection
                self.connections[tok].token = tok;
                reactor.register_opt(&self.connections[tok].stream, tok, Interest::readable(), PollOpt::edge() | PollOpt::oneshot())
                    .ok().expect("Could not register socket with event loop.");
            },
            tok => {
                self.connections[tok].readable(reactor, &mut self.replica).unwrap();
            }
        }
    }

    /// A registered timer has expired. This is either:
    ///
    /// * An election timeout, when a `Follower` node has waited too long for a heartbeat and doing
    /// to become a `Candidate`.
    /// * A heartbeat timeout, when the `Leader` node needs to refresh it's authority over the
    /// followers. Initializes and sends an `AppendEntries` request to all followers.
    fn timeout(&mut self, reactor: &mut EventLoop<Server<S, M>>, token: Token) {
        debug!("Timeout");
        let mut message = MallocMessageBuilder::new_default();
        let mut send_message = None;
        match token {
            ELECTION_TIMEOUT => {
                let request = message.init_root::<rpc_request::Builder>();
                send_message = self.replica.election_timeout(request.init_request_vote());
                // Set timeout.
                let timeout = rand::thread_rng().gen_range::<u64>(ELECTION_MIN, ELECTION_MAX);
                reactor.timeout_ms(ELECTION_TIMEOUT, timeout).unwrap();

            },
            HEARTBEAT_TIMEOUT => {
                let request = message.init_root::<rpc_request::Builder>();
                send_message = self.replica.heartbeat_timeout(request.init_append_entries());
                // Set Timeout
                reactor.timeout_ms(HEARTBEAT_TIMEOUT, HEARTBEAT_DURATION).unwrap();
            },
            _ => unreachable!(),
        }
        // Send if necessary.
        match send_message {
            Some(Broadcast) => {
                let mut buf = RingBuf::new(RINGBUF_SIZE);
                serialize_packed::write_message(
                    &mut buf,
                    &mut message
                ).unwrap();
                for connection in self.connections.iter_mut() {
                    connection.add_write(buf.clone());
                }
            },
            None => (),
        }
    }
}

struct Connection {
    stream: TcpStream,
    token: Token,
    interest: Interest,
    current_read: BufReader<RingBuf>,
    current_write: BufReader<RingBuf>,
    next_write: VecDeque<RingBuf>,
}

impl Connection {
    /// Note: The caller must manually assign `token` to what is desired.
    fn new(sock: TcpStream) -> Connection {
        Connection {
            stream: sock,
            token: Token(0), // Effectively a `null`. This needs to be assigned by the caller.
            interest: Interest::hup(),
            current_read: BufReader::new(RingBuf::new(4096)),
            current_write: BufReader::new(RingBuf::new(4096)),
            next_write: VecDeque::with_capacity(10),
        }
    }

    /// A registered IoHandle has available writing space.
    fn writable<S, M>(&mut self, event_loop: &mut EventLoop<Server<S, M>>, replica: &mut Replica<S,M>)
                      -> Result<()>
    where S: Store, M: StateMachine {
        // Attempt to write data.
        // The `current_write` buffer will be advanced based on how much we wrote.
        match self.stream.write(self.current_write.get_mut()) {
            Ok(None) => {
                // This is a buffer flush. WOULDBLOCK
                self.interest.insert(Interest::writable());
            },
            Ok(Some(r)) => {
                // We managed to write data!
                match (self.current_write.get_ref().has_remaining(), self.next_write.is_empty()) {
                    // Need to write more of what we have.
                    (true, _) => (),
                    // Need to roll over.
                    (false, false) => self.current_write = BufReader::new(self.next_write.pop_front().unwrap()),
                    // We're done writing for now.
                    (false, true) => self.interest.remove(Interest::writable()),

                }
            },
            Err(e) => return Err(Error::from(e)),
        }

        match event_loop.reregister(&self.stream, self.token, self.interest, PollOpt::edge() | PollOpt::oneshot()) {
            Ok(()) => Ok(()),
            Err(e) => Err(Error::from(e)),
        }
    }

    /// A registered IoHandle has available data to read.
    /// This does not necessarily mean that there is an entire packed item on the stream. We could
    /// get some, all of it, or none. We'll use the buffer to read in until we can find one.
    fn readable<S, M>(&mut self, event_loop: &mut EventLoop<Server<S, M>>, replica: &mut Replica<S,M>)
                      -> Result<()>
    where S: Store, M: StateMachine {
        let mut read = 0;
        match self.stream.read(self.current_read.get_mut()) {
            Ok(Some(r)) => {
                // Just read `r` bytes.
                read = r;
            },
            Ok(None) => panic!("We just got readable, but were unable to read from the socket?"),
            Err(e) => return Err(Error::from(e)),
        };
        if read > 0 {
            match serialize_packed::read_message(&mut self.current_read, ReaderOptions::new()) {
                // We have something reasonably interesting in the buffer!
                Ok(reader) => {
                    self.handle_reader(reader, event_loop, replica);
                },
                // It's not read entirely yet.
                // Should roll back, pending changes to bytes upstream.
                // TODO: This was fixed.
                Err(_) => unimplemented!(),
            }
        }
        match event_loop.reregister(&self.stream, self.token, self.interest, PollOpt::edge()) {
            Ok(()) => Ok(()),
            Err(e) => Err(Error::from(e)),
        }
    }
    
    /// This is called when there is a full reader available in the buffer.
    /// It handles what to do with the data.
    fn handle_reader<S, M>(&mut self, reader: OwnedSpaceMessageReader,
                           event_loop: &mut EventLoop<Server<S, M>>, replica: &mut Replica<S,M>)
    where S: Store, M: StateMachine {
        let mut builder_message = MallocMessageBuilder::new_default();
        let from = self.stream.peer_addr().unwrap();
        if let Ok(request) = reader.get_root::<rpc_request::Reader>() {
            match request.which().unwrap() {
                // TODO: Move these into replica?
                rpc_request::Which::AppendEntries(Ok(call)) => {
                    let builder = builder_message.init_root::<append_entries_response::Builder>();
                    match replica.append_entries_request(from, call, builder) {
                        Some(Emit) => {
                            // Special Cirsumstance Detection
                            unimplemented!();
                        },
                        None => (),
                    }
                },
                rpc_request::Which::RequestVote(Ok(call)) => {
                    let respond = {
                        let builder = builder_message.init_root::<request_vote_response::Builder>();
                        replica.request_vote_request(from, call, builder)
                    };
                    match respond {
                        Some(Emit) => {
                            // TODO
                            self.emit(builder_message);
                        },
                        None            => (),
                    }
                },
                _ => unimplemented!(),
            };
        } else if let Ok(response) = reader.get_root::<rpc_response::Reader>() {
            // We won't be responding. This is already a response.
            match response.which().unwrap() {
                rpc_response::Which::AppendEntries(Ok(call)) => {
                    let respond = {
                        let builder = builder_message.init_root::<append_entries_request::Builder>();
                        replica.append_entries_response(from, call, builder)
                    };
                    match respond {
                        Some(Emit) => {
                            // TODO
                            self.emit(builder_message);
                        },
                        None => (),
                    }
                },
                rpc_response::Which::RequestVote(Ok(call)) => {
                    let respond = {
                        let builder = builder_message.init_root::<append_entries_request::Builder>();
                        replica.request_vote_response(from, call, builder)
                    };
                    match respond {
                        Some(Broadcast) => {
                            // Won an election!
                            self.broadcast(builder_message);
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
                    let respond = {
                        let builder = builder_message.init_root::<client_response::Builder>();
                        replica.client_append(from, call, builder)
                    };
                    match respond {
                        Some(emit) => {
                            self.emit(builder_message);
                        },
                        None => (),
                    }

                },
                client_request::Which::Die(Ok(call)) => {
                    should_die = true;
                    let mut builder = builder_message.init_root::<client_response::Builder>();
                    builder.set_success(());
                    self.interest.insert(Interest::writable());
                    debug!("Got a Die request from Client({}). Reason: {}", from, call);
                },
                client_request::Which::LeaderRefresh(()) => {
                    let respond = {
                        let builder = builder_message.init_root::<client_response::Builder>();
                        replica.client_leader_refresh(from, builder)
                    };
                    match respond {
                        Some(Emit) => {
                            self.emit(builder_message);
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

    fn broadcast(&mut self, builder: MallocMessageBuilder) {
        unimplemented!();
    }

    /// Push the new message into `self.next_write`. This does not actually send the message, it
    /// just queues it up.
    pub fn emit(&mut self, mut builder: MallocMessageBuilder) {
        let mut buf = RingBuf::new(RINGBUF_SIZE);
        serialize_packed::write_message(
            &mut buf,
            &mut builder
        ).unwrap();
        self.add_write(buf);
    }

    /// This queues a byte buffer into the write queue. This is used primarily when message has
    /// already been packed.
    pub fn add_write(&mut self, buf: RingBuf) {
        self.next_write.push_back(buf);
    }
}
