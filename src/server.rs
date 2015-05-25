use std::{fmt, thread};
use std::collections::{HashSet, VecDeque};
use std::io::BufReader;
use std::net::SocketAddr;

use mio::tcp::{TcpListener, TcpStream};
use mio::util::Slab;
use mio::{
    EventLoop,
    Handler,
    Interest,
    PollOpt,
    ReadHint,
    Token,
    TryRead,
    TryWrite,
};
use mio::buf::{Buf, RingBuf};
use rand::{self, Rng};
use capnp::{
    serialize_packed,
    MallocMessageBuilder,
    MessageBuilder,
    MessageReader,
    OwnedSpaceMessageReader,
    ReaderOptions,
};

use messages_capnp::message;
use replica::{Replica, EmitType};
use state_machine::StateMachine;
use store::Store;
use super::{Error, Result};

// MIO Tokens
const ELECTION_TIMEOUT: Token = Token(0);
const HEARTBEAT_TIMEOUT: Token = Token(1);
const LISTENER:  Token = Token(2);

const ELECTION_MIN: u64 = 1500;
const ELECTION_MAX: u64 = 3000;
const HEARTBEAT_DURATION: u64 = 500;
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
        //listener.set_reuseaddr(true).unwrap();
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
                connections: Slab::new_starting_at(Token(3), 128),
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
        debug!("{:?}: Writeable {:?}", self, token);
        match token {
            ELECTION_TIMEOUT => unreachable!(),
            HEARTBEAT_TIMEOUT => unreachable!(),
            LISTENER => unreachable!(),
            tok => {
                self.connections[tok].writable(reactor).unwrap();
            }
        }
    }

    /// A registered IoHandle has available data to read
    fn readable(&mut self, reactor: &mut EventLoop<Server<S, M>>, token: Token, _hint: ReadHint) {
        debug!("{:?}: Readable {:?}", self, token);
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
        debug!("{:?}: Timeout", self);
        let mut message_builder = MallocMessageBuilder::new_default();
        let emit_type = {
            let message = message_builder.init_root::<message::Builder>();
            match token {
                ELECTION_TIMEOUT => {
                    // Set timeout.
                    let timeout = rand::thread_rng().gen_range::<u64>(ELECTION_MIN, ELECTION_MAX);
                    reactor.timeout_ms(ELECTION_TIMEOUT, timeout).unwrap();

                    self.replica.election_timeout(message.init_request_vote_request())
                },
                HEARTBEAT_TIMEOUT => {
                    // Set Timeout
                    reactor.timeout_ms(HEARTBEAT_TIMEOUT, HEARTBEAT_DURATION).unwrap();

                    self.replica.heartbeat_timeout(message.init_append_entries_request())
                },
                _ => unreachable!(),
            }
        };
        debug!("{:?}: emit_type: {:?}", self, emit_type);
        // Send if necessary.
        match emit_type {
            EmitType::None => (),
            EmitType::Broadcast => {
                let mut buf = RingBuf::new(RINGBUF_SIZE);
                serialize_packed::write_message(
                    &mut buf,
                    &mut message_builder
                ).unwrap();
                for connection in self.connections.iter_mut() {
                    connection.add_write(reactor, buf.clone()).unwrap();
                }
            },
            _ => unreachable!(),
        }
    }
}

impl <S, M> fmt::Debug for Server<S, M> where S: Store, M: StateMachine {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Server({})", self.listener.local_addr().unwrap())
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
    fn writable<S, M>(&mut self,
                      event_loop: &mut EventLoop<Server<S, M>>)
                      -> Result<()>
    where S: Store, M: StateMachine {
        debug!("{:?}: writable", self);
        // Attempt to write data.
        // The `current_write` buffer will be advanced based on how much we wrote.
        match self.stream.write(self.current_write.get_mut()) {
            Ok(None) => {
                // This is a buffer flush. WOULDBLOCK
                self.interest.insert(Interest::writable());
            },
            Ok(Some(_)) => {
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
        debug!("{:?}: readable", self);
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
                    self.handle_message(reader, event_loop, replica);
                },
                // It's not read entirely yet.
                // Should roll back, pending changes to bytes upstream.
                // TODO: This was fixed.
                Err(_) => {
                    unimplemented!()
                },
            }
        }
        match event_loop.reregister(&self.stream, self.token, self.interest, PollOpt::edge()) {
            Ok(()) => Ok(()),
            Err(e) => Err(Error::from(e)),
        }
    }

    /// This is called when there is a full reader available in the buffer.
    /// It handles what to do with the data.
    fn handle_message<S, M>(&mut self,
                            incoming_message: OwnedSpaceMessageReader,
                            event_loop: &mut EventLoop<Server<S, M>>,
                            replica: &mut Replica<S,M>)
    where S: Store, M: StateMachine {
        let from = self.stream.peer_addr().unwrap();
        let mut outgoing_message_builder = MallocMessageBuilder::new_default();
        let emit_type = {
            let outgoing_message = outgoing_message_builder.init_root::<message::Builder>();
            match incoming_message.get_root::<message::Reader>().unwrap().which().unwrap() {
                message::Which::AppendEntriesRequest(Ok(request)) => {
                    let response = outgoing_message.init_append_entries_response();
                    replica.append_entries_request(from, request, response)
                },
                message::Which::AppendEntriesResponse(Ok(response)) => {
                    let request = outgoing_message.init_append_entries_request();
                    replica.append_entries_response(from, response, request)
                },
                message::Which::RequestVoteRequest(Ok(request)) => {
                    let response = outgoing_message.init_request_vote_response();
                    replica.request_vote_request(from, request, response)
                },
                message::Which::RequestVoteResponse(Ok(response)) => {
                    let request = outgoing_message.init_append_entries_request();
                    replica.request_vote_response(from, response, request)
                },
                message::Which::ClientAppendRequest(Ok(request)) => {
                    let response = outgoing_message.init_client_append_response();
                    replica.client_append_request(from, request, response)
                },
                _ => panic!("cannot handle message"),
            }
        };

        match emit_type {
            EmitType::None => (),
            EmitType::Reply => {
                self.emit(event_loop, outgoing_message_builder).unwrap()
            },
            EmitType::Broadcast => {
                self.broadcast(event_loop, outgoing_message_builder).unwrap()
            },
        }
    }

    /// Push a new message into `self.next_write` for **all** connections. First serialize the
    /// message, then distribute it to avoid any extra work.
    /// // TODO: A broadcast can be done through mio's notify functionality.
    fn broadcast<S, M>(&mut self,
                       event_loop: &mut EventLoop<Server<S, M>>,
                       builder: MallocMessageBuilder)
                       -> Result<()>
    where S: Store, M: StateMachine {
        debug!("{:?}: broadcast", self);
        unimplemented!();
    }

    /// Push the new message into `self.next_write`. This does not actually send the message, it
    /// just queues it up.
    pub fn emit<S, M>(&mut self,
                      event_loop: &mut EventLoop<Server<S, M>>,
                      mut builder: MallocMessageBuilder)
                      -> Result<()>
    where S: Store, M: StateMachine  {
        debug!("{:?}: emit", self);
        let mut buf = RingBuf::new(RINGBUF_SIZE);
        serialize_packed::write_message(
            &mut buf,
            &mut builder
        ).unwrap();
        self.add_write(event_loop, buf)
    }

    /// This queues a byte buffer into the write queue. This is used primarily when message has
    /// already been packed.
    pub fn add_write<S, M>(&mut self,
                           event_loop: &mut EventLoop<Server<S, M>>,
                           buf: RingBuf)
                           -> Result<()>
    where S: Store, M: StateMachine {
        debug!("{:?}: add_write", self);
        self.next_write.push_back(buf);
        self.interest.insert(Interest::writable());
        match event_loop.reregister(&self.stream, self.token, self.interest, PollOpt::edge() | PollOpt::oneshot()) {
            Ok(()) => Ok(()),
            Err(e) => Err(Error::from(e)),
        }
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Connection({})", self.stream.peer_addr().unwrap())
    }
}
