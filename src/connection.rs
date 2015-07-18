use std::fmt;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::rc::Rc;

use mio::tcp::TcpStream;
use mio::Timeout as TimeoutHandle;
use mio::{
    EventLoop,
    EventSet,
    PollOpt,
    Token,
};
use capnp::{
    MallocMessageBuilder,
    OwnedSpaceMessageReader,
    ReaderOptions,
};
use capnp::serialize::{
    read_message_async,
    write_message_async,
    AsyncValue,
    ReadContinuation,
    WriteContinuation,
};

use ClientId;
use Result;
use ServerId;
use backoff::Backoff;
use messages;
use server::{Server, ServerTimeout};
use state_machine::StateMachine;
use store::Store;

fn poll_opt() -> PollOpt {
    PollOpt::edge() | PollOpt::oneshot()
}

/// The type of a connection.
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub enum ConnectionKind {
    /// A peer in the cluster.
    Peer(ServerId),
    /// A client which is asking the Raft cluster to do things.
    Client(ClientId),
    /// Something else.
    Unknown,
}

impl ConnectionKind {
    /// Returns if the `Connection` is a peer type.
    fn is_peer(&self) -> bool {
        match *self {
            ConnectionKind::Peer(..) => true,
            _ => false,
        }
    }
    /// Returns if the `Connection` is a client type.
    fn is_client(&self) -> bool {
        match *self {
            ConnectionKind::Client(..) => true,
            _ => false,
        }
    }
}

pub struct Connection {
    kind: ConnectionKind,
    addr: SocketAddr,
    stream: TcpStream,
    backoff: Backoff,
    events: EventSet,
    read_continuation: Option<ReadContinuation>,
    write_continuation: Option<WriteContinuation>,
    write_queue: VecDeque<Rc<MallocMessageBuilder>>,
    is_connected: bool,
}

impl Connection {

    /// Creates a new `Connection` wrapping the provided socket stream.
    ///
    /// The socket must already be connected.
    ///
    /// Note: the caller must manually set the token field after inserting the
    /// connection into a slab.
    pub fn unknown(socket: TcpStream) -> Result<Connection> {
        let addr = try!(socket.peer_addr());
        Ok(Connection {
            kind: ConnectionKind::Unknown,
            addr: addr,
            stream: socket,
            backoff: Backoff::with_duration_range(50, 10000),
            events: EventSet::hup() | EventSet::readable(),
            read_continuation: None,
            write_continuation: None,
            write_queue: VecDeque::new(),
            is_connected: true,
        })
    }

    /// Creates a new peer connection.
    pub fn peer(id: ServerId, addr: SocketAddr) -> Result<Connection> {
        let stream = try!(TcpStream::connect(&addr));
        Ok(Connection {
            kind: ConnectionKind::Peer(id),
            addr: addr,
            stream: stream,
            backoff: Backoff::with_duration_range(50, 10000),
            events: EventSet::hup() | EventSet::readable(),
            read_continuation: None,
            write_continuation: None,
            write_queue: VecDeque::new(),
            is_connected: true,
        })
    }

    pub fn kind(&self) -> &ConnectionKind {
        &self.kind
    }

    pub fn set_kind(&mut self, kind: ConnectionKind) {
        self.kind = kind;
    }

    /// Writes queued messages to the socket.
    pub fn writable(&mut self) -> Result<()> {
        scoped_trace!("{:?}: writable; queued message count: {}", self, self.write_queue.len());
        scoped_assert!(self.is_connected, "{:?}: writable event while not connected", self);

        while let Some(message) = self.write_queue.pop_front() {
            let continuation = self.write_continuation.take();
            match write_message_async(&mut self.stream, &*message, continuation) {
                Ok(AsyncValue::Complete(())) => (),
                Ok(AsyncValue::Continue(continuation)) =>  {
                    // The write only partially completed. Save the continuation and add the
                    // message back to the front of the queue.
                    self.write_continuation = Some(continuation);
                    self.write_queue.push_front(message);
                    break;
                }
                Err(error) => {
                    // The write failed; reinsert the message back to the write queue.
                    self.write_queue.push_front(message);
                    return Err(From::from(error));
                }
            }
        }

        if self.write_queue.is_empty() {
            self.events.remove(EventSet::writable());
        }

        self.backoff.reset();
        Ok(())
    }

    /// Reads a message from the connection's stream, or if a full message is
    /// not available, returns `None`.
    ///
    /// Connections are edge-triggered, so the handler must continue calling
    /// until no more messages are returned.
    pub fn readable(&mut self) -> Result<Option<OwnedSpaceMessageReader>> {
        scoped_trace!("{:?}: readable", self);
        scoped_assert!(self.is_connected, "{:?}: readable event while not connected", self);

        let read = try!(read_message_async(&mut self.stream,
                                           ReaderOptions::new(),
                                           self.read_continuation.take()));
        self.backoff.reset();
        match read {
            AsyncValue::Complete(message) => {
                Ok(Some(message))
            },
            AsyncValue::Continue(continuation) => {
                // the read only partially completed. Save the continuation and return.
                self.read_continuation = Some(continuation);
                Ok(None)
            },
        }
    }

    /// Queues a message to be sent to this connection.
    pub fn send_message(&mut self, message: Rc<MallocMessageBuilder>) {
        scoped_trace!("{:?}: send_message", self);
        if self.is_connected {
            if self.write_queue.is_empty() {
                self.events.insert(EventSet::writable());
            }
            self.write_queue.push_back(message);
        }
    }

    /// Registers the connection with the event loop.
    pub fn register<S, M>(&mut self, event_loop: &mut EventLoop<Server<S, M>>, token: Token) -> Result<()>
    where S: Store, M: StateMachine {
        scoped_trace!("{:?}: register", self);
        event_loop.register_opt(&self.stream, token, self.events, poll_opt())
                  .map_err(|error| {
                      scoped_warn!("{:?}: reregister failed: {}", self, error);
                      From::from(error)
                  })
    }

    /// Reregisters the connection with the event loop.
    pub fn reregister<S, M>(&mut self, event_loop: &mut EventLoop<Server<S, M>>, token: Token) -> Result<()>
    where S: Store, M: StateMachine {
        scoped_trace!("{:?}: reregister", self);
        event_loop.reregister(&self.stream, token, self.events, poll_opt())
                  .map_err(|error| {
                      scoped_warn!("{:?}: register failed: {}", self, error);
                      From::from(error)
                  })
    }

    pub fn reconnect_peer(&mut self, id: ServerId) -> Result<()> {
        scoped_trace!("{:?}: reconnect", self);
        self.stream = try!(TcpStream::connect(&self.addr));
        self.is_connected = true;
        self.read_continuation = None;
        self.write_continuation = None;
        self.write_queue.clear();
        self.send_message(messages::server_connection_preamble(id));
        Ok(())
    }

    /// Resets a peer connection.
    pub fn reset_peer<S, M>(&mut self,
                            event_loop: &mut EventLoop<Server<S, M>>,
                            token: Token)
                            -> Result<(ServerTimeout, TimeoutHandle)>
    where S: Store, M: StateMachine {
        scoped_assert!(self.kind.is_peer());
        let duration = self.backoff.next_backoff_ms();
        self.read_continuation = None;
        self.write_continuation = None;
        self.write_queue.clear();
        self.is_connected = false;
        let timeout = ServerTimeout::Reconnect(token);
        let handle = event_loop.timeout_ms(timeout, duration).unwrap();

        scoped_info!("{:?}: reset, will attempt to reconnect in {}ms", self, duration);
        Ok((timeout, handle))
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self.kind {
            ConnectionKind::Peer(id) => {
                write!(fmt, "PeerConnection({})", id)
            },
            ConnectionKind::Client(id) => {
                write!(fmt, "ClientConnection({})", id)
            },
            ConnectionKind::Unknown => {
                write!(fmt, "UnknownConnection({})", &self.addr)
            },
        }
    }
}
