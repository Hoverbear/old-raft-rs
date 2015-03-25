/// This module exposes a variety of data-interchange formats used by the library. In general,
/// a consumer of the library won't need to utilize any of these.

use rustc_serialize::{Encodable};
use uuid::Uuid;

use LogIndex;
use Term;

/// Data interchange format for RPC calls. These should match directly to the Raft paper's RPC
/// descriptions.
#[derive(RustcEncodable, RustcDecodable, Debug, Clone)]
pub enum RemoteProcedureCall<T> {
    AppendEntries(AppendEntries<T>),
    RequestVote(RequestVote),
}

#[derive(RustcEncodable, RustcDecodable, Debug, Clone)]
pub struct AppendEntries<T> {
    pub term: Term,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<(Term, T)>,
    pub leader_commit: LogIndex,
    pub uuid: Uuid, // For tracking ACKs
}

#[derive(RustcEncodable, RustcDecodable, Debug, Clone)]
pub struct RequestVote {
    pub term: Term,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
    pub uuid: Uuid, // For tracking ACKs
}

impl<T> RemoteProcedureCall<T> {
    /// Returns (term, success)
    pub fn append_entries(term: Term,
                          prev_log_index: LogIndex, prev_log_term: Term,
                          entries: Vec<(Term, T)>, leader_commit: LogIndex)
                          -> (Uuid, RemoteProcedureCall<T>) {
        let id = Uuid::new_v4();
        (id.clone(), RemoteProcedureCall::AppendEntries(AppendEntries::<T> {
            term: term,
            prev_log_index: prev_log_index,
            prev_log_term: prev_log_term,
            entries: entries,
            leader_commit: leader_commit,
            uuid: id,
        }))
    }

    /// Returns (term, voteGranted)
    pub fn request_vote(term: Term,
                        last_log_index: LogIndex, last_log_term: Term)
                        -> (Uuid, RemoteProcedureCall<T>) {
        let id = Uuid::new_v4();
        (id.clone(), RemoteProcedureCall::RequestVote(RequestVote {
            term: term,
            last_log_index: last_log_index,
            last_log_term: last_log_term,
            uuid: id,
        }))
    }
}

/// Data interchange format for RPC responses.
/// * `Accepted` mean that it worked.
/// * `Rejected` means that `rpc.term < node.persistent_state.current_term` or if the
/// Node's `log` doesn't contain the entry at `rpc.prev_log_index` that maches `prev_log_term`.
/// The caller should follow the `current_leader` it is directed to.
/// The UUID should match the coresponding RPC.
#[derive(RustcEncodable, RustcDecodable, Debug)]
pub enum RemoteProcedureResponse {
    Accepted(Accepted),
    Rejected(Rejected),
}

#[derive(RustcEncodable, RustcDecodable, Debug)]
pub struct Accepted {
    pub uuid: Uuid,
    pub term: Term,
    pub match_index: LogIndex, // For Leader State
    pub next_index: LogIndex,  // For Leader State
}

#[derive(RustcEncodable, RustcDecodable, Debug)]
pub struct Rejected {
    pub uuid: Uuid,
    pub term: Term,
    pub match_index: LogIndex, // For Leader State
    pub next_index: LogIndex,  // For Leader State
}

impl RemoteProcedureResponse {
    /// Creates a new RemoteProcedureResponse::Accepted.
    pub fn accept(uuid: Uuid, term: Term, match_index: LogIndex, next_index: LogIndex) -> RemoteProcedureResponse {
        RemoteProcedureResponse::Accepted(Accepted {
            uuid: uuid,
            term: term,
            match_index: match_index,
            next_index: next_index,
        })
    }

    /// Creates a new RemoteProcedureResponse::rejected.
    pub fn reject(uuid: Uuid, term: Term,
                  match_index: LogIndex, next_index: LogIndex)
                  -> RemoteProcedureResponse {
        RemoteProcedureResponse::Rejected(Rejected {
            uuid: uuid,
            term: term,
            match_index: match_index,
            next_index: next_index,
        })
    }
}

/// Data interchange request format for Client <-> Node Communication.
/// Each variant of this is a command which can be asked of the `RaftNode` after it is spun up with
/// `node.spinup()` The node attached to this application will poll it's channel regularly and
/// return results on the channel.
/// If you're wondering where vote requesting is, it's hidden within the module.
/// TODO: Currently requests are not queued or gaurenteed to serviced in order. This is probably a
/// bad thing as most clients will `.send()` then `.recv()`. We can probably make a service queue for this.
#[derive(RustcEncodable, RustcDecodable, Debug, Clone)]
pub enum ClientRequest<T> {
    /// Gets the log entries from start to end.
    IndexRange(IndexRange),
    /// Asks the node to append an entry after a given entry.
    AppendRequest(AppendRequest<T>),
}

#[derive(RustcEncodable, RustcDecodable, Debug, Clone)]
pub struct IndexRange {
    pub start_index: LogIndex,
    pub end_index: LogIndex,
}

#[derive(RustcEncodable, RustcDecodable, Debug, Clone)]
pub struct AppendRequest<T> {
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<T>,
}

impl<T> ClientRequest<T> {
    /// Returns (term, success)
    pub fn index_range(start: LogIndex, end: LogIndex) -> ClientRequest<T> {
        ClientRequest::IndexRange(IndexRange {
            start_index: start,
            end_index: end,
        })
    }

    /// Returns (term, voteGranted)
    pub fn append_request(prev_log_index: LogIndex, prev_log_term: Term, entries: Vec<T>) -> ClientRequest<T> {
        ClientRequest::AppendRequest(AppendRequest {
            prev_log_index: prev_log_index,
            prev_log_term: prev_log_term,
            entries: entries
        })
    }
}
