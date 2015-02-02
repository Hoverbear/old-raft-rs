extern crate "rustc-serialize" as rustc_serialize;
use rustc_serialize::{json, Encodable, Decodable};


/// Data interchange format for RPC calls. These should match directly to the Raft paper's RPC
/// descriptions.
#[derive(RustcEncodable, RustcDecodable, Debug, Clone)]
pub enum RemoteProcedureCall<T> {
    AppendEntries(AppendEntries<T>),
    RequestVote(RequestVote),
}

#[derive(RustcEncodable, RustcDecodable, Debug, Clone)]
pub struct AppendEntries<T> {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: u64,
    pub prev_log_term: T,
    pub entries: Vec<T>,
    pub leader_commit: u64,
}


#[derive(RustcEncodable, RustcDecodable, Debug, Clone, Copy)]
pub struct RequestVote {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

impl<T> RemoteProcedureCall<T> {
    /// Returns (term, success)
    pub fn append_entries(term: u64, leader_id: u64, prev_log_index: u64,
                      prev_log_term: T, entries: Vec<T>,
                      leader_commit: u64) -> RemoteProcedureCall<T> {
        RemoteProcedureCall::AppendEntries(AppendEntries::<T> {
            term: term,
            leader_id: leader_id,
            prev_log_index: prev_log_index,
            prev_log_term: prev_log_term,
            entries: entries,
            leader_commit: leader_commit,
        })
    }

    /// Returns (term, voteGranted)
    pub fn request_vote(term: u64, candidate_id: u64, last_log_index: u64,
                    last_log_term: u64) -> RemoteProcedureCall<T> {
        RemoteProcedureCall::RequestVote(RequestVote {
            term: term,
            candidate_id: candidate_id,
            last_log_index: last_log_index,
            last_log_term: last_log_term,
        })
    }
}

/// Data interchange format for RPC responses.
/// * `Accepted` mean that it worked.
/// * `Rejected` means that `rpc.term < node.persistent_state.current_term` or if the
/// Node's `log` doesn't contain the entry at `rpc.prev_log_index` that maches `prev_log_term`.
/// The caller should follow the `current_leader` it is directed to.
#[derive(RustcEncodable, RustcDecodable, Debug, Copy)]
pub enum RemoteProcedureResponse {
    Accepted { term: u64 },
    Rejected { term: u64, current_leader: u64 },
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

#[derive(RustcEncodable, RustcDecodable, Debug, Clone, Copy)]
pub struct IndexRange {
    pub start_index: u64,
    pub end_index: u64,
}

#[derive(RustcEncodable, RustcDecodable, Debug, Clone)]
pub struct AppendRequest<T> {
    pub prev_log_index: u64,
    pub prev_log_term: T,
    pub entries: Vec<T>,
}

impl<T> ClientRequest<T> {
    /// Returns (term, success)
    pub fn index_range(start: u64, end: u64) -> ClientRequest<T> {
        ClientRequest::IndexRange(IndexRange {
            start_index: start,
            end_index: end,
        })
    }

    /// Returns (term, voteGranted)
    pub fn append_request(prev_log_index: u64, prev_log_term: T, entries: Vec<T>) -> ClientRequest<T> {
        ClientRequest::AppendRequest(AppendRequest {
            prev_log_index: prev_log_index,
            prev_log_term: prev_log_term,
            entries: entries
        })
    }
}
