extern crate "rustc-serialize" as rustc_serialize;
use rustc_serialize::{json, Encodable, Decodable};


/// Data interchange format for RPC calls. These should match directly to the Raft paper's RPC
/// descriptions.
#[derive(RustcEncodable, RustcDecodable, Debug, Clone)]
pub enum RemoteProcedureCall<T> {
    AppendEntries {
        term: u64,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: T,
        entries: Vec<T>,
        leader_commit: u64,
    },
    RequestVote {
        term: u64,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64,
    },
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
#[derive(Debug, Clone)]
pub enum ClientRequest<T> {
    /// Gets the log entries from start to end.
    IndexRange { start_index: u64, end_index: u64 },
    /// Asks the node to append an entry after a given entry.
    /// TODO: This interface might be bad.
    AppendEntries { prev_log_index: u64, prev_log_term: T, entries: Vec<T> },
}

/// Data interchange response format for Client <-> Node communication.
/// Each variant of this is a response to a `ClientRequest`.
#[derive(Debug, Clone)]
pub enum ClientResponse<T> {
    /// Returns the entries requested as well as the index of the last item (thus the rest can be
    /// trivially calculated.) Note this will only return entries that have been commited by the
    /// majority of nodes, and will not necessarily reflect the most up to date information.
    Accepted { entries: Vec<T>, last_index: u64 },
    /// Returns the reason why it failed.
    /// TODO: This probably shouldn't be a string.
    Rejected { reason: String },
}

