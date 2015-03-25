@0xbdca3d7c76dab735;

struct RpcRequest {
    union {
        appendEntries @0 :AppendEntriesRequest;
        requestVote @1 :RequestVoteRequest;
    }
}

struct RpcResponse {
    union {
        appendEntries @0 :AppendEntriesResponse;
        requestVote @1 :RequestVoteResponse;
    }
}

struct AppendEntriesRequest {

  term @0 :UInt64;
  # The leader's term

  leader @1 :Text;
  # The leader requesting append entries

  prevLogIndex @2 :UInt64;
  # Index of log entry immediately preceding new ones

  prevLogTerm @3 :UInt64;
  # Term of prevLogIndex entry

  entries @4 :List(Data);
  # Log entries to store (empty for heartbeat; may send more than one for
  # efficiency)

  leaderCommit @5 :UInt64;
  # leader’s commit index
}

struct AppendEntriesResponse {

  union {
    success @0 :Void;
    # The `AppendEntries` request was a success

    staleTerm @1 :UInt64;
    # The `AppendEntries` request failed because the follower has a greater term
    # than the leader. The follower's term is included.

    inconsistentPrevEntry @2 :Void;
    # The `AppendEntries` request failed because the follower failed the
    # previous entry term and index checks

    internalError @3 :Text;
    # an internal error occured; a description is included.
  }
}

struct RequestVoteRequest {

  term @0 :UInt64;
  # The candidate's term

  candidate @1 :Text;
  # The candidate requesting a vote

  lastLogIndex @2 :UInt64;
  # The index of the candidate's last log entry

  lastLogTerm @3 :UInt64;
  # The term of the candidate's last log entry
}

struct RequestVoteResponse {

  union {
    granted @0 :UInt64;
    # The voter voted for the candidate; the candidate's term is included.

    staleTerm @1 :UInt64;
    # The `RequestVote` request failed because the voter has a greater term
    # than the candidate. The voter's term is included.

    alreadyVoted @2 :Void;
    # The voter did not vote for the candidate, because the voter already voted
    # in the term

    inconsistentLog @3 :Void;
    # The `RequestVote` request failed because the candidate's log is not
    # up-to-date with the voter's log

    internalError @4 :Text;
    # an internal error occured; a description is included.
  }
}
