@0xbdca3d7c76dab735;

struct Message {
    union {
        appendEntriesRequest @0 :AppendEntriesRequest;
        appendEntriesResponse @1 :AppendEntriesResponse;
        requestVoteResponse @2 :RequestVoteResponse;
        requestVoteRequest @3 :RequestVoteRequest;
        clientAppendRequest @4 :ClientAppendRequest;
        clientAppendResponse @5 :ClientAppendResponse;
    }
}

struct AppendEntriesRequest {

  term @0 :UInt64;
  # The leader's term.

  prevLogIndex @1 :UInt64;
  # Index of log entry immediately preceding new ones.

  prevLogTerm @2 :UInt64;
  # Term of prevLogIndex entry.

  entries @3 :List(Data);
  # Log entries to store (empty for heartbeat; may send more than one for
  # efficiency).

  leaderCommit @4 :UInt64;
  # The Leader’s commit log index.
}

struct AppendEntriesResponse {

  term @0 :UInt64;
  # The responder's current term.

  union {
    success @1 :UInt64;
    # The `AppendEntries` request was a success. The responder's latest log
    # index is returned.

    staleTerm @2 :Void;
    # The `AppendEntries` request failed because the follower has a greater term
    # than the leader.

    inconsistentPrevEntry @3 :Void;
    # The `AppendEntries` request failed because the follower failed the
    # previous entry term and index checks.

    internalError @4 :Text;
    # an internal error occured; a description is included.
  }
}

struct RequestVoteRequest {

  term @0 :UInt64;
  # The candidate's term.

  lastLogIndex @1 :UInt64;
  # The index of the candidate's last log entry.

  lastLogTerm @2 :UInt64;
  # The term of the candidate's last log entry.
}

struct RequestVoteResponse {

  term @0 :UInt64;
  # The responder's current term.

  union {
    granted @1 :Void;
    # The voter voted for the candidate.

    staleTerm @2 :Void;
    # The `RequestVote` request failed because the voter has a greater term
    # than the candidate.

    alreadyVoted @3 :Void;
    # The voter did not vote for the candidate, because the voter already voted
    # in the term.

    inconsistentLog @4 :Void;
    # The `RequestVote` request failed because the candidate's log is not
    # up-to-date with the voter's log.

    internalError @5 :Text;
    # An internal error occured; a description is included.
  }
}

struct ClientAppendRequest {
    entry @0 :Data;
    # An entry to append.
}

struct ClientAppendResponse {
    union {
        success @0 :Void;
        # The client request succeeded.

        unknownLeader @1 :Void;
        # The client request failed because the Raft node is not the leader,
        # and does not know who the leader is.

        notLeader @2 :Text;
        # The client request failed because the Raft node is not the leader.
        # The value returned may be the address of the current leader.
    }
}
