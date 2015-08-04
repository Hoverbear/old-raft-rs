@0xbdca3d7c76dab735;

struct ConnectionPreamble {
    # Every connection opened to a Raft server, whether it is from a peer server
    # or a client, must begin with a ConnectionPreamble message. The Raft server
    # will not reply to this message, and it is safe for the connecting process
    # to immediately begin sending further messages. The connecting process must
    # include its ID, which indicates if the connecting process is a server or
    # client.

    id :union {
        server @0 :UInt64;
        # Indicates that the connecting process is a Raft peer, and that all
        # further messages in the connection (in both directions) will be of
        # type Message.

        client @1 :Data;
        # Indicates that the connecting process is a client, and that all
        # further messages sent by the client will be of type ClientRequest, and
        # all replys from the server to the client will be of type
        # ClientResponse.
    }
}

struct Message {
    union {
        appendEntriesRequest @0 :AppendEntriesRequest;
        appendEntriesResponse @1 :AppendEntriesResponse;
        requestVoteResponse @2 :RequestVoteResponse;
        requestVoteRequest @3 :RequestVoteRequest;
        installSnapshotRequest @4 :InstallSnapshotRequest;
        installSnapshotResponse @5 :InstallSnapshotResponse;
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

struct ClientRequest {
  union {
    ping @0 :PingRequest;
    proposal @1 :ProposalRequest;
    query @2 :QueryRequest;
  }
}

struct ClientResponse {
  union {
    ping @0 :PingResponse;
    proposal @1 :CommandResponse;
    query @2 :CommandResponse;
  }
}

struct PingRequest {
}

struct PingResponse {

  term @0 :UInt64;
  # The servers current term.

  index @1 :UInt64;
  # The servers current index.

  state :union {
  # The servers current state.
    leader @2 :Void;
    follower @3 :Void;
    candidate @4 :Void;
  }
}

struct ProposalRequest {
  entry @0 :Data;
  # An entry to append.
}

struct QueryRequest {
    query @0 :Data;
    # An query to issue to the state machine.
}

struct CommandResponse {
  union {
    success @0 :Data;
    # The proposal succeeded.

    unknownLeader @1 :Void;
    # The proposal failed because the Raft node is not the leader, and does
    # not know who the leader is.

    notLeader @2 :Text;
    # The client request failed because the Raft node is not the leader.
    # The value returned may be the address of the current leader.
  }
}

struct InstallSnapshotRequest {
    term @0 :UInt64;
    # Leader's term.

    leaderId @1 :UInt64;
    # So follower can redirect clients.

    lastIncludedIndex @2 :UInt64;
    # The snapshot replaces all entries up through and including this index.

    lastIncludedTerm @3 :UInt64;
    # The term of lastIncludedIndex.

    offset @4 :UInt64;
    # The byte offset where chunk is positions in snapshot file.

    data @5 :Data;
    # Raw bytes of the snapshot chunk, starting at offset.

    done @6 :Bool;
    # `true` if this is the last chunk.
}

struct InstallSnapshotResponse {
    term @0 :UInt64;
    # `currentTerm` for the `Leader` to update itself.
}
