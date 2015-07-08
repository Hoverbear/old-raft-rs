//! Utility functions for working with Cap'n Proto Raft messages.
#![allow(dead_code)]

use std::rc::Rc;

use capnp::{
    MallocMessageBuilder,
    MessageBuilder,
};

use {ClientId, Term, LogIndex, ServerId};
use messages_capnp::{
    client_request,
    client_response,
    connection_preamble,
    message
};

// ConnectionPreamble

pub fn server_connection_preamble(id: ServerId) -> Rc<MallocMessageBuilder> {
    let mut message = MallocMessageBuilder::new_default();
    {
        message.init_root::<connection_preamble::Builder>()
               .init_id()
               .set_server(From::from(id));
    }
    Rc::new(message)
}

pub fn client_connection_preamble(id: ClientId) -> Rc<MallocMessageBuilder> {
    let mut message = MallocMessageBuilder::new_default();
    {
        message.init_root::<connection_preamble::Builder>()
               .init_id()
               .set_client(id.as_bytes());
    }
    Rc::new(message)
}

// AppendEntries

pub fn append_entries_request(term: Term,
                              prev_log_index: LogIndex,
                              prev_log_term: Term,
                              entries: &[&[u8]],
                              leader_commit: LogIndex)
                              -> Rc<MallocMessageBuilder> {
    let mut message = MallocMessageBuilder::new_default();
    {
        let mut request = message.init_root::<message::Builder>()
                                 .init_append_entries_request();
        request.set_term(term.into());
        request.set_prev_log_index(prev_log_index.into());
        request.set_prev_log_term(prev_log_term.into());
        request.set_leader_commit(leader_commit.into());

        let mut entry_list = request.init_entries(entries.len() as u32);
        for (n, entry) in entries.iter().enumerate() {
            entry_list.set(n as u32, entry);
        }
    }
    Rc::new(message)
}

pub fn append_entries_response_success(term: Term, log_index: LogIndex) -> Rc<MallocMessageBuilder> {
    let mut message = MallocMessageBuilder::new_default();
    {
        let mut response = message.init_root::<message::Builder>()
                                  .init_append_entries_response();
        response.set_term(term.into());
        response.set_success(log_index.into());
    }
    Rc::new(message)
}

pub fn append_entries_response_stale_term(term: Term) -> Rc<MallocMessageBuilder> {
    let mut message = MallocMessageBuilder::new_default();
    {
        let mut response = message.init_root::<message::Builder>()
                                  .init_append_entries_response();
        response.set_term(term.into());
        response.set_stale_term(());
    }
    Rc::new(message)
}

pub fn append_entries_response_inconsistent_prev_entry(term: Term) -> Rc<MallocMessageBuilder> {
    let mut message = MallocMessageBuilder::new_default();
    {
        let mut response = message.init_root::<message::Builder>()
                                  .init_append_entries_response();
        response.set_term(term.into());
        response.set_inconsistent_prev_entry(());
    }
    Rc::new(message)
}

pub fn append_entries_response_internal_error(term: Term,
                                              error: &str)
                                              -> Rc<MallocMessageBuilder> {
    let mut message = MallocMessageBuilder::new_default();
    {
        let mut response = message.init_root::<message::Builder>()
                                  .init_append_entries_response();
        response.set_term(term.into());
        response.set_internal_error(error);
    }
    Rc::new(message)
}

// RequestVote

pub fn request_vote_request(term: Term,
                            last_log_index: LogIndex,
                            last_log_term: Term)
                            -> Rc<MallocMessageBuilder> {
    let mut message = MallocMessageBuilder::new_default();
    {
        let mut request = message.init_root::<message::Builder>()
                                 .init_request_vote_request();
        request.set_term(term.into());
        request.set_last_log_index(last_log_index.into());
        request.set_last_log_term(last_log_term.into());
    }
    Rc::new(message)
}

pub fn request_vote_response_granted(term: Term) -> Rc<MallocMessageBuilder> {
    let mut message = MallocMessageBuilder::new_default();
    {
        let mut response = message.init_root::<message::Builder>()
                                  .init_request_vote_response();
        response.set_term(term.into());
        response.set_granted(());
    }
    Rc::new(message)
}

pub fn request_vote_response_stale_term(term: Term) -> Rc<MallocMessageBuilder> {
    let mut message = MallocMessageBuilder::new_default();
    {
        let mut response = message.init_root::<message::Builder>()
                                  .init_request_vote_response();
        response.set_term(term.into());
        response.set_stale_term(());
    }
    Rc::new(message)
}

pub fn request_vote_response_already_voted(term: Term) -> Rc<MallocMessageBuilder> {
    let mut message = MallocMessageBuilder::new_default();
    {
        let mut response = message.init_root::<message::Builder>()
                                  .init_request_vote_response();
        response.set_term(term.into());
        response.set_already_voted(());
    }
    Rc::new(message)
}

pub fn request_vote_response_inconsistent_log(term: Term) -> Rc<MallocMessageBuilder> {
    let mut message = MallocMessageBuilder::new_default();
    {
        let mut response = message.init_root::<message::Builder>()
                                  .init_request_vote_response();
        response.set_term(term.into());
        response.set_inconsistent_log(());
    }
    Rc::new(message)
}

pub fn request_vote_response_internal_error(term: Term, error: &str) -> Rc<MallocMessageBuilder> {
    let mut message = MallocMessageBuilder::new_default();
    {
        let mut response = message.init_root::<message::Builder>()
                                  .init_request_vote_response();
        response.set_term(term.into());
        response.set_internal_error(error);
    }
    Rc::new(message)
}

// Ping

pub fn ping_request() -> MallocMessageBuilder {
    let mut message = MallocMessageBuilder::new_default();
    {
        message.init_root::<client_request::Builder>()
               .init_ping();
    }
    message
}

// Proposal

pub fn proposal_request(entry: &[u8]) -> MallocMessageBuilder {
    let mut message = MallocMessageBuilder::new_default();
    {
        message.init_root::<client_request::Builder>()
               .init_proposal()
               .set_entry(entry);
    }
    message
}

pub fn proposal_response_success() -> Rc<MallocMessageBuilder> {
    let mut message = MallocMessageBuilder::new_default();
    {
        message.init_root::<client_response::Builder>()
               .init_proposal()
               .set_success(());
    }
    Rc::new(message)
}

pub fn proposal_response_unknown_leader() -> Rc<MallocMessageBuilder> {
    let mut message = MallocMessageBuilder::new_default();
    {
        message.init_root::<client_response::Builder>()
               .init_proposal()
               .set_unknown_leader(());
    }
    Rc::new(message)
}

pub fn proposal_response_not_leader(leader_hint: &str) -> Rc<MallocMessageBuilder> {
    let mut message = MallocMessageBuilder::new_default();
    {
        message.init_root::<client_response::Builder>()
               .init_proposal()
               .set_not_leader(leader_hint);
    }
    Rc::new(message)
}
