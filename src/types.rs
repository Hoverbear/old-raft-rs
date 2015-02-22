extern crate "rustc-serialize" as rustc_serialize;
extern crate uuid;

use uuid::Uuid;
use rustc_serialize::{json, Encodable, Decodable};
use rustc_serialize::base64::{ToBase64, FromBase64, Config, CharacterSet, Newline};
use types::NodeState::{Leader, Follower, Candidate};
use types::TransactionState::{Polling, Accepted, Rejected};
use std::fs::{File, OpenOptions};
use std::fs;
use std::str;
use std::str::StrExt;
use std::io;
use std::io::{Write, ReadExt, Seek};
use std::old_io::IoError;
use std::marker;
use std::collections::VecDeque;

/// Persistent state
/// **Must be updated to stable storage before RPC response.**
pub struct PersistentState<T: Encodable + Decodable + Send + Clone> {
    current_term: u64,
    voted_for: Option<u64>,      // request_vote cares if this is `None`
    log: File,
    last_index: u64,             // The last index of the file.
    last_term: u64,              // The last index of the file.
    marker: marker::PhantomData<T>, // A marker... Because of
    // https://github.com/rust-lang/rfcs/blob/master/text/0738-variance.md#the-corner-case-unused-parameters-and-parameters-that-are-only-used-unsafely
}

impl<T: Encodable + Decodable + Send + Clone> PersistentState<T> {
    pub fn new(current_term: u64, log_path: Path) -> PersistentState<T> {
        let mut open_opts = OpenOptions::new();
        open_opts.read(true);
        open_opts.write(true);
        open_opts.create(true);
        let mut file = open_opts.open(&log_path).unwrap();
        write!(&mut file, "{:20} {:20}\n", current_term, 0).unwrap();
        PersistentState {
            current_term: current_term,
            voted_for: None,
            log: file,
            last_index: 0,
            last_term: 0,
            marker: marker::PhantomData,
        }
    }
    /// Gets the `last_index` which you can use to make append requests with.
    pub fn get_last_index(&self) -> u64 { self.last_index }
    pub fn get_last_term(&self) -> u64 { self.last_term }
    /// Gets the `current_term` which is used for request vote.
    pub fn get_current_term(&self) -> u64 { self.current_term }
    /// Sets the current_term. **This should reflect on stable storage.**
    pub fn set_current_term(&mut self, new: u64) -> io::Result<()> {
        // The first line is the header with `current_term`, `voted_for`.
        self.log.seek(io::SeekFrom::Start(0)); // Take the start.
        self.current_term = new;
        // TODO: What do we do about the none case?
        write!(&mut self.log, "{:20} {:20}\n", self.current_term, self.voted_for.unwrap_or(0))
    }
    /// Increments the current_term. **This should reflect on stable storage.**
    pub fn inc_current_term(&mut self) { self.current_term += 1 }
    /// Gets the `voted_for`.
    pub fn get_voted_for(&mut self) -> Option<u64> { self.voted_for }
    /// Sets the `voted_for. **This should reflect on stable storage.**
    pub fn set_voted_for(&mut self, new: Option<u64>) -> io::Result<()> {
        // The first line is the header with `current_term`, `voted_for`.
        self.log.seek(io::SeekFrom::Start(0)); // Take the start.
        self.voted_for = new;
        // TODO: What do we do about the none case?
        write!(&mut self.log, "{:20} {:20}\n", self.current_term, self.voted_for.unwrap_or(0))
    }
    pub fn append_entries(&mut self, prev_log_index: u64, prev_log_term: u64,
                      entries: Vec<(u64, T)>) -> io::Result<()> {
        // TODO: No checking of `prev_log_index` & `prev_log_term` yet... Do we need to?
        let position = try!(self.move_to(prev_log_index + 1));
        self.last_index = prev_log_index;
        let number = {
            let len = entries.len();
            if len == 0 { return Ok(()) } else { len }
        };
        let last_term = entries[number-1].0; // -1 for zero index.
        try!(self.purge_from_bytes(position)); // Update `last_log_index` later.
        // TODO: Possibly purge.
        for (term, entry) in entries {
            // TODO: I don't like the "doubling" here. How can we do this better?
            write!(&mut self.log, "{} {}\n", term, PersistentState::encode(entry));
        }
        self.last_index = if self.last_index == 0 { // Empty
            number as u64
        } else { self.last_index + number as u64 };
        self.last_term = last_term;
        Ok(())
    }
    fn encode(entry: T) -> String {
        let json_encoded = json::encode(&entry)
            .unwrap(); // TODO: Don't unwrap.
        json_encoded.as_bytes().to_base64(Config {
            char_set: CharacterSet::UrlSafe,
            newline: Newline::LF,
            pad: false,
            line_length: None,
        })
    }
    fn decode(bytes: String) -> Result<T, rustc_serialize::json::DecoderError> {
        let based = bytes.from_base64()
            .ok().expect("Decoding error. log likely corrupt.");
        let string = str::from_utf8(based.as_slice())
                .unwrap();
        json::decode::<T>(string)
    }
    /// Returns the number of bytes containing `line` lines.
    /// TODO: Cache?
    fn move_to(&mut self, line: u64) -> io::Result<u64> {
        // Gotcha: The first line is NOT a log entry.
        let mut lines_read = 0u64;
        self.log.seek(io::SeekFrom::Start(0)); // Take the start.
        // Go until we've reached `from` new lines.
        let _ = self.log.by_ref().chars().skip_while(|opt| {
            match *opt {
                Ok(val) => {
                    if val == '\n' {
                        lines_read += 1;
                        // Greater than because 1 indexed.
                        if lines_read >= line {
                            false // At right location.
                        } else {
                            true // Not done yet, more lines to go.
                        }
                    } else {
                        true // Not a new line.
                    }
                },
                _ => false // At EOF. Nothing to purge.
            }
        }).next(); // Side effects.
        self.log.seek(io::SeekFrom::Current(0)) // Where are we?
    }
    /// Do *not* invoke this unless you update the `last_index`!
    fn purge_from_bytes(&mut self, from_bytes: u64) -> io::Result<()> {
        self.log.set_len(from_bytes) // Chop off the file at the given position.
    }
    /// Removes all entries from `from` to the last entry, inclusively.
    pub fn purge_from_index(&mut self, from_line: u64) -> io::Result<()> {
        let position = try!(self.move_to(from_line));
        let last_index = from_line - 1; // We're 1 indexed.
        self.last_index = last_index;
        self.last_term = try!(self.retrieve_entry(last_index)).0;
        self.purge_from_bytes(position)
    }
    pub fn retrieve_entries(&mut self, start: u64, end: u64) -> io::Result<Vec<(u64, T)>> {
        let position = self.move_to(start);
        let mut index = start;
        let mut out = vec![];
        let mut read_in = self.log.by_ref()
            .chars()
            .take_while(|val| val.is_ok())
            .filter_map(|val| val.ok()); // We don't really care about issues here.
        for index in range(start, end +1) {
            let mut chars = read_in.by_ref()
                .take_while(|&val| val != '\n')
                .collect::<String>();
            if chars.len() == 0 { continue; }
            let entry = try!(parse_entry::<T>(chars));
            out.push(entry);
        }
        Ok(out)
    }
    pub fn retrieve_entry(&mut self, index: u64) -> io::Result<(u64, T)> {
        let position = self.move_to(index);
        let mut chars = self.log.by_ref()
            .chars()
            .take_while(|val| val.is_ok())
            .filter_map(|val| val.ok()) // We don't really care about issues here.
            .take_while(|&val| val != '\n').collect::<String>();
        parse_entry::<T>(chars)
    }
}

fn parse_entry<T: Encodable + Decodable + Send + Clone>(val: String) -> io::Result<(u64, T)> {
    let mut splits = val.split(' ');
    let term = {
        let chunk = splits.next()
            .and_then(|v| v.parse::<u64>().ok());
        match chunk {
            Some(v) => v,
            None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "Could not parse term.", None)),
        }
    };
    let encoded = {
        let chunk = splits.next();
        match chunk {
            Some(v) => v,
            None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "Could not parse encoded data.", None)),
        }
    };
    let decoded: T = PersistentState::decode(encoded.to_string())
        .ok().expect("Could not unwrap log entry.");
    Ok((term, decoded))
}

/// Volatile state
#[derive(Copy)]
pub struct VolatileState {
    pub commit_index: u64,
    pub last_applied: u64
}

/// Leader Only
/// **Reinitialized after election.**
#[derive(PartialEq, Eq, Clone)]
pub struct LeaderState {
    pub next_index: Vec<u64>,
    pub match_index: Vec<u64>
}


/// Nodes can either be:
///
///   * A `Follower`, which replicates AppendEntries requests and votes for it's leader.
///   * A `Leader`, which leads the cluster by serving incoming requests, ensuring data is
///     replicated, and issuing heartbeats..
///   * A `Candidate`, which campaigns in an election and may become a `Leader` (if it gets enough
///     votes) or a `Follower`, if it hears from a `Leader`.
#[derive(PartialEq, Eq, Clone)]
pub enum NodeState {
    Follower(VecDeque<Transaction>),
    Leader(LeaderState),
    Candidate(Vec<Transaction>),
}

#[derive(PartialEq, Eq, Clone)]
pub struct Transaction {
    pub uuid: Uuid,
    pub state: TransactionState,
}

/// Used to signify the state of of a Request/Response pair. This is only needed
/// on the original sender... not on the reciever.
#[derive(PartialEq, Eq, Copy, Clone)]
pub enum TransactionState {
    Polling,
    Accepted,
    Rejected,
}

#[test]
fn test_persistent_state() {
    let path = Path::new("/tmp/test_path");
    fs::remove_file(&path.clone());
    let mut state = PersistentState::new(0, path.clone());
    // Add 1
    assert_eq!(state.append_entries(0, 0, // Zero is the initialization state.
        vec![(1, "One".to_string())]),
        Ok(()));
    // Check 1
    assert_eq!(state.retrieve_entry(1),
        Ok((1, "One".to_string())));
    assert_eq!(state.get_last_index(), 1);
    assert_eq!(state.get_last_term(), 1);
    // Do a blank check.
    assert_eq!(state.append_entries(1,1, vec![]),
        Ok(()));
        assert_eq!(state.get_last_index(), 1);
        assert_eq!(state.get_last_term(), 1);
    // Add 2
    assert_eq!(state.append_entries(1, 1,
        vec![(2, "Two".to_string())]),
        Ok(()));
    assert_eq!(state.get_last_index(), 2);
    assert_eq!(state.get_last_term(), 2);
    // Check 1, 2
    assert_eq!(state.retrieve_entries(1, 2),
        Ok(vec![(1, "One".to_string()),
                (2, "Two".to_string())
        ]));
    // Check 2
    assert_eq!(state.retrieve_entry(2),
        Ok((2, "Two".to_string())));
    // Add 3, 4
    assert_eq!(state.append_entries(2, 2,
        vec![(3, "Three".to_string()),
             (4, "Four".to_string())]),
        Ok(()));
    // Check 3, 4
    assert_eq!(state.retrieve_entries(3, 4),
        Ok(vec![(3, "Three".to_string()),
                (4, "Four".to_string())
        ]));
    assert_eq!(state.get_last_index(), 4);
    assert_eq!(state.get_last_term(), 4);
    // Remove 3, 4
    assert_eq!(state.purge_from_index(3),
        Ok(()));
    assert_eq!(state.get_last_index(), 2);
    assert_eq!(state.get_last_term(), 2);
    // Check 3, 4 are removed, and that code handles lack of entry gracefully.
    assert_eq!(state.retrieve_entries(0, 4),
        Ok(vec![(1, "One".to_string()),
                (2, "Two".to_string())
        ]));
    // Add 3, 4, 5
    assert_eq!(state.append_entries(2, 2,
        vec![(3, "Three".to_string()),
             (4, "Four".to_string()),
             (5, "Five".to_string())]),
        Ok(()));
    assert_eq!(state.get_last_index(), 5);
    assert_eq!(state.get_last_term(), 5);
    // Add 3, 4 again. (5 should be purged)
    assert_eq!(state.append_entries(2, 2,
        vec![(3, "Three".to_string()),
             (4, "Four".to_string())]),
        Ok(()));
    assert_eq!(state.retrieve_entries(0, 4),
        Ok(vec![(1, "One".to_string()),
                (2, "Two".to_string()),
                (3, "Three".to_string()),
                (4, "Four".to_string()),
        ]));
    assert_eq!(state.get_last_index(), 4);
    assert_eq!(state.get_last_term(), 4);
    // Do a blank check.
    assert_eq!(state.append_entries(4,4, vec![]),
        Ok(()));
    assert_eq!(state.get_last_index(), 4);
    assert_eq!(state.get_last_term(), 4);
    fs::remove_file(&path.clone());
}
