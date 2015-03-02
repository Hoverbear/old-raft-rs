extern crate "rustc-serialize" as rustc_serialize;
extern crate uuid;

use std::collections::{hash_map, HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{self, Write, ReadExt, Seek};
use std::marker;
use std::old_io::net::ip::SocketAddr;
use std::str::{self, StrExt};
use std::string::ToString;

use rustc_serialize::base64::{ToBase64, FromBase64, Config, CharacterSet, Newline};
use rustc_serialize::{json, Encodable, Decodable};
use uuid::Uuid;

use state::NodeState::{Leader, Follower, Candidate};
use state::TransactionState::{Polling, Accepted, Rejected};
use LogIndex;
use Term;

/// Persistent state
/// **Must be updated to stable storage before RPC response.**
pub struct PersistentState<T: Encodable + Decodable + Send + Clone> {
    current_term: Term,
    voted_for: Option<SocketAddr>,  // request_vote cares if this is `None`
    log: File,
    last_index: LogIndex,           // The last index of the file.
    last_term: Term,                 // The last index of the file.
    marker: marker::PhantomData<T>, // A marker... Because of
    // https://github.com/rust-lang/rfcs/blob/master/text/0738-variance.md#the-corner-case-unused-parameters-and-parameters-that-are-only-used-unsafely
}

impl<T: Encodable + Decodable + Send + Clone> PersistentState<T> {
    pub fn new(current_term: Term, log_path: Path) -> PersistentState<T> {
        let mut open_opts = OpenOptions::new();
        open_opts.read(true);
        open_opts.write(true);
        open_opts.create(true);
        let mut file = open_opts.open(&log_path).unwrap();
        write!(&mut file, "{:20} {:40}\n", current_term.0, "").unwrap();
        PersistentState {
            current_term: current_term,
            voted_for: None,
            log: file,
            last_index: LogIndex(0),
            last_term: Term(0),
            marker: marker::PhantomData,
        }
    }
    /// Gets the `last_index` which you can use to make append requests with.
    pub fn get_last_index(&self) -> LogIndex { self.last_index }
    pub fn get_last_term(&self) -> Term { self.last_term }
    /// Gets the `current_term` which is used for request vote.
    pub fn get_current_term(&self) -> Term { self.current_term }
    /// Sets the current_term. **This should reflect on stable storage.**
    pub fn set_current_term(&mut self, term: Term) -> io::Result<()> {
        self.current_term = term;
        let voted_for = self.voted_for;
        self.set_header(term, voted_for)
    }
    /// Increments the current_term. **This should reflect on stable storage.**
    pub fn inc_current_term(&mut self) { self.current_term = self.current_term + 1 }
    /// Gets the `voted_for`.
    pub fn get_voted_for(&mut self) -> Option<SocketAddr> {
        self.voted_for
    }
    /// Sets the `voted_for. **This should reflect on stable storage.**
    pub fn set_voted_for(&mut self, node: Option<SocketAddr>) -> io::Result<()> {
        let current_term = self.current_term;
        self.voted_for = node.clone();
        self.set_header(current_term, node)
    }
    /// Set the first line of the file with a header including `current_term` and `voted_for`.
    fn set_header(&mut self, term: Term, voted_for: Option<SocketAddr>) -> io::Result<()> {
        try!(self.log.seek(io::SeekFrom::Start(0))); // Take the start.
        let candidate = voted_for.map(|v| v.to_string()).unwrap_or("".to_string());
        write!(&mut self.log, "{:20} {:40}\n", term.0, candidate)
    }
    pub fn append_entries(&mut self,
                          prev_log_index: LogIndex, prev_log_term: Term,
                          entries: Vec<(Term, T)>)
                          -> io::Result<()> {
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
            write!(&mut self.log, "{} {}\n", term.0, PersistentState::encode(entry)).unwrap();
        }
        self.last_index = if self.last_index == LogIndex(0) { // Empty
            LogIndex(number as u64)
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
    fn move_to(&mut self, index: LogIndex) -> io::Result<u64> {
        // Gotcha: The first line is NOT a log entry.
        let mut lines_read = 0u64;
        self.log.seek(io::SeekFrom::Start(0)).unwrap(); // Take the start.
        // Go until we've reached `from` new lines.
        let _ = self.log.by_ref().chars().skip_while(|opt| {
            match *opt {
                Ok(val) => {
                    if val == '\n' {
                        lines_read += 1;
                        // Greater than because 1 indexed.
                        if lines_read >= index.0 {
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
    pub fn purge_from_index(&mut self, from_index: LogIndex) -> io::Result<()> {
        let position = try!(self.move_to(from_index));
        let last_index = from_index - 1; // We're 1 indexed.
        self.last_index = last_index;
        self.last_term = self.retrieve_entry(last_index).map(|(t, _)| t).unwrap_or(Term(0));
        self.purge_from_bytes(position)
    }
    pub fn retrieve_entries(&mut self, start: LogIndex, end: LogIndex) -> io::Result<Vec<(Term, T)>> {
        let _ = self.move_to(start);
        let mut out = vec![];
        let mut read_in = self.log.by_ref()
            .chars()
            .take_while(|val| val.is_ok())
            .filter_map(|val| val.ok()); // We don't really care about issues here.
        for _ in range(start.0, end.0 + 1) {
            let chars = read_in.by_ref()
                .take_while(|&val| val != '\n')
                .collect::<String>();
            if chars.len() == 0 { continue; }
            let entry = try!(parse_entry::<T>(chars));
            out.push(entry);
        }
        Ok(out)
    }
    pub fn retrieve_entry(&mut self, index: LogIndex) -> io::Result<(Term, T)> {
        let _ = self.move_to(index);
        let chars = self.log.by_ref()
            .chars()
            .take_while(|val| val.is_ok())
            .filter_map(|val| val.ok()) // We don't really care about issues here.
            .take_while(|&val| val != '\n').collect::<String>();
        parse_entry::<T>(chars)
    }
}

fn parse_entry<T: Encodable + Decodable + Send + Clone>(val: String) -> io::Result<(Term, T)> {
    let mut splits = val.split(' ');
    let term = {
        let chunk = splits.next()
            .and_then(|v| v.parse::<u64>().ok());
        match chunk {
            Some(v) => Term(v),
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
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
}

#[derive(Clone)]
pub struct LeaderState {
    last_index: LogIndex,
    next_index: HashMap<SocketAddr, LogIndex>,
    match_index: HashMap<SocketAddr, LogIndex>,
}

impl LeaderState {

    /// Returns a new `LeaderState` struct.
    ///
    /// # Arguments
    ///
    /// * `last_index` - The index of the leader's most recent log entry at the
    ///                  time of election.
    pub fn new(last_index: LogIndex) -> LeaderState {
        LeaderState {
            last_index: last_index,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
        }
    }

    /// Returns the next log entry index of the follower node.
    pub fn next_index(&mut self, node: SocketAddr) -> LogIndex {
        match self.next_index.entry(node) {
            hash_map::Entry::Occupied(entry) => *entry.get(),
            hash_map::Entry::Vacant(entry) => *entry.insert(self.last_index + 1),
        }
    }

    /// Sets the next log entry index of the follower node.
    pub fn set_next_index(&mut self, node: SocketAddr, index: LogIndex) {
        self.next_index.insert(node, index);
    }

    /// Returns the index of the highest log entry known to be replicated on
    /// the follower node.
    pub fn match_index(&self, node: SocketAddr) -> LogIndex {
        *self.match_index.get(&node).unwrap_or(&LogIndex(0))
    }

    /// Sets the index of the highest log entry known to be replicated on the
    /// follower node.
    pub fn set_match_index(&mut self, node: SocketAddr, index: LogIndex) {
        self.match_index.insert(node, index);
    }

    /// Counts the number of follower nodes containing the given log index.
    pub fn count_match_indexes(&self, index: LogIndex) -> usize {
        self.match_index.values().filter(|&&i| i >= index).count()
    }
}

/// Nodes can either be:
///
///   * A `Follower`, which replicates AppendEntries requests and votes for it's leader.
///   * A `Leader`, which leads the cluster by serving incoming requests, ensuring data is
///     replicated, and issuing heartbeats..
///   * A `Candidate`, which campaigns in an election and may become a `Leader` (if it gets enough
///     votes) or a `Follower`, if it hears from a `Leader`.
#[derive(Clone)]
pub enum NodeState {
    Follower(VecDeque<Transaction>),
    Leader(LeaderState),
    Candidate(HashMap<SocketAddr, Transaction>),
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
fn test_update_header() {
    use std::fs;
    use std::old_io::net::ip::SocketAddr;
    use std::str::FromStr;

    let path = Path::new("/tmp/test_path");
    fs::remove_file(&path).ok();
    let mut state = PersistentState::new(Term(0), path);

    // Add 1
    assert_eq!(state.append_entries(LogIndex(0), Term(0), // Zero is the initialization state.
        vec![(Term(1), "One".to_string())]),
        Ok(()));
    // Check 1
    assert_eq!(state.retrieve_entry(LogIndex(1)),
        Ok((Term(1), "One".to_string())));
    assert_eq!(state.get_last_index(), LogIndex(1));
    assert_eq!(state.get_last_term(), Term(1));

    // Update current term
    state.set_current_term(Term(1));

    // Check again
    assert_eq!(state.retrieve_entry(LogIndex(1)),
        Ok((Term(1), "One".to_string())));
    assert_eq!(state.get_last_index(), LogIndex(1));
    assert_eq!(state.get_last_term(), Term(1));

    // Update voted for
    state.set_voted_for(Some(SocketAddr::from_str("127.0.0.1:11110").unwrap())).unwrap();

    // Check again
    assert_eq!(state.retrieve_entry(LogIndex(1)),
        Ok((Term(1), "One".to_string())));
    assert_eq!(state.get_last_index(), LogIndex(1));
    assert_eq!(state.get_last_term(), Term(1));
}

#[test]
fn test_persistent_state() {
    use std::fs;
    let path = Path::new("/tmp/test_path");
    fs::remove_file(&path.clone()).ok();
    let mut state = PersistentState::new(Term(0), path.clone());
    // Add 1
    assert_eq!(state.append_entries(LogIndex(0), Term(0), // Zero is the initialization state.
        vec![(Term(1), "One".to_string())]),
        Ok(()));
    // Check 1
    assert_eq!(state.retrieve_entry(LogIndex(1)),
        Ok((Term(1), "One".to_string())));
    assert_eq!(state.get_last_index(), LogIndex(1));
    assert_eq!(state.get_last_term(), Term(1));
    // Do a blank check.
    assert_eq!(state.append_entries(LogIndex(1), Term(1), vec![]),
        Ok(()));
        assert_eq!(state.get_last_index(), LogIndex(1));
        assert_eq!(state.get_last_term(), Term(1));
    // Add 2
    assert_eq!(state.append_entries(LogIndex(1), Term(1),
        vec![(Term(2), "Two".to_string())]),
        Ok(()));
    assert_eq!(state.get_last_index(), LogIndex(2));
    assert_eq!(state.get_last_term(), Term(2));
    // Check 1, 2
    assert_eq!(state.retrieve_entries(LogIndex(1), LogIndex(2)),
        Ok(vec![(Term(1), "One".to_string()),
                (Term(2), "Two".to_string())
        ]));
    // Check 2
    assert_eq!(state.retrieve_entry(LogIndex(2)),
        Ok((Term(2), "Two".to_string())));
    // Add 3, 4
    assert_eq!(state.append_entries(LogIndex(2), Term(2),
        vec![(Term(3), "Three".to_string()),
             (Term(4), "Four".to_string())]),
        Ok(()));
    // Check 3, 4
    assert_eq!(state.retrieve_entries(LogIndex(3), LogIndex(4)),
        Ok(vec![(Term(3), "Three".to_string()),
                (Term(4), "Four".to_string())
        ]));
    assert_eq!(state.get_last_index(), LogIndex(4));
    assert_eq!(state.get_last_term(), Term(4));
    // Remove 3, 4
    assert_eq!(state.purge_from_index(LogIndex(3)),
        Ok(()));
    assert_eq!(state.get_last_index(), LogIndex(2));
    assert_eq!(state.get_last_term(), Term(2));
    // Check 3, 4 are removed, and that code handles lack of entry gracefully.
    assert_eq!(state.retrieve_entries(LogIndex(0), LogIndex(4)),
        Ok(vec![(Term(1), "One".to_string()),
                (Term(2), "Two".to_string())
        ]));
    // Add 3, 4, 5
    assert_eq!(state.append_entries(LogIndex(2), Term(2),
        vec![(Term(3), "Three".to_string()),
             (Term(4), "Four".to_string()),
             (Term(5), "Five".to_string())]),
        Ok(()));
    assert_eq!(state.get_last_index(), LogIndex(5));
    assert_eq!(state.get_last_term(), Term(5));
    // Add 3, 4 again. (5 should be purged)
    assert_eq!(state.append_entries(LogIndex(2), Term(2),
        vec![(Term(3), "Three".to_string()),
             (Term(4), "Four".to_string())]),
        Ok(()));
    assert_eq!(state.retrieve_entries(LogIndex(0), LogIndex(4)),
        Ok(vec![(Term(1), "One".to_string()),
                (Term(2), "Two".to_string()),
                (Term(3), "Three".to_string()),
                (Term(4), "Four".to_string()),
        ]));
    assert_eq!(state.get_last_index(), LogIndex(4));
    assert_eq!(state.get_last_term(), Term(4));
    // Do a blank check.
    assert_eq!(state.append_entries(LogIndex(4), Term(4), vec![]),
        Ok(()));
    assert_eq!(state.get_last_index(), LogIndex(4));
    assert_eq!(state.get_last_term(), Term(4));
    fs::remove_file(&path.clone()).ok();
}
