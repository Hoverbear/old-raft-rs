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

/// Persistent state
/// **Must be updated to stable storage before RPC response.**
pub struct PersistentState<T: Encodable + Decodable + Send + Clone> {
    pub current_term: u64,
    pub voted_for: Option<u64>, // request_vote cares if this is `None`
    log: File,
    pub last_index: u64,             // The last index of the file.
}

impl<T: Encodable + Decodable + Send + Clone> PersistentState<T> {
    pub fn new(current_term: u64, log_path: Path) -> PersistentState<T> {
        let mut open_opts = OpenOptions::new();
        open_opts.read(true);
        open_opts.write(true);
        open_opts.create(true);
        PersistentState {
            current_term: current_term,
            voted_for: None,
            log: open_opts.open(&log_path).unwrap(),
            last_index: 0,
        }
    }
    pub fn append_entries(&mut self, prev_log_index: u64, prev_log_term: u64,
                      entries: Vec<(u64, T)>) -> io::Result<()> {
        let position = self.move_to(prev_log_index + 1);
        // TODO: Possibly purge.
        for (term, entry) in entries {
            // TODO: I don't like the "doubling" here. How can we do this better?
            write!(&mut self.log, "{} {}\n", term, PersistentState::encode(entry));
        }
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
        let mut lines_read = 0u64;
        self.log.seek(io::SeekFrom::Start(0)); // Take the start.
        if line == 0 { return Ok(0) } // early exit
        // Go until we've reached `from` new lines.
        let _ = self.log.by_ref().chars().skip_while(|opt| {
            match *opt {
                Ok(val) => {
                    if val == '\n' {
                        lines_read += 1;
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
    fn purge_from_bytes(&mut self, from_bytes: u64) -> io::Result<()> {
        self.log.set_len(from_bytes) // Chop off the file at the given position.
    }
    /// Removes all entries from `from` to the last entry, inclusively.
    pub fn purge_from_index(&mut self, from_line: u64) -> io::Result<()> {
        let position = try!(self.move_to(from_line));
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
            let mut splits = chars.split(' ');
            let term = {
                let chunk = splits.next()
                    .and_then(|v| v.parse::<u64>().ok());
                match chunk {
                    Some(v) => v,
                    None => break,
                }
            };
            let encoded = {
                let chunk = splits.next();
                match chunk {
                    Some(v) => v,
                    None => break,
                }
            };
            let decoded: T = PersistentState::decode(encoded.to_string())
                .ok().expect("Could not unwrap log entry.");
            out.push((term, decoded));
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
        let mut splits = chars.split(' ');
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
    Follower,
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
    // Add 0, 1
    assert_eq!(state.append_entries(0, 0,
        vec![(1, "Foo".to_string()), (2, "Bar".to_string())]),
        Ok(()));
    // Check 0
    assert_eq!(state.retrieve_entry(0),
        Ok((1, "Foo".to_string())));
    // Check 0, 1
    assert_eq!(state.retrieve_entries(0, 1),
        Ok(vec![(1, "Foo".to_string()),
                (2, "Bar".to_string())
        ]));
    // Check 1
    assert_eq!(state.retrieve_entry(1),
        Ok((2, "Bar".to_string())));
    // Add 2, 3
    assert_eq!(state.append_entries(1, 2,
        vec![(2, "Baz".to_string()), (3, "FooBarBaz".to_string())]),
        Ok(()));
    // Check 2, 3
    assert_eq!(state.retrieve_entries(2, 3),
        Ok(vec![(2, "Baz".to_string()),
                (3, "FooBarBaz".to_string())
        ]));
    // Remove 2, 3
    assert_eq!(state.purge_from_index(2),
        Ok(()));
    // Check 3,4 are removed, and that code handles lack of entry gracefully.
    assert_eq!(state.retrieve_entries(0, 4),
        Ok(vec![(1, "Foo".to_string()),
                (2, "Bar".to_string())
        ]));
    // Add 3,4,5.
    fs::remove_file(&path.clone());
}
