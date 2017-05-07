use std::{error, fmt, fs, path, result};
use std::io::prelude::*;
use std::io::SeekFrom;

use byteorder::{ByteOrder, BigEndian, ReadBytesExt, WriteBytesExt};
use persistent_log::Log;
use LogIndex;
use ServerId;
use Term;

/// This is a `Log` implementation that stores entries in the filesystem
/// as well as in a struct. It is chiefly intended for testing.
///
/// # Panic
///
/// No bounds checking is performed and attempted access to non-existing log
/// indexes will panic.


/// Error type for FsLog

#[derive(Debug, PartialEq, Eq)]
pub struct Error;

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "An error occurred")
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        "An error occurred"
    }
}

impl ::std::convert::From<::std::io::Error> for Error {
    fn from(_err: ::std::io::Error) -> Error {
        Error
    }
}

pub type Result<T> = result::Result<T, Error>;
pub type Entry = (Term, Vec<u8>);

#[derive(Debug)]
pub struct FsLog {
    file: fs::File,
    current_term: Term,
    voted_for: Option<ServerId>,
    entries: Vec<(Term, Vec<u8>)>,
    offsets: Vec<u64>,
}

/// Stores log as 8 bytes for current_term, 8 bytes for voted_for, and 
/// As much as needed for the log.
impl FsLog {
    pub fn new(filename: &path::Path) -> Result<FsLog> {
        let mut f = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)?;

        assert_eq!(f.seek(SeekFrom::Current(0)).unwrap(), 0);
        let filelen = f.metadata()?.len();
        if filelen == 0 {
            f.write_u64::<BigEndian>(0)?;
            f.write_u64::<BigEndian>(<u64>::max_value())?;
            f.seek(SeekFrom::Start(0))?;
        }

        let current_term: Term = f.read_u64::<BigEndian>()?.into();

        let voted_for: Option<ServerId> = match f.read_u64::<BigEndian>()? {
            x if x == <u64>::max_value() => None,
            x => Some(x.into())
        };

        let mut log = FsLog {
            file: f,
            current_term: current_term,
            voted_for: voted_for,
            entries: Vec::new(),
            offsets: Vec::new(),
        };

        let mut offset = 16;
        while offset < filelen {
            log.offsets.push(offset);
            let entry = log.read_entry(None)?;
            log.entries.push(entry);
            offset = log.file.seek(SeekFrom::Current(0))?;
        }
        Ok(log)
    }

    fn write_term(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_u64::<BigEndian>(self.current_term.into())?;
        self.write_voted_for()?;
        Ok(())
    }
    
    fn write_voted_for(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(8))?;
        self.file.write_u64::<BigEndian>(
            match self.voted_for {
                None => <u64>::max_value(),
                Some(ServerId(n)) => n,
            }
        )?;
        Ok(())
    }

    fn read_entry(&mut self, index: Option<usize>) -> Result<Entry> {
        // Could be more efficient about not copying data here.
        if let Some(index) = index {
            let offset = self.offsets.get(index).ok_or(Error)?;
            self.file.seek(SeekFrom::Start(*offset))?;
        }
        let length = self.file.read_u64::<BigEndian>()? as usize;
        let mut vecbuf = vec![0u8; length - 8];
        self.file.read_exact(&mut vecbuf[0..(length - 8) as usize])?;
        let term = BigEndian::read_u64(&vecbuf[..8]).into();
        let command = (&vecbuf[8..]).to_owned();
        Ok((term, command))
    }

    fn truncate_file(&mut self, index: usize) -> Result<()> {
        match self.offsets.get(index) {
            None => {},
            Some(offset) => self.file.set_len(*offset)?,
        };
        Ok(())
    }

    ///Add an entry to the log
    fn write_entry(&mut self, index: usize, term: Term, command: &[u8]) -> Result<()> {
        if index > self.entries.len() {
            Err(Error)
        } else {
            let new_offset = self.file.seek(SeekFrom::End(0))?;
            self.offsets.push(new_offset);
            let entry_len = (command.len() + 16) as u64;
            self.file.write_u64::<BigEndian>(entry_len)?;
            self.file.write_u64::<BigEndian>(term.into())?;
            self.file.write_all(&command[..])?;
            Ok(())
        }
    }

    fn rewrite_entries(&mut self, from: LogIndex, entries: &[(Term, &[u8])]) -> Result<()> {
        assert!(self.latest_log_index().unwrap() + 1 >= from);
        let mut index = (from - 1).as_u64() as usize;
        self.truncate_file(index)?;
        self.entries.truncate(index);  
        self.offsets.truncate(index);  
        self.entries.extend(entries.iter().map(|&(term, command)| (term, command.to_vec())));
        for &(term, command) in entries {
            self.write_entry(index, term, command)?;
            index += 1;
        }
        Ok(())
    }
}


impl Log for FsLog {
    type Error = Error;

    fn current_term(&self) -> Result<Term> {
        Ok(self.current_term)
    }

    fn set_current_term(&mut self, term: Term) -> Result<()> {
        self.current_term = term;
        self.voted_for = None;
        self.write_term()?;
        Ok(())
    }

    fn inc_current_term(&mut self) -> Result<Term> {
        self.current_term = self.current_term + 1;
        self.voted_for = None;
        self.write_term()?;
        self.current_term()
    }

    fn voted_for(&self) -> Result<Option<ServerId>> {
        Ok(self.voted_for)
    }

    fn set_voted_for(&mut self, address: ServerId) -> Result<()> {
        self.voted_for = Some(address);
        self.write_voted_for()?;
        Ok(())
    }

    fn latest_log_index(&self) -> Result<LogIndex> {
        Ok(LogIndex(self.entries.len() as u64))
    }

    fn latest_log_term(&self) -> Result<Term> {
        let len = self.entries.len();
        if len == 0 {
            Ok(Term::from(0))
        } else {
            Ok(self.entries[len - 1].0)
        }
    }

    fn entry(&self, index: LogIndex) -> Result<(Term, &[u8])> {
        let (term, ref bytes) = self.entries[(index - 1).as_u64() as usize];
        Ok((term, bytes))
    }

    /// Append entries sent from the leader.  
    fn append_entries(&mut self,
                      from: LogIndex,
                      entries: &[(Term, &[u8])])
                      -> Result<()> {
        assert!(self.latest_log_index().unwrap() + 1 >= from);
        let from_idx = (from - 1).as_u64() as usize;
        for idx in 0..entries.len() {
            match self.entries.get(from_idx + idx).map(|entry| entry.0) {
                Some(term) => {
                    let sent_term = entries[idx].0; 
                    if term == sent_term {
                        continue;
                    } else {
                        self.rewrite_entries(from + idx as u64, &entries[idx..])?;
                        break;
                    }
                },
                None => {
                    self.rewrite_entries(from + idx as u64, &entries[idx..])?;
                    break;
                }
            };
        }
        Ok(())
    }
}


impl Clone for FsLog {
    fn clone(&self) -> FsLog {
        // Wish I didn't have to unwrap the filehandle...
        FsLog {
            file: self.file.try_clone().unwrap(),
            current_term: self.current_term,
            voted_for: self.voted_for,
            entries: self.entries.clone(),
            offsets: self.offsets.clone(),
        }
    }
}


#[cfg(test)]
mod test {
    use std::fs::remove_file;
    use std::path::Path;
    use super::*;
    use LogIndex;
    use ServerId;
    use Term;
    use persistent_log::Log;

    fn assert_entries_equal(store: &FsLog, expected: Vec<(Term, &[u8])>) {
        assert_eq!(LogIndex::from(expected.len() as u64), store.latest_log_index().unwrap());
        assert_eq!(expected[expected.len() - 1].0, store.latest_log_term().unwrap());
        for i in 0..expected.len() {
            assert_eq!(store.entry(LogIndex::from((i + 1) as u64)).unwrap(), expected[i]);
        }
    }

    #[test]
    fn test_current_term() {
        let filename = Path::new("/tmp/raft-store.1");
        remove_file(&filename).unwrap_or(());
        let mut store = FsLog::new(&filename).unwrap();
        assert_eq!(Term(0), store.current_term().unwrap());
        store.set_voted_for(ServerId::from(0)).unwrap();
        store.set_current_term(Term(42)).unwrap();
        assert_eq!(None, store.voted_for().unwrap());
        assert_eq!(Term(42), store.current_term().unwrap());
        store.inc_current_term().unwrap();
        assert_eq!(Term(43), store.current_term().unwrap());
        remove_file(&filename).unwrap();
    }

    #[test]
    fn test_voted_for() {
        let filename = Path::new("/tmp/raft-store.2");
        remove_file(&filename).unwrap_or(());
        let mut store = FsLog::new(&filename).unwrap();
        assert_eq!(None, store.voted_for().unwrap());
        let id = ServerId::from(0);
        store.set_voted_for(id).unwrap();
        assert_eq!(Some(id), store.voted_for().unwrap());
        remove_file(&filename).unwrap();
    }

    #[test]
    fn test_append_entries() {
        let filename = Path::new("/tmp/raft-store.3");
        remove_file(&filename).unwrap_or(());
        let mut store = FsLog::new(&filename).unwrap();
        assert_eq!(LogIndex::from(0), store.latest_log_index().unwrap());
        assert_eq!(Term::from(0), store.latest_log_term().unwrap());

        // [0.1, 0.2, 0.3, 1.4]  Initial log
        store.append_entries(LogIndex(1),
                             &[(Term::from(0), &[1]),
                               (Term::from(0), &[2]),
                               (Term::from(0), &[3]),
                               (Term::from(1), &[4])])
             .unwrap();
        assert_entries_equal(&store, vec![(Term::from(0), &*vec![1]),
                                          (Term::from(0), &*vec![2]),
                                          (Term::from(0), &*vec![3]),
                                          (Term::from(1), &*vec![4])]);

        // [0.1, 0.2, 0.3, 1.4]  Empty log, no modification
        store.append_entries(LogIndex::from(3), &[]).unwrap();
        assert_entries_equal(&store, vec![(Term::from(0), &*vec![1]),
                                          (Term::from(0), &*vec![2]),
                                          (Term::from(0), &*vec![3]),
                                          (Term::from(1), &*vec![4])]);

        // [0.1, 0.2, 0.3, 1.4]  All match, non-exhaustive
        store.append_entries(LogIndex::from(2), 
                             &[(Term::from(0), &[2]),
                               (Term::from(0), &[3])])
             .unwrap();
        assert_entries_equal(&store, vec![(Term::from(0), &[1u8]),
                                         (Term::from(0), &[2u8]),
                                         (Term::from(0), &[3u8]),
                                         (Term::from(1), &[4u8])]);

        // [0.1, 0.2, 2.5, 2.6]  One match, two new
        store.append_entries(LogIndex::from(2), 
                             &[(Term::from(0), &[2]),
                               (Term::from(2), &[5]),
                               (Term::from(2), &[6])])
             .unwrap();
        assert_entries_equal(&store, vec![(Term::from(0), &*vec![1]),
                                          (Term::from(0), &*vec![2u8]),
                                          (Term::from(2), &*vec![5u8]),
                                          (Term::from(2), &*vec![6u8])]);

        // [0.1, 0.2, 4.7, 5.8]  All new entries
        store.append_entries(LogIndex::from(3), &[(Term(4), &[7]), (Term(5), &[8])]).unwrap();
        assert_entries_equal(&store, vec![(Term::from(0), &*vec![1]),
                                          (Term::from(0), &*vec![2]),
                                          (Term::from(4), &*vec![7]),
                                          (Term::from(5), &*vec![8])]);
        remove_file(&filename).unwrap();
    }

    #[test]
    fn test_restore_log() {
        let filename = Path::new("/tmp/raft-store.4");
        remove_file(&filename).unwrap_or(());
        {
            let mut store = FsLog::new(&filename).unwrap();
            store.set_current_term(Term(42)).unwrap();
            store.set_voted_for(ServerId::from(4)).unwrap();
            store.append_entries(LogIndex(1),
                                &[(Term::from(0), &[1]),
                                (Term::from(0), &[2]),
                                (Term::from(0), &[3]),
                                (Term::from(1), &[4])])
                .unwrap();
        }

        // New store with the same backing file starts with the same state.
        let store = FsLog::new(&filename).unwrap();
        assert_eq!(store.voted_for().unwrap(), Some(ServerId::from(4)));
        assert_eq!(store.current_term().unwrap(), Term(42));
        assert_entries_equal(&store, vec![(Term::from(0), &[1]),
                                          (Term::from(0), &[2]),
                                          (Term::from(0), &[3]),
                                          (Term::from(1), &[4])]);
        assert_eq!(store.offsets, [16, 33, 50, 67]);
        remove_file(&filename).unwrap();
    }
}
