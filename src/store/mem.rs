use std::{error, fmt, result};
use std::net::SocketAddr;

use store::Store;

use LogIndex;
use Term;

/// This is a `Store` implementation that stores entries in a simple in-memory vector. Other data
/// is stored in a struct. It is chiefly intended for testing.
#[derive(Clone, Debug)]
pub struct MemStore {
    current_term: Term,
    voted_for: Option<SocketAddr>,
    entries: Vec<(Term, Vec<u8>)>,
}

/// Non-instantiable error type for MemStore
pub enum Error { }

impl fmt::Display for Error {
    fn fmt(&self, _fmt: &mut fmt::Formatter) -> fmt::Result {
        unreachable!()
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, _fmt: &mut fmt::Formatter) -> fmt::Result {
        unreachable!()
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        unreachable!()
    }
}

impl MemStore {

    pub fn new() -> MemStore {
        MemStore {
            current_term: Term(0),
            voted_for: None,
            entries: Vec::new(),
        }
    }
}

impl Store for MemStore {

    type Error = Error;

    fn current_term(&self) -> result::Result<Term, Error> {
        Ok(self.current_term)
    }

    fn set_current_term(&mut self, term: Term) -> result::Result<(), Error> {
        self.voted_for = None;
        Ok(self.current_term = term)
    }

    fn inc_current_term(&mut self) -> result::Result<Term, Error> {
        self.voted_for = None;
        self.current_term = self.current_term + 1;
        self.current_term()
    }

    fn voted_for(&self) -> result::Result<Option<SocketAddr>, Error> {
        Ok(self.voted_for)
    }

    fn set_voted_for(&mut self, address: SocketAddr) -> result::Result<(), Error> {
        Ok(self.voted_for = Some(address))
    }

    fn latest_log_index(&self) -> result::Result<LogIndex, Error> {
        Ok(LogIndex(self.entries.len() as u64))
    }

    fn latest_log_term(&self) -> result::Result<Term, Error> {
        let len = self.entries.len();
        if len == 0 {
            Ok(Term::from(0))
        } else {
            Ok(self.entries[len - 1].0)
        }
    }

    fn entry(&self, index: LogIndex) -> result::Result<(Term, &[u8]), Error> {
        let (term, ref bytes) = self.entries[Into::<u64>::into(index) as usize - 1];
        Ok((term, &bytes))
    }

    fn append_entries(&mut self,
                      from: LogIndex,
                      entries: &[(Term, &[u8])])
                      -> result::Result<(), Error> {
        assert!(self.latest_log_index().unwrap() + 1 >= from);
        self.entries.truncate(Into::<u64>::into(from) as usize - 1);
        Ok(self.entries.extend(entries.iter().map(|&(term, command)| (term, command.to_vec()))))
    }
}

#[cfg(test)]
mod test {

    use std::str::FromStr;
    use std::net::SocketAddr;

    use super::*;
    use LogIndex;
    use Term;
    use store::Store;

    #[test]
    fn test_current_term() {
        let mut store = MemStore::new();
        assert_eq!(Term(0), store.current_term().unwrap());
        store.set_voted_for(SocketAddr::from_str("127.0.0.1:0").unwrap()).unwrap();
        store.set_current_term(Term(42)).unwrap();
        assert_eq!(None, store.voted_for().unwrap());
        assert_eq!(Term(42), store.current_term().unwrap());
        store.inc_current_term().unwrap();
        assert_eq!(Term(43), store.current_term().unwrap());
    }

    #[test]
    fn test_voted_for() {
        let mut store = MemStore::new();
        assert_eq!(None, store.voted_for().unwrap());
        let addr = SocketAddr::from_str("127.0.0.1:0").unwrap();
        store.set_voted_for(addr.clone()).unwrap();
        assert_eq!(Some(addr), store.voted_for().unwrap());
    }

    #[test]
    fn test_append_entries() {
        let mut store = MemStore::new();
        assert_eq!(LogIndex::from(0), store.latest_log_index().unwrap());
        assert_eq!(Term::from(0), store.latest_log_term().unwrap());

        store.append_entries(LogIndex(1), &[(Term::from(0), &[1]),
                                            (Term::from(0), &[2]),
                                            (Term::from(0), &[3]),
                                            (Term::from(1), &[4])]).unwrap();
        assert_eq!(LogIndex::from(4), store.latest_log_index().unwrap());
        assert_eq!(Term::from(1), store.latest_log_term().unwrap());
        assert_eq!((Term::from(0), &*vec![1u8]), store.entry(LogIndex::from(1)).unwrap());
        assert_eq!((Term::from(0), &*vec![2u8]), store.entry(LogIndex::from(2)).unwrap());
        assert_eq!((Term::from(0), &*vec![3u8]), store.entry(LogIndex::from(3)).unwrap());
        assert_eq!((Term::from(1), &*vec![4u8]), store.entry(LogIndex::from(4)).unwrap());

        store.append_entries(LogIndex::from(4), &[]).unwrap();
        assert_eq!(LogIndex(3), store.latest_log_index().unwrap());
        assert_eq!(Term::from(0), store.latest_log_term().unwrap());
        assert_eq!((Term::from(0), &*vec![1u8]), store.entry(LogIndex::from(1)).unwrap());
        assert_eq!((Term::from(0), &*vec![2u8]), store.entry(LogIndex::from(2)).unwrap());
        assert_eq!((Term::from(0), &*vec![3u8]), store.entry(LogIndex::from(3)).unwrap());

        store.append_entries(LogIndex::from(3), &[(Term(2), &[3]), (Term(3), &[4])]).unwrap();
        assert_eq!(LogIndex(4), store.latest_log_index().unwrap());
        assert_eq!(Term::from(3), store.latest_log_term().unwrap());
        assert_eq!((Term::from(0), &*vec![1u8]), store.entry(LogIndex::from(1)).unwrap());
        assert_eq!((Term::from(0), &*vec![2u8]), store.entry(LogIndex::from(2)).unwrap());
        assert_eq!((Term::from(2), &*vec![3u8]), store.entry(LogIndex::from(3)).unwrap());
        assert_eq!((Term::from(3), &*vec![4u8]), store.entry(LogIndex::from(4)).unwrap());
    }
}
