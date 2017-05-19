#![feature(test)]
#![cfg(test)]

extern crate raft;
extern crate rand;
extern crate test;

use std::fs::remove_file;
use std::path::Path;

use rand::Rng;

use raft::persistent_log::FsLog;
use raft::{Log, LogIndex, Term};

#[bench]
fn bench_log_control(b: &mut test::Bencher) {
    let mut rng = rand::OsRng::new().unwrap();
    b.iter(|| {
        let i: u64 = rng.gen();
        let name = format!("/tmp/raft-rs-bench-log-control-{:016x}", i);
        let filename = Path::new(&name);
        let log = FsLog::new(&filename).unwrap();
        let x = log.latest_log_index();
        remove_file(&filename).expect("Could not remove file");
        x
    });
}

fn do_bench_append(b: &mut test::Bencher, name: &str, count: usize) {
    let mut rng = rand::OsRng::new().unwrap();
    let values: Vec<u8> = (0..255).collect();
    let mut entries = vec![];
    for x in 0..count {
        entries.push((Term::from(0x1234abcd8765fedc), &values[(x % 100)..(x % 100 + 100)]));
    }
    b.iter(|| {
        let i: u64 = rng.gen();
        let name = format!("/tmp/raft-rs-bench-log-{}-{:016x}", name, i);
        let filename = Path::new(&name);
        let mut log = FsLog::new(&filename).unwrap();
        log.append_entries(
            LogIndex::from(1), 
            &entries[..],
        ).expect("appending entries");
        let x = log.latest_log_index();
        remove_file(&filename).expect("Could not remove file");
        x
    });
}

fn do_bench_append_then_rewrite(b: &mut test::Bencher, name: &str, count: usize, rewrite: usize, from: LogIndex) {
    let mut rng = rand::OsRng::new().unwrap();
    let values: Vec<u8> = (0..100).collect();
    let mut initial_entries = vec![];
    let mut rewrite_entries = vec![];
    for x in 0..count {
        initial_entries.push((Term::from(0x12), &values[(x % 100)..(x % 100 + 1)]));
    }
    for x in 0..rewrite {
        rewrite_entries.push((Term::from(0x30af), &values[((rewrite - x) % 100)..((rewrite - x) % 100 + 1)]));
    }
    b.iter(|| {
        let i: u64 = rng.gen();
        let name = format!("/tmp/raft-rs-bench-log-{}-{:016x}", name, i);
        let filename = Path::new(&name);
        let mut log = FsLog::new(&filename).unwrap();
        log.append_entries(LogIndex::from(1), &initial_entries[..]).expect("append entries");
        log.append_entries(from, &rewrite_entries[..]).expect("rewrite entries");

        let x = log.latest_log_index();
        remove_file(&filename).expect("Could not remove file");
        x
    });

}

#[bench]
fn bench_log_append_0(b: &mut test::Bencher) {
    do_bench_append(b, "append0", 0);
}

#[bench]
fn bench_log_append_1(b: &mut test::Bencher) {
    do_bench_append(b, "append1", 1);
}


#[bench]
fn bench_log_append_10(b: &mut test::Bencher) {
    do_bench_append(b, "append10", 10);
}

#[bench]
fn bench_log_append_100(b: &mut test::Bencher) {
    do_bench_append(b, "append100", 100);
}
#[bench]
fn bench_log_append_1000(b: &mut test::Bencher) {
    do_bench_append(b, "append1000", 1000);
}

#[bench]
fn bench_log_rewrite_100_1(b: &mut test::Bencher) {
    do_bench_append_then_rewrite(b, "rewrite100.1", 100, 1, LogIndex::from(50));
}

#[bench]
fn bench_log_rewrite_100_50(b: &mut test::Bencher) {
    do_bench_append_then_rewrite(b, "rewrite100.50", 100, 50, LogIndex::from(50));
}

#[bench]
fn bench_log_rewrite_100_100(b: &mut test::Bencher) {
    do_bench_append_then_rewrite(b, "rewrite100.100", 100, 100, LogIndex::from(50));
}
