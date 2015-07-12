# Raft #

> Note: This project is **incomplete** and the code is of **pre-alpha** quality. **Only some specific, basic functionality is available.** A stable version will be released when we feel it is ready.

[![Build Status](https://travis-ci.org/Hoverbear/raft.svg)](https://travis-ci.org/Hoverbear/raft)

**[Development Updates](http://www.hoverbear.org/tag/raft/)**

## Problem and Importance ##

When building a distributed system one principal goal is often to build in *fault-tolerance*. That is, if one particular node in a network goes down, or if there is a network partition, the entire cluster does not fall over. The cluster of nodes taking part in a distributed consensus protocol must come to agreement regarding values, and once that decision is reached, that choice is final.

Distributed Consensus Algorithms often take the form of a replicated state machine and log. Each state machine accepts inputs from its log, and represents the value(s) to be replicated, for example, a hash table. They allow a collection of machines to work as a coherent group that can survive the failures of some of its members.

Two well known Distributed Consensus Algorithms are Paxos and Raft. Paxos is used in systems like [Chubby](http://research.google.com/archive/chubby.html) by Google, and Raft is used in things like [`etcd`](https://github.com/coreos/etcd/tree/master/raft). Raft is generally seen as a more understandable and simpler to implement than Paxos, and was chosen for this project for this reason.

## Compiling ##

> For Linux, BSD, or Mac. Windows is not supported at this time.

You will need the [Rust](http://rust-lang.org/) compiler:

```bash
curl -L https://static.rust-lang.org/rustup.sh | sudo sh
```

This should install `cargo` and `rustc`. Next, you'll need `capnp` to build the
`messages.canpnp` file . It is suggested to use the [git method](https://capnproto.org/install.html#installation-unix)

```bash
git clone https://github.com/sandstorm-io/capnproto.git
cd capnproto/c++
./setup-autotools.sh
autoreconf -i
./configure
make -j6 check
sudo make install
```

Finally, clone the repository and build it:

```bash
git clone git@github.com:Hoverbear/raft.git && \
cd raft && \
cargo build
```

> Note this is a library, so building won't necessarily produce anything useful for you unless you're developing.

## Documentation ##

* [Raft Crate Documentation](https://hoverbear.github.io/raft/raft/)
* [The Raft site](https://raftconsensus.github.io/)
* [The Secret Lives of Data - Raft](http://thesecretlivesofdata.com/raft/)
* [Raft Paper](http://ramcloud.stanford.edu/raft.pdf)
* [Raft Dissertation](https://github.com/ongardie/dissertation#readme)

## Testing ##

You can run `raft`'s full bank of tests with all debug output like so:

```bash
RUST_LOG=raft=debug cargo test -- --nocapture
```

> Due to the nature of this library's pre-alpha state testing is not complete.

For something more terse use `cargo test`.

The `examples/dummy.rs` file currently hosts a working example of using the library. We'll add more as we go!
