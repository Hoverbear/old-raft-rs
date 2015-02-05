# Raft #

> Note: This project is **incomplete** and the code is of **pre-alpha** quality. **Most functionality is currently `unimplemented!()` and will panic immediately.**

## Problem and Importance ##

When building a distributed system one principal goal is often to build in *fault-tolerance*. That is, if one particular node in a network goes down, or if there is a network partition, the entire cluster does not fall over. The cluster of nodes taking part in a distributed consensus protocol must come to agreement regarding values, and once that decision is reached, that choice is final.

Distributed Consensus Algorithms often take the form of a replicated state machine and log. Each state machine accepts inputs from its log, and represents the value(s) to be replicated, for example, a hash table. They allow a collection of machines to work as a coherent group that can survive the failures of some of its members.

Two well known Distributed Consensus Algorithms are Paxos and Raft. Paxos is used in systems like [Chubby](http://research.google.com/archive/chubby.html) by Google, and Raft is used in things like [`etcd`](https://github.com/coreos/etcd/tree/master/raft). Raft is generally seen as a more understandable and simpler to implement than Paxos, and was chosen for this project for this reason.

This project is appropriate for this class as it involves a number of peers distributed over a network with no consistent leader attempting to reliably replicate a log (and accompanying state machine). The vast majority of the code will be related to Remote Procedure Calls and the handling of network connections, particularly UDP based connections. There are a number of interesting failure modes to explore, including network partitions, failed hosts, and a variety of I/O errors.

## What's Been Done ##

There are numerous Raft implementations in a variety of languages, the most popular are in Go. Given this algorithm's foundational role in building distributed systems and  networks, it would be useful to have this algorithm easily available to users of the Rust programming language as well.

## Approach ##

I propose a simple implementation of Raft with Leader Election and Log Replication. If time permits, Membership Changes and Log Compaction will be included, however they are not required. The implementation language will be [Rust](http://rust-lang.org/) for a number of reasons: personal interest, strong typing, data safety, lack of garbage collector, and acceptable FFI interfaces.

My deliverables will be a functioning MIT licensed library with simple bindings, adequate test coverage, and example code.

## Schedule ##

* Jan 21-Feb 4: Basic structure and scaffold implementation.
* Feb 4-18: Refinement and materialization of protocol.
* Feb 18-Mar 4: Testing and example building.
* Mar 4-Onward: Release to community and improve based on feedback.

## Project Website ##

I'll keep track of my progress both on my blog through the [Raft](http://www.hoverbear.org/tag/raft/) tag, and via Github.

## Links ##

* [The Raft site](https://raftconsensus.github.io/)
* [The Secret Lives of Data - Raft](http://thesecretlivesofdata.com/raft/)
* [Raft Paper](http://ramcloud.stanford.edu/raft.pdf)
* [Raft Dissertation](https://github.com/ongardie/dissertation#readme)
