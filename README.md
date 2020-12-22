# SimpleRaft
This project implements a demo Raft consensus protocol. It includes all the core functions of Raft: log replication, leader election, cluster membership change. It has thorough test coverage:
```
# Run all the unit tests.
go test -v raft_test.go raft.go
# Run the simulation test.
go test -v -run TestEventualConsistency 
```
The tests may occasionally fail. That's because there is inherent randomness in Raft protocol and the tests may not have waited long enough to see the final results. Simply rerun or adjust the individual sleep timeout in the tests. That should yield a successful result.

A more elaborated [blog post](https://eileen-code4fun.medium.com/raft-consensus-protocol-implementations-2b487320a5fc) is written to explain the internal working of this demo implementation.
