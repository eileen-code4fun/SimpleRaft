package raft

// go test -v -run TestLog*

import (
  "testing"
  "time"
)

func compareLogs(a []logEntry, b []logEntry, t *testing.T) {
  if len(a) != len(b) {
    t.Fatalf("unequal log size %d vs %d", len(a), len(b))
  }
  for i, _ := range a {
    if a[i].term != b[i].term || a[i].val != b[i].val {
      t.Errorf("unequal log at %d; %v vs %v", i, a[i], b[i])
    }
  }
}

func TestInitialLeaderElection(t *testing.T) {
  t.Parallel()
  // Launch 3 servers and verify one of them becomes leader.
  svrs := map[int]*server{}
  for i := 0; i < 3; i ++ {
    svrs[i] = newServer(i)
  }
  for _, svr := range svrs {
    svr.start(svrs, false)
  }
  time.Sleep(3 * time.Second)
  var leaderCount, followerCount, candidateCount int
  for _, svr := range svrs {
    switch svr.role {
    case LEADER:
      leaderCount += 1
    case FOLLOWER:
      followerCount += 1
    case CANDIDATE:
      candidateCount += 1
    }
  }
  if leaderCount != 1 || followerCount != 2 || candidateCount != 0 {
    t.Errorf("got leaderCount=%d, followerCount=%d, candidateCount=%d; want 1, 2, 0", leaderCount, followerCount, candidateCount)
  }
}

func TestLogReplicated(t *testing.T) {
  t.Parallel()
  svrs := map[int]*server{}
  for i := 0; i < 3; i ++ {
    svrs[i] = newServer(i)
  }
  // Set server 1 to be the leader.
  svrs[0].start(svrs, false)
  svrs[1].start(svrs, true)
  svrs[2].start(svrs, false)
  // Send a client request to the leader.
  resp := make(chan response, 1)
  svrs[1].incoming <- clientRequest{val: 5, resp: resp}
  select {
  case r := <-resp:
    if !r.success {
      t.Errorf("got failed response %v; want success", r)
    }
  case <- time.After(3 * time.Second):
    t.Error("time out waiting for response")
  }
  // Check log is replicated.
  expected := []logEntry{{term: 0, val: 5}}
  compareLogs(svrs[0].logs, expected, t)
  compareLogs(svrs[1].logs, expected, t)
  compareLogs(svrs[2].logs, expected, t)
}
