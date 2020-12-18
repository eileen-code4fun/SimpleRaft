package raft

// go test -v -run TestLog*
// go test -v raft_test.go raft.go

import (
  "math"
  "testing"
  "time"
)

func min(a, b int) int {
  return int(math.Min(float64(a), float64(b)))
}

func compareLogs(svr *server, expected []logEntry, t *testing.T) {
  t.Helper()
  if len(svr.logs) != len(expected) {
    t.Errorf("server %d has log size %d vs %d", svr.id, len(svr.logs), len(expected))
  }
  for i := 0; i < min(len(svr.logs), len(expected)); i ++ {
    if svr.logs[i].term != expected[i].term || svr.logs[i].val != expected[i].val {
      t.Errorf("server %d has log at %d; %v vs %v", svr.id, i, svr.logs[i], expected[i])
    }
  }
}

func verifyRoles(svrs map[int]*server, wantLeaderCount, wantFollowerCount, wantCandidateCount int, t *testing.T) {
  t.Helper()
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
  if leaderCount != wantLeaderCount || followerCount != wantFollowerCount || candidateCount != wantCandidateCount {
    t.Errorf("got leaderCount=%d, followerCount=%d, candidateCount=%d; want %d, %d, %d", leaderCount, followerCount, candidateCount, wantLeaderCount, wantFollowerCount, wantCandidateCount)
  }
}

func sendClientRequest(q chan request, r request, t *testing.T) {
  t.Helper()
  q <- r
  select {
  case r := <-r.getRespChan():
    if !r.success {
      t.Errorf("got failed response %v; want success", r)
    }
  case <- time.After(3 * time.Second):
    t.Error("time out waiting for response")
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
  verifyRoles(svrs, 1, 2, 0, t)
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
  sendClientRequest(svrs[1].incoming, clientRequest{val: 5, resp: make(chan response, 1)}, t)
  // Check log is replicated.
  expected := []logEntry{{term: 0, val: 5}}
  compareLogs(svrs[0], expected, t)
  compareLogs(svrs[1], expected, t)
  compareLogs(svrs[2], expected, t)
  // Restart a follower and see if it catches later.
  svrs[0].kill()
  // Send another client request.
  sendClientRequest(svrs[1].incoming, clientRequest{val: 6, resp: make(chan response, 1)}, t)
  sendClientRequest(svrs[1].incoming, clientRequest{val: 7, resp: make(chan response, 1)}, t)
  svrs[0].start(svrs, false)
  // Wait for the server to catch up.
  time.Sleep(2 * time.Second)
  expected = []logEntry{{term: 0, val: 5}, {term: 0, val: 6}, {term: 0, val: 7}}
  compareLogs(svrs[0], expected, t)
  compareLogs(svrs[1], expected, t)
  compareLogs(svrs[2], expected, t)
}

func TestReelectLeader(t *testing.T) {
  t.Parallel()
  svrs := map[int]*server{}
  for i := 0; i < 5; i ++ {
    svrs[i] = newServer(i)
  }
  // Set server 0 to be the leader.
  svrs[0].start(svrs, true)
  for i := 1; i < 5; i ++ {
    svrs[i].start(svrs, false)
  }
  // Wait briefly so that leader has a chance to contact followers.
  time.Sleep(time.Second)
  svrs[0].kill()
  time.Sleep(5 * time.Second)
  verifyRoles(svrs[0].peers, 1, 3, 0, t)
}

func TestResolveLeaderCompetition(t *testing.T) {
  t.Parallel()
  svrs := map[int]*server{}
  for i := 0; i < 3; i ++ {
    svrs[i] = newServer(i)
  }
  // Set server 0 to be the leader.
  svrs[0].start(svrs, true)
  svrs[1].start(svrs, false)
  svrs[2].start(svrs, false)
  time.Sleep(time.Second)
  // Kill the leader.
  svrs[0].kill()
  // Wait for new leader to be elected.
  time.Sleep(5 * time.Second)
  verifyRoles(svrs[0].peers, 1, 1, 0, t)
  // Old leader comes back.
  svrs[0].start(svrs, true)
  time.Sleep(time.Second)
  verifyRoles(svrs, 1, 2, 0, t)
}