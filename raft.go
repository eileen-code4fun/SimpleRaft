// Package raft is a simplified implementation of the Raft consensus protocol.
package raft

import (
  "fmt"
  "log"
  "math/rand"
  "sync"
  "time"
)

const (
  LEADER = iota + 1
  CANDIDATE
  FOLLOWER

  // A big enough buffer size to avoid blocking on sending the RPC.
  defaultBufferSize = 1000
  rpcTimeout = 100 * time.Millisecond
  heartbeatTimeout = 200 * time.Millisecond
  electionTimeout = 1 * time.Second
  minElectionTimeoutMultiplier = 1
  maxElectionTimeoutMultiplier = 5
)

type request interface {
  getRespChan() chan response
}

type response struct {
  leaderId int // only used to redirect client request.
  term int
  success bool
  err error
}

type clientRequest struct {
  val int
  resp chan response
}

func (r clientRequest) getRespChan() chan response {
  return r.resp
}

type appendEntryRequest struct {
  request
  term int
  leaderId int
  prevLogIndex int
  prevLogTerm int
  entry *logEntry
  resp chan response
}

func (r appendEntryRequest) getRespChan() chan response {
  return r.resp
}

type requestVoteRequest struct {
  term int
  candidateId int
  logSize int
  lastLogTerm int
  resp chan response
}

func (r requestVoteRequest) getRespChan() chan response {
  return r.resp
}

type logEntry struct {
  term, val int
}

// server struct represents a server in Raft protocol.
// It could be a leader, follower, or candidate.
type server struct {
  id int
  peers map[int]*server
  mu sync.Mutex
  running bool
  done chan struct{}

  // Persistent state that won't be reset when killed/restart.
  currentTerm int
  votedFor map[int]int // term -> voted for candidateId.
  logs []logEntry

  // Volatile state that will be reset when killed/restart.
  leaderId int
  nextIndex map[int]int // The next log entry to send to peers.
  role int

  // RPC channel.
  incoming chan request
}

func (s *server) isRunning() bool {
  s.mu.Lock()
  defer s.mu.Unlock()
  return s.running
}

func newServer(id int) *server {
  return &server{
    id: id,
    votedFor: map[int]int{},
    done: make(chan struct{}, 1),
  }
}

// kill shuts down the server.
// This can be used to simulate crash.
func (s *server) kill() {
  s.mu.Lock()
  if s.running {
    close(s.incoming)
    s.running = false
    defer func() {
      <-s.done
    } ()
  }
  s.mu.Unlock()
}

// start launches the server.
// This resets the volatile state.
func (s *server) start(peers map[int]*server, forceLeader bool) error {
  s.mu.Lock()
  defer s.mu.Unlock()
  if s.running {
    return fmt.Errorf("server is already running; call kill() first")
  }
  s.incoming = make(chan request, defaultBufferSize)
  s.running = true
  if forceLeader {
    s.role = LEADER
  } else {
    s.role = FOLLOWER
  }
  s.peers = map[int]*server{}
  s.nextIndex = map[int]int{}
  for id, svr := range peers {
    if s.role == LEADER {
      // Initialize to be the current last log entry + 1.
      s.nextIndex[id] = len(s.logs)
    }
    if id == s.id {
      continue
    }
    s.peers[id] = svr
  }
  go s.run()
  return nil
}

func newElectionCountdown() time.Duration {
  return electionTimeout * time.Duration(rand.Intn(maxElectionTimeoutMultiplier - minElectionTimeoutMultiplier) + minElectionTimeoutMultiplier)
}

func (s *server) run() {
  electionCountdown := newElectionCountdown()
  for ;s.isRunning(); {
    select {
    case r := <-s.incoming:
      s.handleRequest(r)
    case <-time.After(heartbeatTimeout):
      switch s.role {
      case FOLLOWER:
        // No request from leader. Count down election.
        electionCountdown -= heartbeatTimeout
        if electionCountdown < 0 {
          // Start election.
          s.role = CANDIDATE
        }
      case LEADER:
        // No request from client. Need to send heartbeat.
        s.broadcastEntry()
      case CANDIDATE:
        if electionCountdown < 0 {
          // Clear election countdown.
          electionCountdown = newElectionCountdown()
          // Run election.
          if s.runElection() {
            // Become leader.
            s.role = LEADER
            s.leaderId = s.id
            log.Printf("server %d becomes leader", s.id)
            for id, _ := range s.peers {
              // Initialize to be the current last log entry + 1.
              s.nextIndex[id] = len(s.logs)
            }
          }
        }
      }
    }
  }
  s.done <- struct{}{}
}

func sendHelper(q chan request, r request) (err error) {
  defer func() {
    // In case the queue is closed.
    if x := recover(); x != nil {
      err = fmt.Errorf("failed to send the request")
    }
  } ()
  q <- r
  err = nil
  return
}

func (s *server) send(reqs map[int]request) map[int]response {
  var mu sync.Mutex
  resps := map[int]response{}
  var wg sync.WaitGroup
  for id, req := range reqs {
    sid := id // record peer ID.
    sreq := req // record the request.
    wg.Add(1)
    go func() {
      defer wg.Done()
      var resp response
      if err := sendHelper(s.peers[sid].incoming, sreq); err != nil {
        resp = response{err: err}
      } else {
        select {
        case resp = <- sreq.getRespChan():
        case <-time.After(rpcTimeout):
          resp = response{err: fmt.Errorf("no response from server %d", sid)}
        }
      }
      mu.Lock()
      defer mu.Unlock()
      resps[sid] = resp
    } ()
  }
  wg.Wait()
  return resps
}

func (s *server) handleRequest(req request) {
  switch r := req.(type) {
  case clientRequest:
    switch s.role {
    case FOLLOWER:
      // redirect client request.
      r.resp <- response{leaderId: s.leaderId}
    case CANDIDATE:
      // No leader yet, returns error to client.
      r.resp <- response{err: fmt.Errorf("no leader yet; please try later")}
    case LEADER:
      s.logs = append(s.logs, logEntry{term: s.currentTerm, val: r.val})
      if s.broadcastEntry() {
        r.resp <- response{term: s.currentTerm, success: true}
      } else {
        r.resp <- response{term: s.currentTerm}
      }
    }
  case appendEntryRequest:
    if r.term < s.currentTerm {
      r.resp <- response{term: s.currentTerm}
      return
    } else if r.term >= s.currentTerm {
      s.role = FOLLOWER
      s.currentTerm = r.term
    }
    s.leaderId = r.leaderId
    // Check if preceding entry matches.
    if r.prevLogIndex == -1 || r.prevLogIndex < len(s.logs) && s.logs[r.prevLogIndex].term == r.prevLogTerm {
      if len(s.logs) > 0 {
        s.logs = s.logs[0:r.prevLogIndex+1]
      }
      if r.entry != nil {
        s.logs = append(s.logs, *r.entry)
      }
      r.resp <- response{term: s.currentTerm, success: true}
    } else {
      r.resp <- response{term: s.currentTerm}
    }
  case requestVoteRequest:
    if r.term < s.currentTerm {
      r.resp <- response{term: s.currentTerm}
      return
    } else if r.term > s.currentTerm {
      s.role = FOLLOWER
      s.currentTerm = r.term
    }
    if votedFor, has := s.votedFor[s.currentTerm]; !has || votedFor == r.candidateId {
      var vote bool
      // Compare logs.
      var ownLastTerm int
      if len(s.logs) > 0 {
        ownLastTerm = s.logs[len(s.logs)-1].term
      }
      if ownLastTerm < r.lastLogTerm {
        vote = true
      }
      if ownLastTerm == r.lastLogTerm && len(s.logs) <= r.logSize {
        vote = true
      }
      if vote {
        s.votedFor[s.currentTerm] = r.candidateId
        r.resp <- response{term: s.currentTerm, success: true}
      } else {
        r.resp <- response{term: s.currentTerm}
      }
    } else {
      // Already voted for somebody else in this term.
      r.resp <- response{term: s.currentTerm}
    }
  }
}

// broadcastEntry sends one entry to each follower based on next index.
// The function returns true if majority of the followers respond success.
func (s *server) broadcastEntry() bool {
  log.Printf("server %d sending broadcast with logs: %v; nextIndex: %v", s.id, s.logs, s.nextIndex)
  reqs := map[int]request{}
  for id, _ := range s.peers {
    req := appendEntryRequest{
      term : s.currentTerm,
      leaderId : s.id,
      prevLogIndex : s.nextIndex[id] - 1,
      resp : make(chan response, 1),
    }
    if s.nextIndex[id] < len(s.logs) {
      // next index could be equal to current log size in which case this is a heartbeat.
      req.entry = &s.logs[s.nextIndex[id]]
    }
    if req.prevLogIndex >= 0 {
      req.prevLogTerm = s.logs[req.prevLogIndex].term
    }
    reqs[id] = req
  }
  resps := s.send(reqs)
  // Go over results
  successCount := 1 // count itself.
  for id, resp := range resps {
    if resp.err != nil {
      // Skip this time and retry later.
      log.Printf("failed to receive response from server %d", id)
      continue
    }
    if resp.term > s.currentTerm {
      // Convert to follower.
      s.currentTerm = resp.term
      s.role = FOLLOWER
      return false
    }
    if resp.success {
      successCount += 1
      // Advance next index for this follower.
      if s.nextIndex[id] < len(s.logs) {
        s.nextIndex[id] += 1
      }
    } else {
      // Backtrack next index for this follower.
      s.nextIndex[id] -= 1
    }
  }
  return successCount >= len(s.peers) / 2 + 1
}

// runElection runs for leader.
// The function returns true if successfully elected.
func (s *server) runElection() bool {
  s.currentTerm += 1
  s.votedFor[s.currentTerm] = s.id
  log.Printf("server %d starts election for term %d", s.id, s.currentTerm)
  reqs := map[int]request{}
  for id, _ := range s.peers {
    req := requestVoteRequest{
      term : s.currentTerm,
      candidateId : s.id,
      logSize : len(s.logs),
      resp : make(chan response, 1),
    }
    if req.logSize > 0 {
      req.lastLogTerm = s.logs[req.logSize - 1].term
    }
    reqs[id] = req
  }
  resps := s.send(reqs)
  // Go over results
  successCount := 1 // count itself.
  for id, resp := range resps {
    if resp.err != nil {
      // Skip this time and retry later.
      log.Printf("failed to receive response from server %d", id)
      continue
    }
    if resp.term > s.currentTerm {
      // Convert to follower.
      s.currentTerm = resp.term
      s.role = FOLLOWER
      return false
    }
    if resp.success {
      successCount += 1
    }
  }
  return successCount >= len(s.peers) / 2 + 1
}
