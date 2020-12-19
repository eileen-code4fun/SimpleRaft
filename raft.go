// Package raft is a simplified implementation of the Raft consensus protocol.
package raft

import (
  "fmt"
  "log"
  "math/rand"
  "sort"
  "sync"
  "time"
)

const (
  LEADER = iota + 1
  CANDIDATE
  FOLLOWER

  ADD = 1
  REMOVE = -1

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
  committedIndex int
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

type addServerRequest struct {
  serverAddr *server
  resp chan response
}

func (r addServerRequest) getRespChan() chan response {
  return r.resp
}

type removeServerRequest struct {
  serverAddr *server
  resp chan response
}

func (r removeServerRequest) getRespChan() chan response {
  return r.resp
}

type logEntry struct {
  term, val int
  serverAddr *server
}

// server struct represents a server in Raft protocol.
// It could be a leader, follower, or candidate.
type server struct {
  id int
  initialPeers map[int]*server
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
  committedIndex int

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
  s.initialPeers = map[int]*server{}
  s.nextIndex = map[int]int{}
  for id, svr := range peers {
    if s.role == LEADER && s.id != id {
      // Initialize to be the current last log entry + 1.
      s.nextIndex[id] = len(s.logs)
    }
    if id == s.id {
      continue
    }
    s.initialPeers[id] = svr
  }
  s.committedIndex = -1
  go s.run()
  return nil
}

// getCurrentPeers returns the current set of peers.
// Peers may change from initialPeers due to addServer or removeServer RPCs.
func (s *server) getCurrentPeers() map[int]*server {
  ret := map[int]*server{}
  for id, svr := range s.initialPeers {
    ret[id] = svr
  }
  for _, e := range s.logs {
    if e.serverAddr != nil {
      if e.val == ADD && e.serverAddr.id != s.id {
        ret[e.serverAddr.id] = e.serverAddr
      } else {
        delete(ret, e.serverAddr.id)
      }
    }
  }
  return ret
}

func newElectionCountdown() time.Duration {
  return electionTimeout * time.Duration(rand.Intn(maxElectionTimeoutMultiplier - minElectionTimeoutMultiplier) + minElectionTimeoutMultiplier)
}

func (s *server) shouldStepDown() bool {
  if s.role != LEADER {
    return false
  }
  for i := len(s.logs) - 1; i >= 0; i -- {
    if s.logs[i].serverAddr != s {
      continue
    }
    if s.logs[i].val == ADD {
      // It's an ADD.
      return false
    }
    // It's remove.
    // Leader only steps down then the entry is committed.
    if s.committedIndex >= i {
      return true
    }
    break
  }
  return false
}

func (s *server) run() {
  electionCountdown := newElectionCountdown()
  var cummulativeHeartbeatTimeout time.Duration
  for ;s.isRunning(); {
    if s.shouldStepDown() {
      s.mu.Lock()
      s.running = false
      close(s.incoming)
      s.mu.Unlock()
      break
    }
    select {
    case r := <-s.incoming:
      s.handleRequest(r, &cummulativeHeartbeatTimeout)
    case <-time.After(heartbeatTimeout):
      switch s.role {
      case FOLLOWER:
        // No request from leader. Count down election.
        cummulativeHeartbeatTimeout += heartbeatTimeout
        if electionCountdown < cummulativeHeartbeatTimeout {
          // Start election.
          s.role = CANDIDATE
        }
      case LEADER:
        // No request from client. Need to send heartbeat.
        s.broadcastEntry()
      case CANDIDATE:
        if electionCountdown < cummulativeHeartbeatTimeout {
          // Clear election countdown.
          electionCountdown = newElectionCountdown()
          cummulativeHeartbeatTimeout = 0
          // Run election.
          if s.runElection() {
            // Become leader.
            s.role = LEADER
            s.leaderId = s.id
            log.Printf("server %d becomes leader", s.id)
            for id, _ := range s.getCurrentPeers() {
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
      if err := sendHelper(s.getCurrentPeers()[sid].incoming, sreq); err != nil {
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

func (s *server) handleRequestHelper(resp chan response, potentialLogEntry logEntry) {
  switch s.role {
  case FOLLOWER:
    // redirect client request.
    resp <- response{leaderId: s.leaderId}
  case CANDIDATE:
    // No leader yet, returns error to client.
    resp <- response{err: fmt.Errorf("no leader yet; please try later")}
  case LEADER:
    s.logs = append(s.logs, potentialLogEntry)
    if s.broadcastEntry() {
      resp <- response{term: s.currentTerm, success: true}
    } else {
      resp <- response{term: s.currentTerm}
    }
  }
}

func (s *server) handleRequest(req request, cummulativeHeartbeatTimeout *time.Duration) {
  switch r := req.(type) {
  case addServerRequest:
    s.handleRequestHelper(r.resp, logEntry{term: s.currentTerm, val: ADD, serverAddr: r.serverAddr})
  case removeServerRequest:
    s.handleRequestHelper(r.resp, logEntry{term: s.currentTerm, val: REMOVE, serverAddr: r.serverAddr})
  case clientRequest:
    s.handleRequestHelper(r.resp, logEntry{term: s.currentTerm, val: r.val})
  case appendEntryRequest:
    if r.term < s.currentTerm {
      r.resp <- response{term: s.currentTerm}
      return
    } else if r.term >= s.currentTerm {
      s.role = FOLLOWER
      s.currentTerm = r.term
    }
    *cummulativeHeartbeatTimeout = 0
    s.leaderId = r.leaderId
    s.committedIndex = r.committedIndex
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
    if r.term < s.currentTerm || *cummulativeHeartbeatTimeout < minElectionTimeoutMultiplier * electionTimeout {
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
  for id, _ := range s.getCurrentPeers() {
    req := appendEntryRequest{
      term : s.currentTerm,
      leaderId : s.id,
      prevLogIndex : s.nextIndex[id] - 1,
      committedIndex: s.committedIndex,
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
  var successFromCurrentTerm []int
  for id, resp := range resps {
    if resp.err != nil {
      // Skip this time and retry later.
      log.Printf("server %d failed to receive broadcast response from server %d", s.id, id)
      continue
    }
    if resp.term > s.currentTerm {
      // Convert to follower.
      log.Printf("server %d current term %d detects broadcast term conflict %d", s.id, s.currentTerm, resp.term)
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
      if s.nextIndex[id] > 0 && s.logs[s.nextIndex[id] - 1].term == s.currentTerm {
        successFromCurrentTerm = append(successFromCurrentTerm, s.nextIndex[id] - 1)
      }
    } else {
      // Backtrack next index for this follower.
      s.nextIndex[id] -= 1
    }
  }
  // Update committed index.
  if len(successFromCurrentTerm) >= len(s.getCurrentPeers()) / 2 && len(successFromCurrentTerm) > 0 {
    sort.Ints(successFromCurrentTerm)
    // Committed index is the largest index that has received majority ack for the current term.
    s.committedIndex = successFromCurrentTerm[len(successFromCurrentTerm) - len(s.getCurrentPeers()) / 2]
  }
  return successCount >= len(s.getCurrentPeers()) / 2 + 1
}

// runElection runs for leader.
// The function returns true if successfully elected.
func (s *server) runElection() bool {
  s.currentTerm += 1
  s.votedFor[s.currentTerm] = s.id
  log.Printf("server %d starts election for term %d", s.id, s.currentTerm)
  reqs := map[int]request{}
  for id, _ := range s.getCurrentPeers() {
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
      log.Printf("server %d failed to receive vote response from server %d", s.id, id)
      continue
    }
    if resp.term > s.currentTerm {
      // Convert to follower.
      log.Printf("server %d current term %d detects election term conflict %d", s.id, s.currentTerm, resp.term)
      s.currentTerm = resp.term
      s.role = FOLLOWER
      return false
    }
    if resp.success {
      successCount += 1
    }
  }
  return successCount >= len(s.getCurrentPeers()) / 2 + 1
}
