package raft

import (
  "math/rand"
  "testing"
  "time"
)

const (
  total = 10
)

func sendClientRequests(svrs map[int]*server, done chan struct{}) {
  for i := 0; i < total; i ++ {
    req := clientRequest{val: i, resp: make(chan response, 1)}
    var resp response
    for ;!resp.success; {
      // Retry until request success.
      if err := sendHelper(svrs[resp.leaderId].incoming, req); err != nil {
        // The server crashes, randoml pick another one.
        resp = response{leaderId: rand.Intn(len(svrs))}
      } else {
        resp = <- req.getRespChan()
      }
    }
  }
  done <- struct{}{}
  done <- struct{}{}
}

func TestEventualConsistency(t *testing.T) {
  svrs := map[int]*server{}
  for i := 0; i < 5; i ++ {
    svrs[i] = newServer(i)
  }
  for i := 0; i < 5; i ++ {
    svrs[i].start(svrs, false)
  }
  done := make(chan struct{}, 2)
  go sendClientRequests(svrs, done)
  // Randomly kill and restart servers
  go func() {
    for ;; {
      select {
      case <- done:
        return
      default:
        time.Sleep(500 * time.Millisecond)
        id := rand.Intn(len(svrs))
        t.Logf("killing server %d", id)
        svrs[id].kill()
        time.Sleep(1 * time.Second)
        t.Logf("restarting server %d", id)
        svrs[id].start(svrs, false)
      }
    }
  } ()
  <- done
  // Sleep long enough so that all servers can catch up.
  time.Sleep(time.Second + heartbeatTimeout * time.Duration(total))
  for id, svr := range svrs {
    if len(svr.logs) != total {
      t.Errorf("server %d has log size %d; want %d", id, len(svr.logs), total)
    }
    for i := 0; i < min(len(svr.logs), total); i ++ {
      if svr.logs[i].val != i {
        t.Errorf("server %d has log at %d: %d vs %d", id, i, svr.logs[i].val, i)
      }
    }
  }
  // Send one final client request to refresh the committed index.
  sendClientRequest(svrs[svrs[0].leaderId].incoming, clientRequest{val: total, resp: make(chan response, 1)}, t)
  time.Sleep(time.Second)
  for id, svr := range svrs {
    if svr.committedIndex != total {
      t.Errorf("server %d has commited index %d; want %d", id, svr.committedIndex, total)
    }
  }
}
