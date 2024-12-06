package broker

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"
)

// testharness basically taken from tutorial github and changed

func init() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}

type Harness struct {
	mu sync.Mutex

	// cluster holds the raftnodes
	cluster []*BrokerServer

	commitChans []chan CommitEntry

	commits [][]CommitEntry

	// to connect peers or disconnect peers (simulate partition)
	connected []bool

	// false to simulate crash
	alive []bool

	n int
	t *testing.T

	peerAddrs map[int]string
}

func NewHarness(t *testing.T, n int) *Harness {
	ns := make([]*BrokerServer, n)
	connected := make([]bool, n)
	alive := make([]bool, n)
	commitChans := make([]chan CommitEntry, n)
	commits := make([][]CommitEntry, n)
	ready := make(chan any)
	//storage := make([]*MapStorage, n)

	peerAddrs := make(map[int]string)
	for i := 0; i < n; i++ {
		peerAddrs[i] = fmt.Sprintf("127.0.0.1:%d", 8000+i)
	}

	for i := 0; i < n; i++ {
		peerIds := make([]int, 0)
		for p := 0; p < n; p++ {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}

		commitChans[i] = make(chan CommitEntry)
		ns[i] = NewBrokerServer(i, peerIds, peerAddrs, peerAddrs[i], Follower, ready, commitChans[i])
		ns[i].Serve()
		alive[i] = true

	}

	// connect peers to each other
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				ns[i].ConnectToPeer(j, ns[j].GetListenAddr())
			}
		}
		connected[i] = true
	}
	close(ready)

	h := &Harness{
		cluster: ns,
		//storage: storage,
		commitChans: commitChans,
		commits:     commits,
		connected:   connected,
		alive:       alive,
		n:           n,
		t:           t,
	}

	for i := 0; i < n; i++ {
		go h.collectCommits(i)
	}
	return h
}

func (h *Harness) Shutdown() {
	for i := 0; i < h.n; i++ {
		h.cluster[i].DisconnectAll()
		h.connected[i] = false
	}

	for i := 0; i < h.n; i++ {
		if h.alive[i] {
			h.alive[i] = false
			h.cluster[i].Shutdown()
		}
	}
	for i := 0; i < h.n; i++ {
		close(h.commitChans[i])
	}

}

func (h *Harness) DisconnectPeer(id int) {
	tlog("Disconnect %d", id)
	h.cluster[id].DisconnectAll()
	for j := 0; j < h.n; j++ {
		if j != id {
			h.cluster[j].DisconnectPeer(id)
		}
	}
	h.connected[id] = false
}

func (h *Harness) ReconnectPeer(id int) {
	tlog("Reconnect d", id)
	for j := 0; j < h.n; j++ {
		if j != id && h.alive[j] {
			if err := h.cluster[id].ConnectToPeer(j, h.cluster[j].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
			if err := h.cluster[j].ConnectToPeer(id, h.cluster[id].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
		}
	}
	h.connected[id] = true
}

// simulates crash by disconnecting and shutting down server
// the same server is not reconnected so will have empty logs
func (h *Harness) CrashPeer(id int) {
	tlog("Crash %d", id)
	h.DisconnectPeer(id)
	h.alive[id] = false
	h.cluster[id].Shutdown()

	h.mu.Lock()
	h.commits[id] = h.commits[id][:0]
	h.mu.Unlock()
}

func (h *Harness) RestartPeer(id int) {
	if h.alive[id] {
		log.Fatalf("id=%d is alive in RestartPeer", id)
	}
	tlog("Restart %d", id)

	peerIds := make([]int, 0)
	for p := 0; p < h.n; p++ {
		if p != id {
			peerIds = append(peerIds, p)
		}
	}

	ready := make(chan any)
	h.cluster[id] = NewBrokerServer(id, peerIds, h.peerAddrs, h.peerAddrs[id], Follower, ready, h.commitChans[id])
	h.cluster[id].Serve()
	h.ReconnectPeer(id)
	close(ready)
	h.alive[id] = true
	sleepMs(20)

}

func (h *Harness) CheckSingleLeader() (int, int) {
	retries := 10
	for r := 0; r < retries; r++ {
		leaderId := -1
		leaderTerm := -1
		for i := 0; i < h.n; i++ {
			if h.connected[i] {
				_, term, isLeader := h.cluster[i].em.Report()
				if isLeader {
					if leaderId < 0 {
						leaderId = i
						leaderTerm = term
					} else {
						h.t.Fatalf("both %d and %d think they're leaders", leaderId, i)
					}
				}
			}
		}
		if leaderId >= 0 {

			return leaderId, leaderTerm
		}
		time.Sleep(150 * time.Millisecond)
	}
	h.t.Fatalf("leader not found")
	return -1, -1
}

func (h *Harness) CheckNoLeader() {
	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			_, _, isLeader := h.cluster[i].em.Report()
			if isLeader {
				h.t.Fatalf("server %d leader; want none", i)
			}
		}
		log.Printf("No Leader Detected")
	}
}

func (h *Harness) CompareCommittedLogs() {
	h.t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()

	// Check that all connected servers have the same length of committedLogs
	commitsLen := -1
	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			if commitsLen >= 0 {
				// If the lengths don't match, fail the test
				if len(h.commits[i]) != commitsLen {
					h.t.Fatalf("commits[%d] has length %d, but commits[%d] has length %d", i, commitsLen, i, len(h.commits[i]))
				}
			} else {
				commitsLen = len(h.commits[i])
			}
		}
	}

	// Now compare the committedLog entries across all servers
	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			for c := 0; c < commitsLen; c++ {
				// Compare CRDTOperation and Index of each log entry
				operation := h.commits[i][c].CRDTOperation
				index := h.commits[i][c].Index

				for j := 0; j < h.n; j++ {
					if i != j && h.connected[j] {
						// If any entry differs between servers, log the error
						if h.commits[j][c].CRDTOperation != operation {
							h.t.Errorf("commits[%d][%d].CRDTOperation mismatch: got %v, want %v", j, c, h.commits[j][c].CRDTOperation, operation)
						}
						if h.commits[j][c].Index != index {
							h.t.Errorf("commits[%d][%d].Index mismatch: got %d, want %d", j, c, h.commits[j][c].Index, index)
						}
					}
				}
			}
		}
	}
}

func (h *Harness) CheckCommitted(cmd int) (nc int, index int) {
	h.t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()

	commitsLen := -1
	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			if commitsLen >= 0 {
				if len(h.commits[i]) != commitsLen {
					h.t.Fatalf("commits[%d] = %d, commitsLen = %d", i, h.commits[i], commitsLen)
				}
			} else {
				commitsLen = len(h.commits[i])
			}
		}
	}

	for c := 0; c < commitsLen; c++ {
		cmdAtC := -1
		for i := 0; i < h.n; i++ {
			if h.connected[i] {
				cmdOfN := h.commits[i][c].CRDTOperation.(int)
				if cmdAtC >= 0 {
					if cmdOfN != cmdAtC {
						h.t.Errorf("got %d, want %d at h.commits[%d][%d]", cmdOfN, cmdAtC, i, c)
					}
				} else {
					cmdAtC = cmdOfN
				}
			}
		}
		if cmdAtC == cmd {
			index := -1
			nc := 0
			for i := 0; i < h.n; i++ {
				if h.connected[i] {
					if index >= 0 && h.commits[i][c].Index != index {
						h.t.Errorf("got Index=%d, want %d at h.commits[%d][%d]", h.commits[i][c].Index, index, i, c)
					} else {
						index = h.commits[i][c].Index
					}
					nc++
				}
			}
			return nc, index
		}
	}
	h.t.Errorf("cmd=%d not found in commits", cmd)
	log.Printf("commits: %+v", h.commits)
	return -1, -1
}

func (h *Harness) SubmitToServer(serverId int, document string, cmd any) int {
	return h.cluster[serverId].rm.Submit(document, cmd)
}

func tlog(format string, a ...any) {
	format = "[TEST] " + format
	log.Printf(format, a...)
}

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}

func (h *Harness) collectCommits(i int) {
	for c := range h.commitChans[i] {
		h.mu.Lock()
		tlog("collectCommits(%d) got %+v", i, c)
		h.commits[i] = append(h.commits[i], c)
		h.mu.Unlock()
	}
}

func (h *Harness) GetLogsAndCommitIndexFromServer(serverId int) ([]LogEntry, []LogEntry, int, int) {
	server := h.cluster[serverId]
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.rm.log, server.rm.committedLog, server.rm.commitIndex, len(server.rm.log)
}

// expose broker server cluster to appserver
func (h *Harness) Cluster() []*BrokerServer {
	return h.cluster
}
