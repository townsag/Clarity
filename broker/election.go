package broker

// move all election shit here later? idk
// basically all it is is the leader sends heartbeats to followers.
// if a follower doesn't detect a heartbeat by timeout, start leader election

// assumed to rely on a ReplicationModule (rm). can get log ids through something like  broker.rm.logindex

import (
	"math/rand"
	"sync"
	"time"
)

type ServerState int

const (
	Follower  ServerState = 0
	Candidate ServerState = 1
	Leader    ServerState = 2
	Dead      ServerState = 3
)

func (s ServerState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("unreachable")
	}
}

type ElectionModule struct {
	// lock
	mu sync.Mutex

	broker *BrokerServer

	// id of connected server
	id int

	// peers of connected server
	peerIds []int

	// persistent state on all servers
	// should be replicated across all brokers
	term     int // what is the current term
	votedFor int // who this server voted for

	state         ServerState
	electionTimer *time.Timer
}

func newEM(id int, peerIds []int, broker *BrokerServer, state ServerState, ready <-chan any) *ElectionModule {

	em := new(ElectionModule)

	em.broker = broker
	em.id = id
	em.peerIds = peerIds
	em.state = state

	// start election timeouts together
	go func() {
		<-ready
		em.mu.Lock()
		em.resetElectionTimer()
		em.mu.Unlock()
	}()

	return em
}

func (em *ElectionModule) resetElectionTimer() {

	// stop timer if there is still time left
	if em.electionTimer != nil {
		em.electionTimer.Stop()
	}

	// set and start new timer
	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	em.electionTimer = time.NewTimer(timeout)

	// start election when timer runs out
	go func() {
		<-em.electionTimer.C
		em.startElection()

	}()
}

func (em *ElectionModule) startElection() {
	em.mu.Lock()
	em.state = Candidate
	em.term++
	em.votedFor = em.id
	em.mu.Unlock()

	// server votes for itself
	//votes := 1

	// send vote request rpc to all peers
	for _, peerId := range em.peerIds {
		go func(peerId int) {
			// need log index so need logs to work first
			// but something like
			// lastlogindex, lastterm = lastLogIndexAndTerm()
			return
		}(peerId)
	}

}

// get last log index and term from replication module for election
// func (em *ElectionModule) lastLogIndexAndTerm() (int, int) {
// 	if len(broker.rm.log) > 0 {
// 		lastIndex := len(broker.rm.log) - 1
// 		return lastIndex, broker.rm.log[lastIndex].Term
// 	} else {
// 		// new server, no log
// 		return -1, -1
// 	}
// }
