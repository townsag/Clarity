// this basically copies from hashicorp githib
// want to seperate election from replication
// this file will be deleted at the end

package broker

import (
	"math/rand"
	"sync"
	"time"
)

// type ServerState int

// const (
// 	Follower  ServerState = 0
// 	Candidate ServerState = 1
// 	Leader    ServerState = 2
// 	Dead      ServerState = 3
// )

// converts CMState values into strings
// for debugging and terminal prints
// func (s ServerState) String() string {
// 	switch s {
// 	case Follower:
// 		return "Follower"
// 	case Candidate:
// 		return "Candidate"
// 	case Leader:
// 		return "Leader"
// 	case Dead:
// 		return "Dead"
// 	default:
// 		panic("unreachable")
// 	}
// }

// type CommitEntry struct {
// 	crdtOperation any

// 	Index int

// 	Term int
// }

type ConsensusModule struct {
	// lock
	mu sync.Mutex

	broker *BrokerServer

	// id of connected server
	id int

	// peers of connected server
	peerIds []int

	// long term storage in case of node failure  <-- need to implement storage.go
	//storage Storage

	commitChan chan<- CommitEntry

	// channel to coordiate commits
	newCommitReadyChan chan struct{}

	// AE stands for appendentry
	triggerAEChan chan struct{}

	// persistent state on all servers
	// should be replicated across all brokers
	term     int // what is the current term
	votedFor int // who this server voted for
	log      []LogEntry

	// volatile state. different for each server
	commitIndex   int //<-- find out what this does
	lastApplied   int //<-- find out what this does
	state         ServerState
	electionTimer *time.Timer

	// Volatile Raft state on leaders
	// nextIndex  map[int]int
	// matchIndex map[int]int

}

func NewCM(id int, peerIds []int, broker *BrokerServer, state ServerState /*storage Storage,*/, ready <-chan any, commitChan chan<- CommitEntry) *ConsensusModule {
	// initialize new cm
	cm := new(ConsensusModule)

	cm.id = id
	cm.peerIds = peerIds
	cm.broker = broker
	//cm.storage = storage

	cm.commitChan = commitChan

	// channels are like temporary storage that will be consumed by some function

	// 16 is buffer size. it means that 16 notifs can be held in channe;
	cm.newCommitReadyChan = make(chan struct{}, 16)

	// 1 ensures only 1 AppendEntry is pending
	cm.triggerAEChan = make(chan struct{}, 1)

	// initialize state. all but one will be Follower
	cm.state = state

	cm.votedFor = -1

	// v wtf does this shit do
	//cm.commitIndex = -1
	//cm.lastApplied = -1
	// cm.nextIndex = make(map[int]int)
	// cm.matchIndex = make(map[int]int)

	// in case of crash  but need to implment storage.go
	// if cm.storage.HasData() {
	// 	cm.restoreFromStorage()
	// }

	go func() {
		<-ready
		cm.mu.Lock()
		cm.resetElectionTimer()
		cm.mu.Unlock()
	}()

	go cm.commitChanSender()
	return cm

}

// set randomized timeout when called
// or start election when timeout runs out
func (cm *ConsensusModule) resetElectionTimer() {
	if cm.electionTimer != nil {
		cm.electionTimer.Stop()
	}

	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	cm.electionTimer = time.NewTimer(timeout)

	// start election when timer runs out
	go func() {
		<-cm.electionTimer.C
		cm.startElection()

	}()
}

// election algorithm
func (cm *ConsensusModule) startElection() {

	cm.mu.Lock()
	cm.state = Candidate
	cm.term++
	cm.votedFor = cm.id

	cm.mu.Unlock()

	// server votes for itself
	//votes := 1

	// send vote request rpc to all peers
	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			// need log index so need logs to work first
			return
		}(peerId)
	}

}

// // helper function for leader election
// // returns last log entry index and term
// func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
// 	if len(cm.log) > 0 {
// 		lastIndex := len(cm.log) - 1
// 		return lastIndex, cm.log[lastIndex].Term
// 	} else {
// 		// new server, no log
// 		return -1, -1
// 	}
// }

// watches newCommitReadyChan to see what entries can be commited
func (cm *ConsensusModule) commitChanSender() {
	for range cm.newCommitReadyChan {

	}
}
