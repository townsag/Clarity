package broker

// move all election shit here later? idk
// basically all it is is the leader sends heartbeats to followers.
// if a follower doesn't detect a heartbeat by timeout, start leader election

// assumed to rely on a ReplicationModule (rm). can get log idx through something like  len(broker.rm.log)

import (
	"fmt"
	"math/rand"

	//"sync"
	"time"
)

// maybe move serverstate to broker_server.go idk

type ElectionModule struct {
	// lock
	//mu sync.Mutex // probably retire for the shared lock in brokerserver

	broker *BrokerServer

	// id of connected server
	id int

	// peers of connected server
	peerIds []int

	// persistent state on all servers
	// should be replicated across all brokers
	term     int // what is the current term
	votedFor int // who this server voted for

	electionTimer *time.Timer

	// map is like a python dict
	nextIndex  map[int]int
	matchIndex map[int]int
}

func NewEM(id int, peerIds []int, broker *BrokerServer, ready <-chan any) *ElectionModule {

	em := new(ElectionModule)

	em.broker = broker
	em.id = id
	em.peerIds = peerIds

	// start election timeouts together
	go func() {
		<-ready
		em.broker.mu2.Lock()
		em.resetElectionTimer()
		em.broker.mu2.Unlock()
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
	em.broker.mu2.Lock()
	em.broker.state = Candidate
	em.term++
	em.votedFor = em.id

	currentTerm := em.term
	em.broker.mu2.Unlock()

	// server votes for itself
	votes := 1

	// send vote request rpc to all peers
	for _, peerId := range em.peerIds {
		go func(peerId int) {
			// need log index so need logs to work first
			// but something like
			// lastlogindex, lastterm = lastLogIndexAndTerm()
			em.broker.mu2.Lock()
			lastLogIndex, lastLogTerm := em.lastLogIndexAndTerm()
			em.broker.mu2.Unlock()

			// build request args
			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  em.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			var reply RequestVoteReply
			if err := em.broker.Call(peerId, "ReplicationModule.RequestVote", args, &reply); err == nil {
				em.broker.mu2.Lock()
				defer em.broker.mu2.Unlock()

				if em.broker.state != Candidate {
					return
				}

				// if reply has greater term, become follower and update own term
				if reply.Term > currentTerm {
					em.becomeFollower(reply.Term)
					return
				} else if reply.Term == currentTerm { // if terms are equal
					// if vote is granted by replier, increment votes and check for majority
					if reply.voteGranted {
						votes += 1
						if votes*2 > len(em.peerIds)+1 {
							em.becomeLeader()
							return
						}
					}
				}

			}

			return
		}(peerId)
	}

}

// set em to follower
func (em *ElectionModule) becomeFollower(term int) {
	fmt.Printf("%d becomes Follower with term:%d", em.id, term)

	em.broker.state = Follower

	em.term = term
	em.votedFor = -1
	em.resetElectionTimer()

}

// set em to leader and start its responsibilities
func (em *ElectionModule) becomeLeader() {

	em.broker.state = Leader

	// structure to keep track of follower log indexes
	for _, peerId := range em.peerIds {
		em.nextIndex[peerId] = len(em.broker.rm.log)
		em.matchIndex[peerId] = -1
	}

	// send heartbeats by using leaderSendAEs in replication.go
	// heartbeets are just blank AppendEntries
	go func(heartbeatTimeout time.Duration) {

		em.broker.rm.leaderSendAEs()

	}(50 * time.Millisecond)

}

// //////////////////////////////////////////////////
// RPC funcs
// //////////////////////////////////////////////////
type RequestVoteArgs struct {
	Term        int
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	voteGranted bool
}

// rpc func that handles incoming vote requests sent from startElection()
func (em *ElectionModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	em.broker.mu2.Lock()
	defer em.broker.mu2.Unlock()

	lastLogIndex, lastLogTerm := em.lastLogIndexAndTerm()

	// check vote request term with own term
	// if own term is lesser, become follower
	if args.Term > em.term {
		em.becomeFollower(args.Term)
	}

	// if own term is equal, and em has not voted/already voted for requestor, and requetor logs are as
	// up to date as own logs. grant vote
	if em.term == args.Term && (em.votedFor == -1 || em.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {

		reply.voteGranted = true
		em.votedFor = args.CandidateId
		em.resetElectionTimer()
	} else {
		reply.voteGranted = false
	}

	reply.Term = em.term

	return nil
}

func (em *ElectionModule) lastLogIndexAndTerm() (int, int) {
	if len(em.broker.rm.log) > 0 {
		lastIndex := len(em.broker.rm.log) - 1
		return lastIndex, em.broker.rm.log[lastIndex].Term
	} else {
		return -1, -1
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

func (rm *ReplicationModule) test() {

}
