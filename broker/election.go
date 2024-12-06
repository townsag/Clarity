package broker

import (
	"log"
	"math/rand"
	"time"
)

type ElectionModule struct {
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

	//////////////////////////////////////////////////
	// below didn't end up being implemented in time
	//////////////////////////////////////////////////
	// in the case a follower receives an http request
	// each broker keeps track of who the leader is and a list of peer addresses
	// so the follower can send an http reply back with the leader's address
	leaderId  int
	peerAddrs map[int]string
}

func NewEM(id int, peerIds []int, peerAddrs map[int]string, broker *BrokerServer, ready <-chan any) *ElectionModule {

	em := new(ElectionModule)

	em.broker = broker
	em.id = id
	em.peerIds = peerIds
	em.votedFor = -1

	em.nextIndex = make(map[int]int)
	em.matchIndex = make(map[int]int)

	em.leaderId = -1
	em.peerAddrs = peerAddrs

	// start election timeouts together
	go func() {
		<-ready
		em.resetElectionTimer()
	}()

	return em
}

func (em *ElectionModule) resetElectionTimer() {

	log.Printf("%d resets election timer", em.id)

	// stop timer if there is still time left
	if em.electionTimer != nil {
		em.electionTimer.Stop()
	}

	// set and start new timer
	//timeout := time.Duration(500+rand.Intn(150)) * time.Millisecond
	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	em.electionTimer = time.NewTimer(timeout)

	// start election when timer runs out
	go func() {

		<-em.electionTimer.C
		log.Printf("%d detected no heartbeat from leader, starting election", em.id)
		em.startElection()

	}()
}

func (em *ElectionModule) startElection() {
	log.Printf("%d starts election", em.id)

	em.broker.state = Candidate
	em.term++

	em.votedFor = em.id

	em.leaderId = -1

	currentTerm := em.term

	log.Printf("%d voted for %d for term %d", em.id, em.votedFor, em.term)

	// server votes for itself
	votes := 1

	// send vote request rpc to all peers
	for _, peerId := range em.peerIds {
		go func(peerId int) {

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

			log.Printf("%d sending RequestVote Call to %d: %+v", em.id, peerId, args)

			var reply RequestVoteReply
			if err := em.broker.Call(peerId, "ElectionModule.RequestVote", args, &reply); err == nil {
				em.broker.mu2.Lock()
				defer em.broker.mu2.Unlock()
				log.Printf("%d received RequestVoteReply %+v", em.id, reply)

				// state no longer candidate during election
				if em.broker.state != Candidate {
					log.Printf("while waiting for reply, state = %v", em.broker.state)
					return
				}

				// if reply has greater term, become follower and update own term
				if reply.Term > currentTerm {
					log.Printf("%d term out of date", em.id)
					em.becomeFollower(reply.Term)
					return
				} else if reply.Term == currentTerm { // if terms are equal
					// if vote is granted by replier, increment votes and check for majority
					if reply.VoteGranted {
						log.Printf("%s %d is granted vote from %d", em.broker.state, em.id, reply.Id)
						votes += 1
						if votes*2 > len(em.peerIds)+1 {
							//log.Printf("%d becomes leader", em.id)
							em.becomeLeader()
							return
						}
					}
				}

			} else {
				log.Printf("error with requestvote call %s", err)
			}

		}(peerId)
	}
	log.Printf("%d's election fails", em.id)
	go em.resetElectionTimer()

}

// set em to follower
func (em *ElectionModule) becomeFollower(term int) {
	log.Printf("%d becomes Follower with term:%d", em.id, term)

	em.broker.state = Follower

	em.term = term
	em.votedFor = -1
	em.leaderId = -1

	go em.resetElectionTimer()

}

// set em to leader and start its responsibilities
func (em *ElectionModule) becomeLeader() {

	em.broker.state = Leader
	em.leaderId = em.id

	// stop timer for leader election
	em.electionTimer.Stop()

	log.Printf("%d becomes leader", em.id)

	// structure to keep track of follower log indexes
	for _, peerId := range em.peerIds {
		em.nextIndex[peerId] = len(em.broker.rm.log)
		em.matchIndex[peerId] = -1
	}

	// send heartbeats by using leaderSendAEs in replication.go
	// heartbeats are just blank AppendEntries
	go func(heartbeatTimeout time.Duration) {
		log.Printf("%d sends heartbeats", em.id)
		em.broker.rm.leaderSendAEs()

		heartbeat := time.NewTimer(heartbeatTimeout)
		defer heartbeat.Stop()
		for {
			doSend := false
			select {
			case <-heartbeat.C:
				doSend = true

				heartbeat.Stop()
				heartbeat.Reset(heartbeatTimeout)
			case _, ok := <-em.broker.rm.triggerAEChan:
				if ok {
					doSend = true
				} else {
					return
				}

				if !heartbeat.Stop() {
					<-heartbeat.C
				}
				heartbeat.Reset(heartbeatTimeout)
			}

			// send another heartbeat
			if doSend {
				em.broker.mu2.Lock()
				if em.broker.state != Leader {
					em.broker.mu2.Unlock()
					return
				}
				em.broker.mu2.Unlock()
				em.broker.rm.leaderSendAEs()
			}
		}
	}(25 * time.Millisecond)
}

// //////////////////////////////////////////////////
// RPC funcs
// //////////////////////////////////////////////////

// there is a naming convention in Go. exported fields must be capitalized. fucking took me forever to realize
type RequestVoteArgs struct {
	Term        int
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	Id          int
}

// rpc func that handles incoming vote requests sent from startElection()
func (em *ElectionModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	log.Printf("%d recieves RequestVote from %d", em.id, args.CandidateId)

	em.broker.mu2.Lock()
	defer em.broker.mu2.Unlock()

	if em.broker.state == Dead {
		return nil
	}

	lastLogIndex, lastLogTerm := em.lastLogIndexAndTerm()

	// check vote request term with own term
	// if own term is lesser, become follower
	if args.Term > em.term {
		log.Printf("%d term out of data in RequestVote", em.id)
		em.becomeFollower(args.Term)
	}

	// if own term is equal, and em has not voted/already voted for requestor, and requestor logs are as
	// up to date as own logs. grant vote
	if em.term == args.Term && (em.votedFor == -1 || em.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {

		log.Printf("%d voteGranted = true for %d", em.id, args.CandidateId)
		reply.VoteGranted = true
		em.votedFor = args.CandidateId
		em.leaderId = args.CandidateId

		em.resetElectionTimer()
	} else {
		log.Printf("%d voteGranted = false for %d", em.id, args.CandidateId)
		reply.VoteGranted = false
	}

	reply.Term = em.term
	reply.Id = em.id

	log.Printf("%d replies RequestVote from %d. %+v", em.id, args.CandidateId, reply)

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

func (em *ElectionModule) GetLeaderAddr() string {
	em.broker.mu2.Lock()
	defer em.broker.mu2.Unlock()

	if leaderAddr, ok := em.peerAddrs[em.leaderId]; ok {
		return leaderAddr
	}
	return ""
}

////////////////////////////////////////////////////////////////////
//THESE FUNCS ARE FOR TESTING AND DEPLOYMENT
////////////////////////////////////////////////////////////////////

func (em *ElectionModule) Report() (id int, term int, idLeader bool) {
	em.broker.mu2.Lock()
	defer em.broker.mu2.Unlock()
	return em.id, em.term, em.broker.state == Leader
}
