package broker

// basically if leader. rpc new log entries to followers and wait for responses the send commit
// if follower when getting an update, send ready message then wait for commit message

//"sync"

type CommitEntry struct {
	crdtOperation any

	Index int

	Term int
}

type LogEntry struct {
	crdtOperation any
	Term          int
}

type ReplicationModule struct {
	//mu sync.Mutex	// probably retire for the shared lock in brokerserver

	broker *BrokerServer

	// id of connected server
	id int

	peerIds []int

	log []LogEntry

	commitIndex int
}

func NewRM(id int, peerIds []int, broker *BrokerServer) *ReplicationModule {

	rm := new(ReplicationModule)

	rm.broker = broker
	rm.id = id
	rm.peerIds = peerIds
	rm.commitIndex = -1

	return rm
}

// main function for leader to send AppendEntry commands to followers
// also used in election.go for heartbeat
func (rm *ReplicationModule) leaderSendAEs() {
	rm.broker.mu2.Lock()

	// if broker is not leader. don't let it send AppendEntries
	if rm.broker.state != Leader {
		rm.broker.mu2.Unlock()
		return
	}

	currentTerm := rm.broker.em.term
	rm.broker.mu2.Unlock()

	for _, peerId := range rm.peerIds {

		// get the most recent index of the leader's log
		// replication for followers will start from there
		go func(peerId int) {
			rm.broker.mu2.Lock()
			nextIndex := rm.broker.em.nextIndex[peerId]

			prevLogIndex := nextIndex - 1
			prevLogTerm := -1

			if prevLogIndex >= 0 {
				prevLogTerm = rm.log[prevLogIndex].Term
			}
			entries := rm.log[nextIndex:]

			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     rm.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rm.commitIndex,
			}
			rm.broker.mu2.Unlock()

			var reply AppendEntriesReply
			if err := rm.broker.Call(peerId, "ReplicationModule.AppendEntries", args, &reply); err == nil {
				rm.broker.mu2.Lock()

				// if it detects through heartbeat that own term is out of data, become follower
				if reply.Term > rm.broker.em.term {
					rm.broker.em.becomeFollower(reply.Term)
					rm.broker.mu2.Unlock()
					return
				}

				// if broker is leader and it's term is up to date
				if rm.broker.state == Leader && currentTerm == reply.Term {
					if reply.Success {
						rm.broker.em.nextIndex[peerId] = nextIndex + len(entries)
						rm.broker.em.matchIndex[peerId] = rm.broker.em.nextIndex[peerId] - 1

						// get replies from followers to decide whether or not to send commit
						savedCommitIndex := rm.commitIndex
						for i := rm.commitIndex + 1; i < len(rm.log); i++ {
							if rm.log[i].Term == rm.broker.em.term {
								matches := 1
								for _, peerId := range rm.peerIds {
									if rm.broker.em.matchIndex[peerId] >= i {
										matches++
									}
								}
								// currently set to atomic. raft does majority
								// if matches*2 > len(rm.peerIds)+1
								if matches == len(rm.peerIds) {
									rm.commitIndex = i
								}
							}

						}
						// notify followers of commit
						if rm.commitIndex != savedCommitIndex {
							rm.broker.mu2.Unlock()
							// cm.newCommitReadyChan <- struct{}{}
							// cm.triggerAEChan <- struct{}{}
						} else {
							rm.broker.mu2.Unlock()
						}

					} else { // if reply.success = false
						if reply.ConflictTerm >= 0 {
							lastIndexOfTerm := -1
							for i := len(rm.log) - 1; i >= 0; i-- {
								if rm.log[i].Term == reply.ConflictTerm {
									lastIndexOfTerm = i
									break
								}
							}

							if lastIndexOfTerm >= 0 {
								rm.broker.em.nextIndex[peerId] = lastIndexOfTerm + 1
							} else {
								rm.broker.em.nextIndex[peerId] = reply.ConflictIndex
							}
						} else {
							rm.broker.em.nextIndex[peerId] = reply.ConflictIndex
						}

						rm.broker.mu2.Unlock()
					}

				} else {
					rm.broker.mu2.Unlock()
				}

			}

		}(peerId)
	}

}

// rpc request from leader to follower
// handles both heartbeat and actual log entries
type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// rpc reply from follower to leader
type AppendEntriesReply struct {
	Term    int
	Success bool

	// idk if these are necessary so remove if unused
	ConflictIndex int
	ConflictTerm  int
}

func (rm *ReplicationModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {

	return nil
}
