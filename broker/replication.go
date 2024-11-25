package broker

// basically if leader. rpc new log entries to followers and wait for responses the send commit
// if follower when getting an update, send ready message then wait for commit message

import (
	"sync"
)

type CommitEntry struct {
	crdtOperation any

	Index int

	Term int
}

type LogEntry struct {
	crdtOperation any
	Term          any
}

type ReplicationModule struct {
	mu sync.Mutex

	broker *BrokerServer

	log []LogEntry
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
