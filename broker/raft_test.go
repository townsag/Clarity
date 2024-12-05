package broker

import (
	"log"
	"testing"
	"time"
)

func TestElectionLeaderDisconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, origTerm := h.CheckSingleLeader()

	h.DisconnectPeer(origLeaderId)
	sleepMs(350)

	newLeaderId, newTerm := h.CheckSingleLeader()
	if newLeaderId == origLeaderId {
		t.Errorf("want new leader to be different from orig leader")
	}
	if newTerm <= origTerm {
		t.Errorf("want newTerm <= origTerm, got %d and %d", newTerm, origTerm)
	}
}

func TestElection2LeaderDC(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()
	log.Printf("leader is %d", origLeaderId)

	h.DisconnectPeer(origLeaderId)
	otherId := (origLeaderId + 1) % 3
	h.DisconnectPeer(otherId)

	// No quorum.
	sleepMs(450)
	h.CheckNoLeader()

	// Reconnect one other server; now we'll have quorum.
	h.ReconnectPeer(otherId)
	h.CheckSingleLeader()
}

func TestDisconnectAllThenRestore(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	sleepMs(100)
	//	Disconnect all servers from the start. There will be no leader.
	for i := 0; i < 3; i++ {
		h.DisconnectPeer(i)
	}
	sleepMs(450)
	h.CheckNoLeader()

	// Reconnect all servers. A leader will be found.
	for i := 0; i < 3; i++ {
		h.ReconnectPeer(i)
	}
	h.CheckSingleLeader()
}

func TestElectionFollowerComesBack(t *testing.T) {

	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, origTerm := h.CheckSingleLeader()

	otherId := (origLeaderId + 1) % 3
	h.DisconnectPeer(otherId)
	time.Sleep(650 * time.Millisecond)
	h.ReconnectPeer(otherId)
	sleepMs(150)

	// We can't have an assertion on the new leader id here because it depends
	// on the relative election timeouts. We can assert that the term changed,
	// however, which implies that re-election has occurred.
	_, newTerm := h.CheckSingleLeader()
	if newTerm <= origTerm {
		t.Errorf("newTerm=%d, origTerm=%d", newTerm, origTerm)
	}
}

func TestCommitOneCommand(t *testing.T) {

	h := NewHarness(t, 5)
	//defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()

	tlog("submitting command {42} for document {testdoc} to %d", origLeaderId)
	isLeader := h.SubmitToServer(origLeaderId, "testdoc", 42) >= 0
	if !isLeader {
		t.Errorf("want id=%d leader, but it's not", origLeaderId)
	}

	sleepMs(500)
	//h.CheckCommittedN(42, 5)

	// log, commitIndex, logLen := h.GetLogAndCommitIndexFromServer(origLeaderId)
	// tlog("Leader %d CommitIndex: %d   log: %+v   idx of latest entry: %d ", origLeaderId, commitIndex, log, logLen-1)
	tlog("Leader is %d", origLeaderId)
	for serverId := 0; serverId < h.n; serverId++ {
		log, committedLog, commitIndex, logLen := h.GetLogsAndCommitIndexFromServer(serverId)
		tlog("Server %d CommitIndex: %d   log: %+v  committed: %+v  idx of latest entry: %d", serverId, commitIndex, log, committedLog, logLen-1)
	}
}

func TestCommitMultipleCommands(t *testing.T) {

	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()

	values := []int{42, 55, 81}
	docnames := []string{"doc1", "doc1", "doc2"}
	for i, v := range values {
		tlog("submitting {%d} for %d to %d", v, docnames[i], origLeaderId)
		isLeader := h.SubmitToServer(origLeaderId, docnames[i], v) >= 0
		if !isLeader {
			t.Errorf("want id=%d leader, but it's not", origLeaderId)
		}
		sleepMs(100)
	}

	sleepMs(250)
	// nc, i1 := h.CheckCommitted(42)
	// _, i2 := h.CheckCommitted(55)
	// if nc != 3 {
	// 	t.Errorf("want nc=3, got %d", nc)
	// }
	// if i1 >= i2 {
	// 	t.Errorf("want i1<i2, got i1=%d i2=%d", i1, i2)
	// }

	// _, i3 := h.CheckCommitted(81)
	// if i2 >= i3 {
	// 	t.Errorf("want i2<i3, got i2=%d i3=%d", i2, i3)
	// }

	// log, commitIndex, logLen := h.GetLogAndCommitIndexFromServer(origLeaderId)
	// tlog("Leader %d CommitIndex: %d   log: %+v   idx of latest entry: %d ", origLeaderId, commitIndex, log, logLen-1)

	tlog("Leader is %d", origLeaderId)
	for serverId := 0; serverId < h.n; serverId++ {
		log, committedLog, commitIndex, logLen := h.GetLogsAndCommitIndexFromServer(serverId)
		tlog("Server %d CommitIndex: %d   log: %+v  committed: %+v  idx of latest entry: %d", serverId, commitIndex, log, committedLog, logLen-1)
	}
}
