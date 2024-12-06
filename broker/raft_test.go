package broker

import (
	"log"
	"testing"
	"time"
)

func TestElectionLeaderDisconnect3brokers(t *testing.T) {

	// election timeout set to 150ms
	// leader heartbeats every 25ms

	h := NewHarness(t, 3)
	// defer h.Shutdown()

	origLeaderId, origTerm := h.CheckSingleLeader()

	h.DisconnectPeer(origLeaderId)
	//sleepMs(350)

	start := time.Now()
	var newLeaderId, newTerm int
	foundNewLeader := false
	const timeout = 5 * time.Second

	// periodically check for new Leader
	for elapsed := time.Since(start); elapsed < timeout; elapsed = time.Since(start) {
		newLeaderId, newTerm = h.CheckSingleLeader()
		if newLeaderId != origLeaderId && newTerm > origTerm {
			foundNewLeader = true
			break
		}
		sleepMs(1)
	}

	end := time.Now()

	// newLeaderId, newTerm := h.CheckSingleLeader()

	if !foundNewLeader {
		t.Fatalf("No new leader elected within 5 seconds")
	}

	if newLeaderId == origLeaderId {
		t.Errorf("want new leader to be different from orig leader")
	}
	if newTerm <= origTerm {
		t.Errorf("want newTerm <= origTerm, got %d and %d", newTerm, origTerm)
	}

	h.Shutdown()

	duration := end.Sub(start)

	log.Printf("\n\n\n\n\n")
	log.Printf("[TestElectionLeaderDisconnect3brokers] metrics:")
	log.Printf("time to elect leader: %s", duration)

}

func TestElectionLeaderDisconnect4brokers(t *testing.T) {

	// election timeout set to 150ms
	// leader heartbeats every 25ms

	h := NewHarness(t, 4)
	// defer h.Shutdown()

	origLeaderId, origTerm := h.CheckSingleLeader()

	h.DisconnectPeer(origLeaderId)
	//sleepMs(350)

	start := time.Now()
	var newLeaderId, newTerm int
	foundNewLeader := false
	const timeout = 5 * time.Second

	// periodically check for new Leader
	for elapsed := time.Since(start); elapsed < timeout; elapsed = time.Since(start) {
		newLeaderId, newTerm = h.CheckSingleLeader()
		if newLeaderId != origLeaderId && newTerm > origTerm {
			foundNewLeader = true
			break
		}
		sleepMs(1)
	}

	end := time.Now()

	// newLeaderId, newTerm := h.CheckSingleLeader()

	if !foundNewLeader {
		t.Fatalf("No new leader elected within 5 seconds")
	}

	if newLeaderId == origLeaderId {
		t.Errorf("want new leader to be different from orig leader")
	}
	if newTerm <= origTerm {
		t.Errorf("want newTerm <= origTerm, got %d and %d", newTerm, origTerm)
	}

	h.Shutdown()

	duration := end.Sub(start)

	log.Printf("\n\n\n\n\n")
	log.Printf("[TestElectionLeaderDisconnect4brokers] metrics:")
	log.Printf("time to elect leader: %s", duration)

}

func TestElectionLeaderDisconnect7brokers(t *testing.T) {

	// election timeout set to 150ms
	// leader heartbeats every 25ms

	h := NewHarness(t, 7)
	// defer h.Shutdown()

	origLeaderId, origTerm := h.CheckSingleLeader()

	h.DisconnectPeer(origLeaderId)
	//sleepMs(350)

	start := time.Now()
	var newLeaderId, newTerm int
	foundNewLeader := false
	const timeout = 5 * time.Second

	// periodically check for new Leader
	for elapsed := time.Since(start); elapsed < timeout; elapsed = time.Since(start) {
		newLeaderId, newTerm = h.CheckSingleLeader()
		if newLeaderId != origLeaderId && newTerm > origTerm {
			foundNewLeader = true
			break
		}
		sleepMs(1)
	}

	end := time.Now()

	// newLeaderId, newTerm := h.CheckSingleLeader()

	if !foundNewLeader {
		t.Fatalf("No new leader elected within 5 seconds")
	}

	if newLeaderId == origLeaderId {
		t.Errorf("want new leader to be different from orig leader")
	}
	if newTerm <= origTerm {
		t.Errorf("want newTerm <= origTerm, got %d and %d", newTerm, origTerm)
	}

	h.Shutdown()

	duration := end.Sub(start)

	log.Printf("\n\n\n\n\n")
	log.Printf("[TestElectionLeaderDisconnect7brokers] metrics:")
	log.Printf("time to elect leader: %s", duration)

}

func TestElectionLeaderDisconnect20brokers(t *testing.T) {

	// election timeout set to 150ms
	// leader heartbeats every 25ms

	h := NewHarness(t, 20)
	// defer h.Shutdown()

	origLeaderId, origTerm := h.CheckSingleLeader()

	h.DisconnectPeer(origLeaderId)
	//sleepMs(350)

	start := time.Now()
	var newLeaderId, newTerm int
	foundNewLeader := false
	const timeout = 5 * time.Second

	// periodically check for new Leader
	for elapsed := time.Since(start); elapsed < timeout; elapsed = time.Since(start) {
		newLeaderId, newTerm = h.CheckSingleLeader()
		if newLeaderId != origLeaderId && newTerm > origTerm {
			foundNewLeader = true
			break
		}
		sleepMs(1)
	}

	end := time.Now()

	// newLeaderId, newTerm := h.CheckSingleLeader()

	if !foundNewLeader {
		t.Fatalf("No new leader elected within 5 seconds")
	}

	if newLeaderId == origLeaderId {
		t.Errorf("want new leader to be different from orig leader")
	}
	if newTerm <= origTerm {
		t.Errorf("want newTerm <= origTerm, got %d and %d", newTerm, origTerm)
	}

	h.Shutdown()

	duration := end.Sub(start)

	log.Printf("\n\n\n\n\n")
	log.Printf("[TestElectionLeaderDisconnect20brokers] metrics:")
	log.Printf("time to elect leader: %s", duration)

}

// func TestElection2LeaderDC(t *testing.T) {
// 	h := NewHarness(t, 3)
// 	defer h.Shutdown()

// 	origLeaderId, _ := h.CheckSingleLeader()
// 	log.Printf("leader is %d", origLeaderId)

// 	h.DisconnectPeer(origLeaderId)
// 	otherId := (origLeaderId + 1) % 3
// 	h.DisconnectPeer(otherId)

// 	// No quorum.
// 	sleepMs(450)
// 	h.CheckNoLeader()

// 	// Reconnect one other server; now we'll have quorum.
// 	h.ReconnectPeer(otherId)
// 	h.CheckSingleLeader()
// }

// func TestDisconnectAllThenRestore(t *testing.T) {
// 	h := NewHarness(t, 3)
// 	defer h.Shutdown()

// 	sleepMs(100)
// 	//	Disconnect all servers from the start. There will be no leader.
// 	for i := 0; i < 3; i++ {
// 		h.DisconnectPeer(i)
// 	}
// 	sleepMs(450)
// 	h.CheckNoLeader()

// 	// Reconnect all servers. A leader will be found.
// 	for i := 0; i < 3; i++ {
// 		h.ReconnectPeer(i)
// 	}
// 	h.CheckSingleLeader()
// }

// func TestElectionFollowerComesBack(t *testing.T) {

// 	h := NewHarness(t, 3)
// 	defer h.Shutdown()

// 	origLeaderId, origTerm := h.CheckSingleLeader()

// 	otherId := (origLeaderId + 1) % 3
// 	h.DisconnectPeer(otherId)
// 	time.Sleep(650 * time.Millisecond)
// 	h.ReconnectPeer(otherId)
// 	sleepMs(150)

// 	_, newTerm := h.CheckSingleLeader()
// 	if newTerm <= origTerm {
// 		t.Errorf("newTerm=%d, origTerm=%d", newTerm, origTerm)
// 	}
// }

func TestCommitOneCommand(t *testing.T) {

	h := NewHarness(t, 5)
	//defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()

	start := time.Now()

	tlog("submitting command {1} for document {testdoc} to %d", origLeaderId)
	isLeader := h.SubmitToServer(origLeaderId, "testdoc", 1) >= 0
	if !isLeader {
		t.Errorf("want id=%d leader, but it's not", origLeaderId)
	}

	//sleepMs(500)
	//h.CompareCommittedLogs()

	// periodically check of logs have been committedLog
	const timeout = 5 * time.Second
	committed := false

	for elapsed := time.Since(start); elapsed < timeout; elapsed = time.Since(start) {
		allcommitted := true
		for serverId := 0; serverId < h.n; serverId++ {
			_, committedLog, _, _ := h.GetLogsAndCommitIndexFromServer(serverId)
			if len(committedLog) == 0 {
				allcommitted = false
				break
			}
		}

		if allcommitted {
			committed = true
			break
		}
		sleepMs(1)
	}

	end := time.Now()

	if !committed {
		t.Fatalf("not all servers committed within %s", timeout)
	}

	h.CompareCommittedLogs()

	// log, commitIndex, logLen := h.GetLogAndCommitIndexFromServer(origLeaderId)
	// tlog("Leader %d CommitIndex: %d   log: %+v   idx of latest entry: %d ", origLeaderId, commitIndex, log, logLen-1)
	tlog("Leader is %d", origLeaderId)
	for serverId := 0; serverId < h.n; serverId++ {
		log, committedLog, commitIndex, logLen := h.GetLogsAndCommitIndexFromServer(serverId)
		tlog("Server %d CommitIndex: %d   log: %+v  committed: %+v  idx of latest entry: %d", serverId, commitIndex, log, committedLog, logLen-1)
	}

	h.Shutdown()

	duration := end.Sub(start)

	log.Printf("\n\n\n\n\n")
	log.Printf("[TestCommitOneCommand] metrics:")
	log.Printf("logs replicated and committed in %s", duration)
}

// make sure there are no dupes or missing entries
func TestCommitMultipleCommands(t *testing.T) {

	h := NewHarness(t, 3)

	origLeaderId, _ := h.CheckSingleLeader()

	start := time.Now()

	values := []int{1, 2, 3}
	docnames := []string{"doc1", "doc1", "doc2"}
	for i, v := range values {
		tlog("submitting {%d} for %d to %d", v, docnames[i], origLeaderId)
		isLeader := h.SubmitToServer(origLeaderId, docnames[i], v) >= 0
		if !isLeader {
			t.Errorf("want id=%d leader, but it's not", origLeaderId)
		}
		//sleepMs(100)
	}

	// periodically check of logs have been committedLog
	const timeout = 5 * time.Second
	committed := false

	for elapsed := time.Since(start); elapsed < timeout; elapsed = time.Since(start) {
		allcommitted := true
		for serverId := 0; serverId < h.n; serverId++ {
			_, committedLog, _, _ := h.GetLogsAndCommitIndexFromServer(serverId)
			if len(committedLog) == 0 {
				allcommitted = false
				break
			}
		}

		if allcommitted {
			committed = true
			break
		}
		sleepMs(1)
	}

	end := time.Now()

	if !committed {
		t.Fatalf("not all servers committed within %s", timeout)
	}

	//sleepMs(250)

	// compare logs across server to make sure they are identical
	h.CompareCommittedLogs()

	// log, commitIndex, logLen := h.GetLogAndCommitIndexFromServer(origLeaderId)
	// tlog("Leader %d CommitIndex: %d   log: %+v   idx of latest entry: %d ", origLeaderId, commitIndex, log, logLen-1)

	tlog("Leader is %d", origLeaderId)
	for serverId := 0; serverId < h.n; serverId++ {
		log, committedLog, commitIndex, logLen := h.GetLogsAndCommitIndexFromServer(serverId)
		tlog("Server %d CommitIndex: %d   log: %+v  committed: %+v  idx of latest entry: %d", serverId, commitIndex, log, committedLog, logLen-1)
	}

	h.Shutdown()

	duration := end.Sub(start)

	log.Printf("\n\n\n\n\n")
	log.Printf("[TestCommitMultipleCommands] metrics:")
	log.Printf("logs replicated and committed in %s", duration)
}

func TestCommitEvenMoreCommands(t *testing.T) {

	h := NewHarness(t, 3)

	origLeaderId, _ := h.CheckSingleLeader()

	start := time.Now()

	values := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	docnames := []string{"doc1", "doc1", "doc2", "doc2", "doc1", "doc2", "doc3", "doc1", "doc2", "doc3"}
	for i, v := range values {
		tlog("submitting {%d} for %d to %d", v, docnames[i], origLeaderId)
		isLeader := h.SubmitToServer(origLeaderId, docnames[i], v) >= 0
		if !isLeader {
			t.Errorf("want id=%d leader, but it's not", origLeaderId)
		}
		//sleepMs(100)
	}

	// periodically check of logs have been committedLog
	const timeout = 5 * time.Second
	committed := false

	for elapsed := time.Since(start); elapsed < timeout; elapsed = time.Since(start) {
		allcommitted := true
		for serverId := 0; serverId < h.n; serverId++ {
			_, committedLog, _, _ := h.GetLogsAndCommitIndexFromServer(serverId)
			if len(committedLog) == 0 {
				allcommitted = false
				break
			}
		}

		if allcommitted {
			committed = true
			break
		}
		sleepMs(1)
	}

	end := time.Now()

	if !committed {
		t.Fatalf("not all servers committed within %s", timeout)
	}

	//sleepMs(250)

	// compare logs across server to make sure they are identical
	h.CompareCommittedLogs()

	// log, commitIndex, logLen := h.GetLogAndCommitIndexFromServer(origLeaderId)
	// tlog("Leader %d CommitIndex: %d   log: %+v   idx of latest entry: %d ", origLeaderId, commitIndex, log, logLen-1)

	tlog("Leader is %d", origLeaderId)
	for serverId := 0; serverId < h.n; serverId++ {
		log, committedLog, commitIndex, logLen := h.GetLogsAndCommitIndexFromServer(serverId)
		tlog("Server %d CommitIndex: %d   log: %+v  committed: %+v  idx of latest entry: %d", serverId, commitIndex, log, committedLog, logLen-1)
	}

	h.Shutdown()

	duration := end.Sub(start)

	log.Printf("\n\n\n\n\n")
	log.Printf("[TestCommitEvenMoreCommands] metrics:")
	log.Printf("logs replicated and committed in %s", duration)
}

func TestFollowerCrashAndRecover(t *testing.T) {

	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()

	values := []int{1}
	docnames := []string{"doc1", "doc1", "doc2"}

	for i, v := range values {
		tlog("submitting {%d} for %d to %d", v, docnames[i], origLeaderId)
		isLeader := h.SubmitToServer(origLeaderId, docnames[i], v) >= 0
		if !isLeader {
			t.Errorf("want id=%d leader, but it's not", origLeaderId)
		}
		//sleepMs(100)
	}

	startFirstCommits := time.Now()
	// periodically check of logs have been committedLog
	const timeout = 5 * time.Second
	committed := false

	for elapsed := time.Since(startFirstCommits); elapsed < timeout; elapsed = time.Since(startFirstCommits) {
		allcommitted := true
		for serverId := 0; serverId < h.n; serverId++ {
			_, committedLog, _, _ := h.GetLogsAndCommitIndexFromServer(serverId)
			if len(committedLog) == 0 {
				allcommitted = false
				break
			}
		}

		if allcommitted {
			committed = true
			break
		}
		sleepMs(1)
	}

	//endFirstCommits := time.Now()

	if !committed {
		t.Fatalf("First commit: not all servers committed within %s", timeout)
	}

	otherId := (origLeaderId + 1) % 3
	tlog("Crashing follower %d", otherId)
	h.CrashPeer(otherId)

	values2 := []int{2}
	for i, v := range values2 {
		tlog("submitting {%d} for %d to %d", v, docnames[i], origLeaderId)
		isLeader := h.SubmitToServer(origLeaderId, docnames[i], v) >= 0
		if !isLeader {
			t.Errorf("want id=%d leader, but it's not", origLeaderId)
		}
		//sleepMs(100)
	}

	startSecondCommits := time.Now()

	const timeout2 = 5 * time.Second
	//committed2 := false

	for elapsed := time.Since(startSecondCommits); elapsed < timeout2; elapsed = time.Since(startSecondCommits) {
		allcommitted := true
		for serverId := 0; serverId < h.n; serverId++ {
			_, committedLog, _, _ := h.GetLogsAndCommitIndexFromServer(serverId)
			if len(committedLog) == 0 {
				allcommitted = false
				break
			}
		}

		if allcommitted {
			committed = true
			break
		}
		sleepMs(10)
	}
	//endSecondCommits := time.Now()

	// if !committed2 {
	// 	t.Fatalf("Second commit: not all servers committed within %s", timeout)
	// }

	time.Sleep(500 * time.Millisecond)
	tlog("Restarting follower %d", otherId)
	h.RestartPeer(otherId)

	startFollowerComesBack := time.Now()

	const timeout3 = 5 * time.Second
	//committed3 := false

	for elapsed := time.Since(startFollowerComesBack); elapsed < timeout3; elapsed = time.Since(startFollowerComesBack) {
		allcommitted := true
		for serverId := 0; serverId < h.n; serverId++ {
			_, committedLog, _, _ := h.GetLogsAndCommitIndexFromServer(serverId)
			if len(committedLog) == 0 {
				allcommitted = false
				break
			}
		}

		if allcommitted {
			committed = true
			break
		}
		sleepMs(10)
	}
	endFollowerComesBack := time.Now()

	// if !committed3 {
	// 	t.Fatalf("Check crashed follower: not all servers committed within %s", timeout)
	// }

	//h.CompareCommittedLogs()

	tlog("Leader is %d", origLeaderId)
	tlog("Crashed and Recovered Follower is %d", otherId)
	for serverId := 0; serverId < h.n; serverId++ {
		log, committedLog, commitIndex, logLen := h.GetLogsAndCommitIndexFromServer(serverId)
		tlog("Server %d CommitIndex: %d   log: %+v  committed: %+v  idx of latest entry: %d", serverId, commitIndex, log, committedLog, logLen-1)
	}

	//h.Shutdown()

	// firstCommitDuration := endFirstCommits.Sub(startFirstCommits)
	// secondCommitDuration := endSecondCommits.Sub(endSecondCommits)
	followerComesBackDuration := endFollowerComesBack.Sub(startFollowerComesBack)

	// log.Printf("first logs replicated and committed in %s", firstCommitDuration)
	// log.Printf("second logs replicated and committed in %s", secondCommitDuration)

	log.Printf("\n\n\n\n\n")
	log.Printf("[TestFollowerCrashAndRecover] metrics:")
	log.Printf("Crashed and Reconnected follower replicated and committed missing logs in %s", followerComesBackDuration)

}
