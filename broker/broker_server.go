package broker

import (
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type LogEntry struct {
	Index int
	Edit  any
	Term  int
}

type BrokerServer struct {
	mu sync.Mutex

	brokerid int

	peerIds     []int
	peerClients map[int]*rpc.Client

	listener net.Listener

	// persistent state on all servers
	// should be replicated across all brokers
	term     int // what is the current term
	votedFor int // who this server voted for
	log      []LogEntry

	// states unique to each server
	state ServerState

	electionTimer *time.Timer

	// channel to ensure servers start together
	ready <-chan any
}

// i think we can just hardcode initialize one server as leader when we start up the cluster?
// ready <-chan any is for make sure everything starts are the same time when close(ready) in whatever starting the servers
func NewBrokerServer(brokerid int, peerIds []int, state ServerState, ready <-chan any) *BrokerServer {
	broker := new(BrokerServer)
	broker.brokerid = brokerid
	broker.peerIds = peerIds
	broker.peerClients = make(map[int]*rpc.Client)
	broker.state = state
	broker.ready = ready

	return broker
}

type ServerState int

const (
	Follower  ServerState = 0
	Candidate ServerState = 1
	Leader    ServerState = 2
	Dead      ServerState = 3
)

// converts CMState values into strings
// for debugging and terminal prints
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

// Broker Server's main routine
// each broker must:
//
//	If leader:
//		recieve CRDT operations from application server/s
//		update own log and send update to followers
//		make sure enough followers recieved update then tell all followers to commit
//		heartbeat to followers and application servers
//		handle application server polls and respond with the correct log
//	if follower:
//		handle application server polls and respond with the correct log
//		maintain consistency with leader
//		maintain timeout to elect new leader of leader is dead (no heartbeat)
//

func (broker *BrokerServer) Serve() {

	broker.mu.Lock()

	var err error
	broker.listener, err = net.Listen("tcp", ":0") // listen on any open port
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%v] listening at %s", broker.brokerid, broker.listener.Addr())

	broker.mu.Unlock()

	// start listening for request
	go func() {
		for {
			conn, err := broker.listener.Accept()
			if err != nil {
				log.Fatal(err)
				continue
			}
			// go routine so that rpc is non blocking
			go rpc.ServeConn(conn)

		}
	}()

	// start election timer
	broker.resetElectionTimer()

	// if follower gets a log update. reject? then app server should resend to leader
}

// set randomized timeout when called
// or start election when timeout runs out
func (broker *BrokerServer) resetElectionTimer() {
	if broker.electionTimer != nil {
		broker.electionTimer.Stop()
	}

	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	broker.electionTimer = time.NewTimer(timeout)

	// start election when timer runs out
	go func() {
		<-broker.electionTimer.C
		broker.startElection()
	}()
}

// election algorithm
func (broker *BrokerServer) startElection() {

	broker.mu.Lock()
	broker.state = Candidate
	broker.term++
	savedCurrentTerm := broker.term

	// server votes for itself
	//votes := 1
	log.Printf("becomes Candidate (currentTerm=%d)", savedCurrentTerm)
	broker.mu.Unlock()

	// send vote request rpc to all peers
	for _, peerId := range broker.peerIds {
		go func(peerId int) {
			// need log index so need logs to work first
			return
		}(peerId)
	}

}

// func main() {

// 	listener, err := net.Listen("tcp", "localhost:8000")
// 	if err != nil {
// 		fmt.Println("Error:", err)
// 		return
// 	}
// 	defer listener.Close()

// 	fmt.Println("Server is listening on port 8080")

// 	for {
// 		// Accept incoming connections
// 		conn, err := listener.Accept()
// 		if err != nil {
// 			fmt.Println("Error:", err)
// 			continue
// 		}

// 		// Handle client connection in a goroutine
// 		go handleClient(conn)
// 	}

// }

// func handleClient(conn net.Conn) {
// 	defer conn.Close()

// 	// Read and process data from the client
// 	// ...

// 	// Write data back to the client
// 	// ...
// }
