package appserver

import (
	"log"
	"testing"
	"time"

	"github.com/townsag/clarity/broker"

	"github.com/gorilla/websocket"
)

func TestHandleWebSocket(t *testing.T) {

	// use broker server harness to create cluster of brokers
	h := broker.NewHarness(t, 3)

	// Setup the AppServer
	brokerAddrs := make([]string, len(h.Cluster()))
	for i, broker := range h.Cluster() {
		brokerAddrs[i] = broker.GetListenAddr().String()
	}

	// set up app server
	appServer := NewAppServer("testReplica", brokerAddrs)
	go func() {
		if err := appServer.Serve(":8080"); err != nil {
			//t.Fatalf("failed to start AppServer: %v", err)
		}
	}()

	// Allow time for servers to start
	time.Sleep(2 * time.Second)

	// Connect a WebSocket client
	addr := "ws://localhost:8080/ws"
	client, _, err := websocket.DefaultDialer.Dial(addr, nil)
	if err != nil {
		t.Fatalf("failed to connect to WebSocket server: %v", err)
	}
	defer client.Close()

	// Send a test message to the WebSocket server
	msg := Message{
		Type:      "insert",
		Index:     1,
		Value:     "test",
		ReplicaID: "testReplica",
		OpIndex:   1,
		Source:    "client",
	}

	// send log to brokers
	err = client.WriteJSON(msg)
	if err != nil {
		t.Fatalf("failed to send WebSocket message: %v", err)
	}
	log.Printf("Log %+v sent to brokers", msg)

	// Call requestCRDTLogs to get the logs from the brokers
	if err := appServer.requestCRDTLogs(); err != nil {
		t.Fatalf("failed to request CRDT logs: %v", err)
	}

	// Verify that the AppServer's CRDT state has the expected update
	expectedState := []interface{}{"test"} // The expected value after the insert operation
	actualState := appServer.GetRepresentation()

	if len(actualState) != len(expectedState) || actualState[0] != expectedState[0] {
		t.Fatalf("Expected CRDT state %v, but got %v", expectedState, actualState)
	}

}
