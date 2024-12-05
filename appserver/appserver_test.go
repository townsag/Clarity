package appserver

import (
	"log"
	"testing"

	"github.com/gorilla/websocket"
)

func TestHandleWebSocket(t *testing.T) {
	// Setup the AppServer
	server := NewAppServer("testReplica", []string{"localhost:8081"})
	go server.Serve(":8080")

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
	err = client.WriteJSON(msg)
	if err != nil {
		t.Fatalf("failed to send WebSocket message: %v", err)
	}

	// Receive the message back
	var receivedOp string
	err = client.ReadJSON(&receivedOp)
	if err != nil {
		t.Fatalf("failed to receive WebSocket message: %v", err)
	}

	log.Printf("%s", receivedOp)
}
