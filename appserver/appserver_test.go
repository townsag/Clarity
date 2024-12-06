package appserver

import (
	"log"
	"testing"
	"time"

	"github.com/townsag/clarity/broker"

	"github.com/gorilla/websocket"
)

func TestHandleSendAndGet3brokers(t *testing.T) {

	// for metrics
	teststart := time.Now()
	createClusterStart := time.Now()

	// use broker server harness to create cluster of brokers
	h := broker.NewHarness(t, 3)

	createClusterEnd := time.Now()

	// get brokerserver http addresses
	brokerAddrs := make([]string, len(h.Cluster()))
	for i, broker := range h.Cluster() {
		brokerAddrs[i] = broker.GetHTTPAddr()
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

	sendMessageToBrokerStart := time.Now()

	// send log to brokers
	err = client.WriteJSON(msg)
	if err != nil {
		t.Fatalf("failed to send WebSocket message: %v", err)
	}
	log.Printf("Log %+v sent to brokers", msg)

	sendMessageToBrokerEnd := time.Now()

	requestLogFromBrokerStart := time.Now()

	// Call requestCRDTLogs to get the logs from the brokers
	if err := appServer.requestCRDTLogs(); err != nil {
		t.Fatalf("failed to request CRDT logs: %v", err)
	}

	requestLogFromBrokerEnd := time.Now()

	time.Sleep(1 * time.Second)

	testend := time.Now()

	h.Shutdown()

	// create and print time metrics

	testDuration := testend.Sub(teststart)
	createClusterDuration := createClusterEnd.Sub(createClusterStart)
	sendMessageToBrokerDuration := sendMessageToBrokerEnd.Sub(sendMessageToBrokerStart)
	requestLogFromBrokerDuration := requestLogFromBrokerEnd.Sub(requestLogFromBrokerStart)

	roundtripDuration := requestLogFromBrokerEnd.Sub(sendMessageToBrokerStart)

	log.Printf("\n\n\n\n")
	log.Printf("[TestHandleSendAndGet3brokers] metrics:")
	log.Printf("Test took %s", testDuration)
	log.Printf("cluster creation took: %s", createClusterDuration)
	log.Printf("appserver send message to broker took: %s", sendMessageToBrokerDuration)
	log.Printf("appserver request log from broker took: %s", requestLogFromBrokerDuration)
	log.Printf("roundtrip: %s", roundtripDuration)

}

func TestHandleSendAndGet4brokers(t *testing.T) {

	// for metrics
	teststart := time.Now()
	createClusterStart := time.Now()

	// use broker server harness to create cluster of brokers
	h := broker.NewHarness(t, 4)

	createClusterEnd := time.Now()

	// get brokerserver http addresses
	brokerAddrs := make([]string, len(h.Cluster()))
	for i, broker := range h.Cluster() {
		brokerAddrs[i] = broker.GetHTTPAddr()
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

	sendMessageToBrokerStart := time.Now()

	// send log to brokers
	err = client.WriteJSON(msg)
	if err != nil {
		t.Fatalf("failed to send WebSocket message: %v", err)
	}
	log.Printf("Log %+v sent to brokers", msg)

	sendMessageToBrokerEnd := time.Now()

	requestLogFromBrokerStart := time.Now()

	// Call requestCRDTLogs to get the logs from the brokers
	if err := appServer.requestCRDTLogs(); err != nil {
		t.Fatalf("failed to request CRDT logs: %v", err)
	}

	requestLogFromBrokerEnd := time.Now()

	time.Sleep(1 * time.Second)

	testend := time.Now()

	h.Shutdown()

	// create and print time metrics

	testDuration := testend.Sub(teststart)
	createClusterDuration := createClusterEnd.Sub(createClusterStart)
	sendMessageToBrokerDuration := sendMessageToBrokerEnd.Sub(sendMessageToBrokerStart)
	requestLogFromBrokerDuration := requestLogFromBrokerEnd.Sub(requestLogFromBrokerStart)

	roundtripDuration := requestLogFromBrokerEnd.Sub(sendMessageToBrokerStart)

	log.Printf("\n\n\n\n")
	log.Printf("[TestHandleSendAndGet3brokers] metrics:")
	log.Printf("Test took %s", testDuration)
	log.Printf("cluster creation took: %s", createClusterDuration)
	log.Printf("appserver send message to broker took: %s", sendMessageToBrokerDuration)
	log.Printf("appserver request log from broker took: %s", requestLogFromBrokerDuration)
	log.Printf("roundtrip: %s", roundtripDuration)

}

func TestHandleSendAndGet7brokers(t *testing.T) {

	// for metrics
	teststart := time.Now()
	createClusterStart := time.Now()

	// use broker server harness to create cluster of brokers
	h := broker.NewHarness(t, 7)

	createClusterEnd := time.Now()

	// get brokerserver http addresses
	brokerAddrs := make([]string, len(h.Cluster()))
	for i, broker := range h.Cluster() {
		brokerAddrs[i] = broker.GetHTTPAddr()
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

	sendMessageToBrokerStart := time.Now()

	// send log to brokers
	err = client.WriteJSON(msg)
	if err != nil {
		t.Fatalf("failed to send WebSocket message: %v", err)
	}
	log.Printf("Log %+v sent to brokers", msg)

	sendMessageToBrokerEnd := time.Now()

	requestLogFromBrokerStart := time.Now()

	// Call requestCRDTLogs to get the logs from the brokers
	if err := appServer.requestCRDTLogs(); err != nil {
		t.Fatalf("failed to request CRDT logs: %v", err)
	}

	requestLogFromBrokerEnd := time.Now()

	time.Sleep(1 * time.Second)

	testend := time.Now()

	h.Shutdown()

	// create and print time metrics

	testDuration := testend.Sub(teststart)
	createClusterDuration := createClusterEnd.Sub(createClusterStart)
	sendMessageToBrokerDuration := sendMessageToBrokerEnd.Sub(sendMessageToBrokerStart)
	requestLogFromBrokerDuration := requestLogFromBrokerEnd.Sub(requestLogFromBrokerStart)

	roundtripDuration := requestLogFromBrokerEnd.Sub(sendMessageToBrokerStart)

	log.Printf("\n\n\n\n")
	log.Printf("[TestHandleSendAndGet3brokers] metrics:")
	log.Printf("Test took %s", testDuration)
	log.Printf("cluster creation took: %s", createClusterDuration)
	log.Printf("appserver send message to broker took: %s", sendMessageToBrokerDuration)
	log.Printf("appserver request log from broker took: %s", requestLogFromBrokerDuration)
	log.Printf("roundtrip: %s", roundtripDuration)

}

func TestHandleSendAndGet20brokers(t *testing.T) {

	// for metrics
	teststart := time.Now()
	createClusterStart := time.Now()

	// use broker server harness to create cluster of brokers
	h := broker.NewHarness(t, 20)

	createClusterEnd := time.Now()

	// get brokerserver http addresses
	brokerAddrs := make([]string, len(h.Cluster()))
	for i, broker := range h.Cluster() {
		brokerAddrs[i] = broker.GetHTTPAddr()
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

	sendMessageToBrokerStart := time.Now()

	// send log to brokers
	err = client.WriteJSON(msg)
	if err != nil {
		t.Fatalf("failed to send WebSocket message: %v", err)
	}
	log.Printf("Log %+v sent to brokers", msg)

	sendMessageToBrokerEnd := time.Now()

	requestLogFromBrokerStart := time.Now()

	// Call requestCRDTLogs to get the logs from the brokers
	if err := appServer.requestCRDTLogs(); err != nil {
		t.Fatalf("failed to request CRDT logs: %v", err)
	}

	requestLogFromBrokerEnd := time.Now()

	time.Sleep(1 * time.Second)

	testend := time.Now()

	h.Shutdown()

	// create and print time metrics

	testDuration := testend.Sub(teststart)
	createClusterDuration := createClusterEnd.Sub(createClusterStart)
	sendMessageToBrokerDuration := sendMessageToBrokerEnd.Sub(sendMessageToBrokerStart)
	requestLogFromBrokerDuration := requestLogFromBrokerEnd.Sub(requestLogFromBrokerStart)

	roundtripDuration := requestLogFromBrokerEnd.Sub(sendMessageToBrokerStart)

	log.Printf("\n\n\n\n")
	log.Printf("[TestHandleSendAndGet3brokers] metrics:")
	log.Printf("Test took %s", testDuration)
	log.Printf("cluster creation took: %s", createClusterDuration)
	log.Printf("appserver send message to broker took: %s", sendMessageToBrokerDuration)
	log.Printf("appserver request log from broker took: %s", requestLogFromBrokerDuration)
	log.Printf("roundtrip: %s", roundtripDuration)

}
