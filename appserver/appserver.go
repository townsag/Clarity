package appserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"

	"clarity/crdt"

	"github.com/gorilla/websocket"
)

type AppServer struct {
	mu       sync.Mutex
	upgrader websocket.Upgrader
	clients  map[*websocket.Conn]bool
	brokers  []string
	textCRDT *crdt.TextCRDT
}

type Message struct { // Type, Index, Value combine to create crdt operation
	Type      string      `json:"type"`  // the crdt operation type {insert, delete}
	Index     int64       `json:"index"` // index of the operation
	Value     interface{} `json:"value"` // chars being inserted / deleted
	ReplicaID string      `json:"replica_id"`
	OpIndex   int64       `json:"operation_index"` // identifies the document the crdt operations edit
	Source    string      `json:"source"`          // "client" or "broker"
}

func NewAppServer(replicaID string, brokerList []string) *AppServer {
	return &AppServer{
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
		clients:  make(map[*websocket.Conn]bool),
		brokers:  brokerList,
		textCRDT: crdt.NewTextCRDT(replicaID),
	}
}

func (s *AppServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %v", err)
		return
	}

	defer func(conn *websocket.Conn) {
		err := conn.Close()
		if err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}(conn)

	s.mu.Lock()
	s.clients[conn] = true
	s.mu.Unlock()

	for {
		var msg Message
		err := conn.ReadJSON(&msg)
		if err != nil {
			log.Printf("Error reading message: %v", err)
			s.mu.Lock()
			delete(s.clients, conn)
			s.mu.Unlock()
			break
		}

		switch msg.Source {
		case "client":
			// Forward the message directly to broker
			s.sendHTTPMessage(msg)
			// Update local CRDT and broadcast to other clients
			s.handleOperation(msg)

		case "broker":
			// Update local CRDT state and broadcast to clients
			s.handleOperation(msg)
		}
	}
}

func (s *AppServer) handleOperation(msg Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var operation crdt.Operation

	switch msg.Type {
	case "insert":
		operation = s.textCRDT.LocalInsert(msg.Index, msg.Value)
	case "delete":
		operation = s.textCRDT.LocalDelete(msg.Index)
	default:
		log.Printf("Unknown operation type: %s", msg.Type)
		return
	}

	// Broadcast operation to all clients
	s.broadcastOperation(operation)
}

func (s *AppServer) sendHTTPMessage(msg Message) {
	for _, brokerAddr := range s.brokers {
		url := fmt.Sprintf("http://%s/crdt", brokerAddr)
		jsonData, err := json.Marshal(msg)
		if err != nil {
			log.Printf("Error marshaling message for broker %s: %v", brokerAddr, err)
			continue
		}

		go func(addr string, data []byte) {
			resp, err := http.Post(addr, "application/json", bytes.NewBuffer(data))
			if err != nil {
				log.Printf("Error sending message to broker %s: %v", addr, err)
				return
			}
			defer func(Body io.ReadCloser) {
				err := Body.Close()
				if err != nil {
					log.Printf("Error closing body: %v", err)
				}
			}(resp.Body)
		}(url, jsonData)
	}
}

func (s *AppServer) broadcastOperation(op crdt.Operation) {
	for client := range s.clients {
		err := client.WriteJSON(op)
		if err != nil {
			log.Printf("Error broadcasting to client: %v", err)
			err := client.Close()
			if err != nil {
				return
			}
			delete(s.clients, client)
		}
	}
}

func (s *AppServer) GetRepresentation() []interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.textCRDT.Representation()
}

func (s *AppServer) Serve(addr string) error {
	http.HandleFunc("/ws", s.handleWebSocket)

	log.Printf("Starting application server on %s", addr)
	return http.ListenAndServe(addr, nil)
}
