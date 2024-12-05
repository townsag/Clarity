module appserver

go 1.23.2

require github.com/gorilla/websocket v1.5.3

require github.com/townsag/clarity/crdt v0.1.0

replace github.com/townsag/clarity/crdt => ../crdt
