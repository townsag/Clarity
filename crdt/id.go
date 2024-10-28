package crdt

type ID struct {
	replicaID 		string
	operationOffset int64
}