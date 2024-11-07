package crdt

/*
- Todo:
	- need to implement serialization and deserialization protocol
		- start with writing the operation to json then converting that 
		json to bytes and sending bytes over the network
		- convert the bytes to jsons then again to an operation object
*/

type OperationType int8
const (
	Insert = iota
	Delete = iota
)

type side int8
const (
	left = iota
	right = iota
)

type Operation interface {
	Type() OperationType
	GetDependencies() *VersionVector
	GetOperationID() ID
}

type InsertOperation struct {
	currentNodeID ID
	value interface{}
	dependencies *VersionVector
	parentNodeID ID
	side side
}

func NewInsertOperation(
	currentNodeID ID, 
	value interface{},
	dependencies *VersionVector,
	parentNodeID ID,
	side side,
) (*InsertOperation) {
	return &InsertOperation{
		currentNodeID: currentNodeID,
		value: value,
		dependencies: dependencies,
		parentNodeID: parentNodeID,
		side: side,
	}
}

func (op *InsertOperation) Type() OperationType {
	return Insert
}

func (op *InsertOperation) GetDependencies() *VersionVector {
	return op.dependencies
}

func (op *InsertOperation) GetOperationID() ID {
	return op.currentNodeID
}

type DeleteOperation struct {
	currentNodeID ID				// the id of the node to be deleted
	operationID ID					// the id of the delete operation
	dependencies *VersionVector		// the version vector indicating the operations with a happened
									// before relationship to the delete operation
}

func NewDeleteOperation(currentNodeID ID, operationID ID, dependencies *VersionVector) (*DeleteOperation) {
	return &DeleteOperation{
		currentNodeID: currentNodeID,
		operationID: operationID,
		dependencies: dependencies,
	}
}

func (op *DeleteOperation) Type() OperationType {
	return Delete
}

func (op *DeleteOperation) GetDependencies() *VersionVector {
	return op.dependencies
}

func (op *DeleteOperation) GetOperationID() ID {
	return op.operationID
}