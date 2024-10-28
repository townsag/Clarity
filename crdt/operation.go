package crdt

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
}

type InsertOperation struct {
	currentNodeID ID
	value interface{}
	parentNodeID ID
	side side
}

func NewInsertOperation(
	currentNodeID ID, 
	value interface{}, 
	parentNodeID ID,
	side side,
) (*InsertOperation) {
	return &InsertOperation{
		currentNodeID: currentNodeID,
		value: value,
		parentNodeID: parentNodeID,
		side: side,
	}
}

func (op *InsertOperation) Type() OperationType {
	return Insert
}

type DeleteOperation struct {
	currentNodeID ID
}

func NewDeleteOperation(currentNodeID ID) (*DeleteOperation) {
	return &DeleteOperation{currentNodeID: currentNodeID}
}

func (op *DeleteOperation) Type() OperationType {
	return Delete
}