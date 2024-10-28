package crdt


type CRDT interface {
	Insert(value interface{}, index int, replica_id string, operation_index int64) error
	Delete(index int, replica_id string, operation_index int64) error
	Traverse(index int) ([]interface{}, error)
	String() string
}