package crdt

import (
	"fmt"
)

type VersionVector struct {
	counters map[string]int64
	// will need a mutex at some point
}

func (v *VersionVector) IsValidOperation(replica_id string, operation_index int64) bool {
	if current_operation_index, ok := v.counters[replica_id]; ok {
		return current_operation_index == operation_index + 1
	}
	return false
}

func (v *VersionVector) UpdateOperation(replica_id string, operation_index int64) error {
	if v.IsValidOperation(replica_id, operation_index) {
		v.counters[replica_id] += 1
	} else {
		return fmt.Errorf(
			"tried to apply operation index %d to operation %d",
			operation_index, 
			v.counters[replica_id],
		)
	}
	return nil

}