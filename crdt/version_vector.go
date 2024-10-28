package crdt

import (
	"fmt"
)

type VersionVector struct {
	counters map[string]int64
	// will need a mutex at some point
}

func NewVersionVector(replicaID string) *VersionVector {
	v := &VersionVector{
		counters: make(map[string]int64),
	}
	v.counters[replicaID] = 0
	return v
}

func (v *VersionVector) ContainsReplicaID(replicaID string) bool {
	_, ok := v.counters[replicaID]
	return ok
}

func (v *VersionVector) RegisterReplica(replicaID string) error {
	if _, ok := v.counters[replicaID]; ok {
		return fmt.Errorf("tried to register new replicaID that already existed in version vector: %v", replicaID)
	}
	v.counters[replicaID] = 0
	return nil
}

func (v *VersionVector) IsValidOperation(replicaID string, operationOffset int64) bool {
	if current_operation_index, ok := v.counters[replicaID]; ok {
		return current_operation_index == operationOffset + 1
	}
	return false
}

func (v *VersionVector) IncrementVersion(replicaID string) (newOperationOffset int64, err error) {
	if _, ok := v.counters[replicaID]; !ok {
		return 0, fmt.Errorf("could not find replicaId: %v in version vector", replicaID)
	}
	v.counters[replicaID] += 1
	return v.counters[replicaID], nil
}

func (v *VersionVector) UpdateOperation(replicaID string, operationOffset int64) error {
	if v.IsValidOperation(replicaID, operationOffset) {
		v.counters[replicaID] += 1
	} else {
		return fmt.Errorf(
			"tried to apply operation index %d to operation %d",
			operationOffset, 
			v.counters[replicaID],
		)
	}
	return nil

}