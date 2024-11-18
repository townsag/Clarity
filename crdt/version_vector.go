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
	v.counters[replicaID] = 0		// does this mean that the version number / operation offset is 1 indexed
	return v
}

func (original *VersionVector) Copy() *VersionVector {
	copy := &VersionVector{
		counters: make(map[string]int64),
	}
	for replicaID, counter := range original.counters {
		copy.counters[replicaID] = counter
	}
	return copy
}

func (first *VersionVector) lessOrEqual(second *VersionVector) bool {
	for replicaID, counter1 := range first.counters {
		counter2, ok := second.counters[replicaID]
		if !ok || !(counter1 <= counter2) {
			return false
		}
	}
	return true
}

func (v *VersionVector) ContainsReplicaID(replicaID string) bool {
	_, ok := v.counters[replicaID]
	return ok
}

func (v *VersionVector) RegisterReplica(replicaID string) error {
	if _, ok := v.counters[replicaID]; ok {
		return fmt.Errorf("tried to register new replicaID that already existed in version vector: %v", replicaID)
	}
	v.counters[replicaID] = 0	// this means that the version 
	return nil
}

func (v *VersionVector) IsDuplicateOperation(operationID ID) bool {
	localCounter, ok := v.counters[operationID.replicaID]
	if !ok || localCounter < operationID.operationOffset {
		return false
	}
	return true
}

func (v *VersionVector) IsValidOperation(operationID ID) bool {
	if current_operation_index, ok := v.counters[operationID.replicaID]; ok {
		return current_operation_index + 1 == operationID.operationOffset
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

func (v *VersionVector) UpdateOperation(operationID ID) error {
	if v.IsValidOperation(operationID) {
		v.counters[operationID.replicaID] += 1
	} else {
		return fmt.Errorf(
			"tried to apply operation index %d to operation %d",
			operationID.operationOffset, 
			v.counters[operationID.replicaID],
		)
	}
	return nil
}