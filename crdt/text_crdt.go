package crdt

import (
	"fmt"
    "sort"
)

type TextCRDT struct {
	replicaID 		string
	root 			*Node
	versionVector	*VersionVector
	// nodeByID		map[ID]*Node
	buffer		    map[string][]Operation
}

func NewTextCRDT(replicaID string) *TextCRDT {
    newBuffer := make(map[string][]Operation)
	return &TextCRDT{
		replicaID: replicaID,
		root: NewNode(
			ID{replicaID: "root", operationOffset: 0},
			nil,
		),
		versionVector: NewVersionVector(replicaID),
        buffer: newBuffer,
	}
}

/* 
Todo:
	- architecture changes:
		- crdt object should have a has-a relationship with the TextCRDT state
		- should be responsible for managing queues of operations
	- need to be able to serialize and deserialize the crdt
		- answer the question: "what does this look like when we write it to a database"
		- options:
			- write the crdt tree to json string, store the json string as a record in 
			a nosql database 
			- 
	- need to better handle version of the operation in the apply function
		- two different types of version rules
			- we need to decide what makes an operation "valid"
			- background:
				- the version vector of the crdt represents the most recent operation that the crdt has
				seen from each replica
				- the version vector of an operation holds the offset of that operation on the replica from
				which it originated as well as the version of that replica at the time of the operation
				- version vectors can be:
					- equal:
						- VT1 = VT2 iff VT1[i] = VT2[i] ∀ i = 1, ..., N
					- less than or equal to
						- VT1 ≤ VT2 iff VT1[i] ≤ VT2[i] ∀ i = 1, ..., N
						- replica 2 has seen all of the same operations that replica 1 has seen and maybe more
					- happened before:
						- VT1 → VT2 iff VT1[i] ≤ VT2[i] ∀ i = 1, ..., N && ∃ j | 1 ≤ j ≤ N & VT1[j] < VT2[j]
						- the operation offset of the most recent operation for each replicaID in vector 
						timestamp one is less than or equal to the operation offset of the most recent 
						operation for each replicaID in vector timestamp 2. Also there is at least one operation
						offset in vector timestamp two that is greater than the respective offset in vector one 
						- replica 2 has seen all of the operations that replica 1 has seen and at least one more 
						operation
					- concurrent:
						- VT1 ||| VT2 iff NOT(VT1 ≤ VT2) AND NOT(VT2 ≤ VT1)
						- two events are concurrent if the version vector from the first is not less than the second
						and vice versa
						- two replicas can have concurrent edits if each replica has operations that are at a higher
						offset than the operations seen for that respective replica offset on the other replica
					- adjacent/consecutive
						- VT1 |→| VT2 iff VT1[i] ≤ VT2[i] ∀ i | i ∈ [1, N] - [j] && ∃ j | 1 ≤ j ≤ N & VT1[j] + 1 = VT2[j]
						- special case of casually related / happens before that also specifies that it is the
						next operation
						- this is not what we are looking for though, we want to apply an operation with version
						vector VT2 when all but one of the offsets are less than or equal to the offsets in VT1,
						the version vector for the local crdt
					
			- strong causal ordering:
				- only apply operations to local CRDT state when the local CRDT is up to date with all the
				operations with a "happened before" relationship to the operation to be applied
					- it may be easier to think of an operation as having a vector timestamp representing the
					operations that operation is dependent on. Then we can use the causal ordering / happened 
					before relationship to determine if it is safe to apply an operation. The happens before
					relationship is very well defined
					- given a local crdt with vector timestamp VT1 and an operation that occurred at a timestamp of 
					VT2 on it's crdt replica, only apply the operation if:
						- VT2[i] ≤ VT1[i] ∀ i | i ≠ replicaID of the operation
						- VT2[j] = VT1[j] + 1 where j = replicaID of the operation
					- **it is not clear to me if this will cause deadlock**
						- What is a situation that would cause deadlock?
						- there are three replicas. Replica1 and Replica2 both have operations that Replica3 needs
						to apply to it's local state
						- try proof by contradiction?
					- can I prove that there will always be one valid operation if all the operations have
					been received
						- can I prove that there is no scenario in which there is a circular wait
					- this approach is **not** robust to network partitions
				- means that the crdt can only apply operations when the crdt has seen the operations that 
				the replica that originally made that operation had seen
				- what does the class api look like? How do I tell the client which operations are missing?
					- is this just using the local version vector
			- weak causal ordering
				- only apply operations to local CRDT state when the node that operation depends on is
				present and the previous operation from that replica has been applied
					- origin node for insert
					- current node for delete
				- each operation has to store:
					- parent node id
					- current node id
				- this is faster but could result in less meaningful inserts
		- options:
			- crdt throws an error when the client tries to perform an operation that is
			more than one offset higher than the current offset of that replica id
				- this would require that the client maintains a queue of pending operations
				and tries them periodically
			- crdt accepts operations but only applies valid operations. maintains an internal
			queue of pending operations
				- might have to write some getters that tell the client what the state of pending
				operations is
		- also, the version of an operation might also need to take into account the version
		of edits from other replicas. look further into version requirements
*/

/*
Design choice:
	- write an receive and an apply function:
		- receive is responsible for receiving a stream of operations and determining if 
		the crdt is ready to apply any of the received operations based on the version
		vector of the operations dependencies and the version vector of the local crdt state
			- maintain a queue of pending operations that have been received but not applied 
		- apply is responsible for updating the local crdt state
*/
func (crdt *TextCRDT) Recieve(operation Operation) error {
    var operationID ID = operation.GetOperationID()
    // check if the version vector already contains this opperation
    if crdt.versionVector.IsDuplicateOperation(operationID) {
        return nil
    }
    // add the operation to the relevant queue
    queue, ok := crdt.buffer[operationID.replicaID]
    if !ok {
        // this is the case in which a queue does not exist for that replica id
        if !crdt.versionVector.ContainsReplicaID(operationID.replicaID) {
            crdt.versionVector.RegisterReplica(operationID.replicaID)
        }
        crdt.buffer[operationID.replicaID] = []Operation{operation}
    } else {
        // this is the case in which a queue does exist for that replica
        // find the index which we need to insert this operation
        i := sort.Search(len(queue), func(i int) bool {
            // false for the prefix of the array that is less than the value to be inserted
            // true for the part of the array that is greater than the value to be inserted
            return queue[i].GetOperationID().operationOffset > operationID.operationOffset
        })
        // insert the new operation into the queue
        crdt.buffer[operationID.replicaID] = append(
            crdt.buffer[operationID.replicaID][:i],
            append([]Operation{operation}, crdt.buffer[operationID.replicaID][i:]...)...,
        )
    }
    return crdt.poll()
}

/*
Assumptions:
- there are no duplicate operations in the queue
    - all the operations in the queue have not been applied yet
    - there is only one copy of each operation in the queue
*/
func (crdt *TextCRDT) poll() error {
    for replicaID, queue := range crdt.buffer {
        // queue is a queue of operations that is sorted by operation offset
        // poll from the same queue until we find a nonvalid operation or the queue is empty
        // then move on to the next queue
        for len(queue) > 0 {
            // check if the operation at the head of the queue is valid:
            //  - check that the dependencies of the operation have been met
            operationDependencies := queue[0].GetDependencies()
            operationID := queue[0].GetOperationID()
            if operationDependencies.lessOrEqual(crdt.versionVector) && crdt.versionVector.IsValidOperation(operationID) {
                // pop the operation from the queue 
                toApply := queue[0]
                queue = queue[1:]
                crdt.apply(toApply)
            } else {
                // found a non-valid operation, stop processing this queue
                break
            }
        }
        crdt.buffer[replicaID] = queue
    }
    return nil
}

// Design Choice:
//	- write two different insert functions
//		- one for receiving an operation from another replica and applying that operation
//		- one for inserting values originating at this replica
// TODO: test that this handles the case where there is no right origin
func (crdt *TextCRDT) apply(operation Operation) error {
	// check to see if the operation already exists
	operationID := operation.GetOperationID()
	if crdt.versionVector.IsDuplicateOperation(operationID) {
		return nil
	}
	// check that the local crdt has seen the dependent operations of the current operation
	operationDependencies := operation.GetDependencies()
	if !operationDependencies.lessOrEqual(crdt.versionVector) {
		return fmt.Errorf(
			"tried to apply operation with version vector: \n%v\n to replica with version vector:\n%v",
			*operationDependencies,
			*crdt.versionVector,
		)
	}
	switch operation.Type() {
	case Insert:
		insertOp := operation.(*InsertOperation)
		parentNode, err := crdt.findNodeByID(insertOp.parentNodeID)
		if err != nil {
			return err
		}
		switch insertOp.side{
		case left:
			parentNode.insertLeftChild(NewNode(insertOp.currentNodeID, insertOp.value))
		case right:
			parentNode.insertRightChild(NewNode(insertOp.currentNodeID, insertOp.value))
		}
	case Delete:
		deleteOp := operation.(*DeleteOperation)
		toDelete, err := crdt.findNodeByID(deleteOp.currentNodeID)
		if err != nil {
			return err
		}
		toDelete.value = nil

	}
	// update the version vector of the crdt to reflect recent change
	err := crdt.versionVector.UpdateOperation(operationID)
	if err != nil {
		return err
	}
	return nil
}

func (crdt *TextCRDT) LocalInsert(index int64, value interface{}) (*InsertOperation) {
	var leftOrigin, rightOrigin *Node
	var err error
	var newOperationOffset int64
	var parentNodeID ID
	var side side
	// for user experience purposes an operation is dependent on all operations that
	// have a happened before relationship with it
	var dependencies *VersionVector = crdt.versionVector.Copy()
	leftOrigin, rightOrigin, err = crdt.findOriginsHelper(index)
	if err != nil {
		panic(err)
	}
	newOperationOffset, _ = crdt.versionVector.IncrementVersion(crdt.replicaID);
	// if there does not exist a right child of the left origin node
	if !leftOrigin.hasRightChild() {
		// create a new node that is a right child of the left origin
		side = right
		leftOrigin.insertRightChild(NewNode(
			ID{replicaID: crdt.replicaID, operationOffset: newOperationOffset},
			value,
		))
		parentNodeID = leftOrigin.nodeID
	} else {
		// else create a new node that is a left child of the right origin
		side = left
		rightOrigin.insertLeftChild(NewNode(
			ID{replicaID: crdt.replicaID, operationOffset: newOperationOffset},
			value,
		))
		parentNodeID = rightOrigin.nodeID
	}
	newNodeID := ID{replicaID: crdt.replicaID, operationOffset: newOperationOffset}
	return NewInsertOperation(newNodeID, value, dependencies, parentNodeID, side)	
}

func (crdt *TextCRDT) LocalDelete(index int64) (*DeleteOperation) {
	var dependencies *VersionVector = crdt.versionVector.Copy()
	nodeToDelete, err := crdt.findNodeByIndex(index)
	if err != nil {
		panic(err)
	}
	nodeToDelete.value = nil
	newOperationOffset, _ := crdt.versionVector.IncrementVersion(crdt.replicaID)
	var operationID ID = ID{replicaID: crdt.replicaID, operationOffset: newOperationOffset}
	return NewDeleteOperation(nodeToDelete.nodeID, operationID, dependencies)
}

// use helper function and closure to implement find origins
// TODO: test that this handles the case where there is no right origin
// TODO: test that this does not reassign the left index or the right index
//		 once they have been found
func (crdt *TextCRDT) findOriginsHelper(index int64) (leftOrigin *Node, rightOrigin *Node, err error) {
	var currentIndex int64 = -1			// list of characters in representation is zero indexed

	var helper func(*Node)
	helper = func(root *Node){
		// base case, stop traversing when correct origins have been found
		if leftOrigin != nil && rightOrigin != nil {
			return
		}
		
		// iterate over the left children of the current node
		for _, leftChild := range root.leftChildren {
			helper(leftChild)
		}

		// check the current node
		// increment the current index if the current node has a value
		// this means that the current node does not represent a tombstone node so 
		if root.value != nil {
			currentIndex += 1
		}
		// check if current node is left origin
		if currentIndex == index - 1 && leftOrigin == nil{
			leftOrigin = root
		}
		// check if current node is right origin
		// remember that the right origin is the node after the left origin in the in order traversal order
		// not the next node in the in order traversal order with a value. Right origin can be a tombstone
		if currentIndex >= index - 1 && root != leftOrigin && rightOrigin == nil {
			rightOrigin = root
		}

		// iterate over the right children of the current node
		for _, rightChild := range root.rightChildren {
			helper(rightChild)
		}
	}
	helper(crdt.root)
	// check for inserts at a position that is greater than the current index + 1
	// at this point currentIndex should be the zero indexed location of the last value
	// in the crdt
	// TODO: check that every index should at least have a leftOrigin
	if index > currentIndex + 1 {
		err = fmt.Errorf("attempted to find origins at index %d but highest index in list crdt is %d", index, currentIndex)
	}
	return leftOrigin, rightOrigin, err
}

// use DFT with global state to find the node with that index
func (crdt *TextCRDT) findNodeByIndex(index int64) (foundNode *Node, err error) {
	var currentIndex int64 = -1 		// list of characters in representation is zero indexed
	var dftHelper func(*Node) 
	dftHelper = func(currentNode *Node) {
		if foundNode != nil {
			return
		}
		for _, leftChild := range currentNode.leftChildren {
			dftHelper(leftChild)
		}
		// increment current index if the current node has a value
		if currentNode.value != nil {
			currentIndex += 1
		}
		if currentIndex == index {
			foundNode = currentNode
			return
		}
		for _, rightChild := range currentNode.rightChildren {
			dftHelper(rightChild)
		}
	}
	dftHelper(crdt.root)
	if foundNode == nil {
		err = fmt.Errorf("unable for find node with index %d", index)
	}
	return foundNode, err
}

// use DFT to traverse the tree to the NodeID then return a pointer to that node
func (crdt *TextCRDT) findNodeByID(nodeID ID) (node *Node, err error) {
	var dfsHelper func(*Node)*Node
	dfsHelper = func(root *Node) (foundNode *Node) {
		if root.nodeID == nodeID {
			return root
		}
		for _, leftChild := range root.leftChildren {
			result := dfsHelper(leftChild)
			if result != nil {
				return result
			}
		}
		for _, rightChild := range root.rightChildren {
			result := dfsHelper(rightChild)
			if result != nil {
				return result
			}
		}
		return nil
	}
	node = dfsHelper(crdt.root)
	if node == nil {
		err = fmt.Errorf(
			"unable to find a node with replicaID %s and offset %d", 
			nodeID.replicaID, 
			nodeID.operationOffset,
		)
	}
	return node, err
}

func (crdt *TextCRDT) Representation() (values []interface{}) {
	var inOrderTraversalHelper func(*Node)
	inOrderTraversalHelper = func(currentNode *Node) {
		for _, leftChild := range currentNode.leftChildren {
			inOrderTraversalHelper(leftChild)
		}
		if currentNode.value != nil {
			values = append(values, currentNode.value)
		}
		for _, rightChild := range currentNode.rightChildren {
			inOrderTraversalHelper(rightChild)
		}
	}
	inOrderTraversalHelper(crdt.root)
	return values
}