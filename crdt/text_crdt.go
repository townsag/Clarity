package crdt

import (
	"fmt"
)

type TextCRDT struct {
	replicaID 		string
	root 			*Node
	versionVector	*VersionVector
}

func NewTextCRDT(replicaID string) *TextCRDT {
	return &TextCRDT{
		replicaID: replicaID,
		root: NewNode(
			ID{replicaID: "root", operationOffset: 0},
			nil,
		),
		versionVector: NewVersionVector(replicaID),
	}
}


// Design Choice:
//	- write two different insert functions
//		- one for receiving an operation from another replica and applying that operation
//		- one for inserting values originating at this replica
// TODO: test that this handles the case where there is no right origin
func (crdt *TextCRDT) Apply(operation Operation) {
	switch operation.Type() {
	case Insert:
		insertOp := operation.(*InsertOperation)
		parentNode, err := crdt.findNodeByID(insertOp.parentNodeID)
		if err != nil {
			panic(err)
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
			panic(err)
		}
		// TODO: we should be checking here if the value has already been deleted, so something with the error
		toDelete.value = nil
	}
}

func (crdt *TextCRDT) LocalInsert(index int64, value interface{}) (*InsertOperation) {
	var leftOrigin, rightOrigin *Node
	var err error
	var newOperationOffset int64
	var parentNodeID ID
	var side side
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
	return NewInsertOperation(newNodeID, value, parentNodeID, side)	
}

func (crdt *TextCRDT) LocalDelete(index int64) (*DeleteOperation) {
	nodeToDelete, err := crdt.findNodeByIndex(index)
	if err != nil {
		panic(err)
	}
	nodeToDelete.value = nil
	return NewDeleteOperation(nodeToDelete.nodeID)
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