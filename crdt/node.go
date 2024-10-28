package crdt

type Node struct {
	nodeID 			ID
	// parentNodeID 	*ID
	// side 			*side
	value 			interface{}
	leftChildren	[]*Node
	rightChildren	[]*Node
}

func NewNode(nodeID ID, value interface{}) (n *Node) {
	return &Node{
		nodeID: nodeID,
		value: value,
		leftChildren: make([]*Node, 0),
		rightChildren: make([]*Node, 0),
	}
}

func (n *Node) hasRightChild() bool {
	return len(n.rightChildren) > 0
}

func (n *Node) insertRightChild(childNode *Node) {
	// linearly traverse the slice until we find the correct index for 
	// the new node in lexicographical order by replicaID
	// index is the position in the list of children of the first node for which the 
	// replicaID is less than the replicaID of the node to be inserted
	index := 0
	for index < len(n.rightChildren) && childNode.nodeID.replicaID > n.rightChildren[index].nodeID.replicaID {
		index += 1
	}
	n.rightChildren = append(
		n.rightChildren[:index], 
		append([]*Node{childNode}, n.rightChildren[index:]...)...
	)
}

func (n *Node) insertLeftChild(childNode *Node) {
	index := 0
	for index < len(n.leftChildren) && childNode.nodeID.replicaID > n.leftChildren[index].nodeID.replicaID {
		index += 1
	}
	n.leftChildren = append(
		n.leftChildren[:index], 
		append([]*Node{childNode}, n.leftChildren[index:]...)...
	)
}