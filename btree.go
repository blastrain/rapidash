package rapidash

import (
	"fmt"
	"strings"
)

const (
	Order     = 4
	BranchNum = Order
	KeyNum    = Order - 1
)

type BTree struct {
	root *Node
}

type Leaf interface {
}

func NewBTree() *BTree {
	return &BTree{
		root: NewNode(),
	}
}

func (t *BTree) searchEq(key *Value) Leaf {
	if t == nil {
		return nil
	}
	return t.root.searchEq(key)
}

func (t *BTree) searchLt(key *Value) []Leaf {
	if t == nil {
		return nil
	}
	return t.root.searchLt(key)
}

func (t *BTree) searchGt(key *Value) []Leaf {
	if t == nil {
		return nil
	}
	return t.root.searchGt(key)
}

func (t *BTree) searchLte(key *Value) []Leaf {
	if t == nil {
		return nil
	}
	return t.root.searchLte(key)
}

func (t *BTree) searchGte(key *Value) []Leaf {
	if t == nil {
		return nil
	}
	return t.root.searchGte(key)
}

func (t *BTree) all() []Leaf {
	values := []Leaf{}
	node := t.leftEdgeLeaf(t.root)
	values = append(values, node.leafs...)
	for node.next != nil {
		node = node.next
		values = append(values, node.leafs...)
	}
	return values
}

func (t *BTree) leftEdgeLeaf(node *Node) *Node {
	if node.isLeaf || len(node.branches) == 0 {
		return node
	}
	return t.leftEdgeLeaf(node.branches[0])
}

func (t *BTree) insert(key *Value, value Leaf) {
	t.root.add(key, value)
}

func (t *BTree) dump() {
	t.root.dump(1)
}

type Node struct {
	keys     []*Value
	leafs    []Leaf
	branches []*Node
	parent   *Node
	next     *Node
	prev     *Node
	isLeaf   bool
}

func (n *Node) dump(depth int) {
	link := ""
	if n.next != nil {
		keys := []string{}
		for _, key := range n.next.keys {
			keys = append(keys, key.String())
		}
		link = fmt.Sprintf("(-> %s)", strings.Join(keys, ","))
	}
	keys := []string{}
	for _, key := range n.keys {
		keys = append(keys, key.String())
	}
	leafs := []string{}
	for _, leaf := range n.leafs {
		v, ok := leaf.(*StructSliceValue)
		if ok {
			leafs = append(leafs, v.EncodeLog())
		} else {
			leafs = append(leafs, fmt.Sprintf("%p", v))
		}
	}
	fmt.Printf("%s %s %v %s\n",
		strings.Repeat("-", depth),
		strings.Join(keys, ","),
		strings.Join(leafs, ","), link)
	for i := 0; i < n.branchNum(); i++ {
		n.branches[i].dump(depth + 1)
	}
}

func (n *Node) keyNum() int {
	return len(n.keys)
}

func (n *Node) branchNum() int {
	return len(n.branches)
}

func (n *Node) leafNum() int {
	return len(n.leafs)
}

func (n *Node) isWithoutBranchAndLeaf() bool {
	if n.branchNum() == 0 && n.leafNum() == 0 {
		return true
	}
	return false
}

func (n *Node) addKey(key *Value, leaf Leaf) {
	found := false
	for _, k := range n.keys {
		if k.EQ(key) {
			found = true
			break
		}
	}
	if !found {
		n.keys = append(n.keys, key)
	}
	if leaf == nil {
		return
	}
	n.leafs = append(n.leafs, leaf)
	n.isLeaf = true
}

func (n *Node) addBranch(node *Node) {
	node.parent = n
	n.branches = append(n.branches, node)
}

func (n *Node) firstKey() *Value {
	return n.keys[0]
}

func (n *Node) lastKey() *Value {
	return n.keys[len(n.keys)-1]
}

func (n *Node) createLeftNode(centerIndex int) *Node {
	node := NewNode()
	centerKey := n.keys[centerIndex]
	for i := 0; i < centerIndex; i++ {
		if len(n.leafs) > i {
			node.addKey(n.keys[i], n.leafs[i])
		} else {
			node.addKey(n.keys[i], nil)
		}
	}
	for i := 0; i < n.branchNum(); i++ {
		if n.branches[i].lastKey().LT(centerKey) {
			node.addBranch(n.branches[i])
		}
	}
	return node
}

func (n *Node) createRightNode(centerIndex int) *Node {
	node := NewNode()
	centerKey := n.keys[centerIndex]
	for i := centerIndex + 1; i < n.keyNum(); i++ {
		if len(n.leafs) > i {
			node.addKey(n.keys[i], n.leafs[i])
		} else {
			node.addKey(n.keys[i], nil)
		}
	}
	for i := 0; i < n.branchNum(); i++ {
		if n.branches[i].firstKey().GTE(centerKey) {
			node.addBranch(n.branches[i])
		}
	}
	return node
}

func (n *Node) removeChild(child *Node) {
	for i := 0; i < n.branchNum(); i++ {
		if n.branches[i] == child {
			n.branches = append(n.branches[:i], n.branches[i+1:]...)
			break
		}
	}
}

func (n *Node) searchEq(key *Value) Leaf {
	if n.isLeaf {
		for idx, k := range n.keys {
			if k.EQ(key) {
				return n.leafs[idx]
			}
		}
		return nil
	}
	for idx, k := range n.keys {
		if k.GT(key) {
			return n.branches[idx].searchEq(key)
		}
	}
	return n.branches[n.branchNum()-1].searchEq(key)
}

func (n *Node) searchLt(key *Value) []Leaf {
	if n.isLeaf {
		leafs := []Leaf{}
		for idx, k := range n.keys {
			if k.LT(key) {
				leafs = append(leafs, n.leafs[idx])
			}
		}
		traversePtr := n
		for traversePtr.prev != nil {
			node := traversePtr.prev
			leafs = append(leafs, node.leafs...)
			traversePtr = node
		}
		return leafs
	}
	for idx, k := range n.keys {
		if k.GT(key) {
			return n.branches[idx].searchLt(key)
		}
	}
	return n.branches[n.branchNum()-1].searchLt(key)
}

func (n *Node) searchGt(key *Value) []Leaf {
	if n.isLeaf {
		leafs := []Leaf{}
		for idx, k := range n.keys {
			if k.GT(key) {
				leafs = append(leafs, n.leafs[idx])
			}
		}
		traversePtr := n
		for traversePtr.next != nil {
			node := traversePtr.next
			leafs = append(leafs, node.leafs...)
			traversePtr = node
		}
		return leafs
	}
	for idx, k := range n.keys {
		if key.LTE(k) {
			return n.branches[idx].searchGt(key)
		}
	}
	return n.branches[n.branchNum()-1].searchGt(key)
}

func (n *Node) searchLte(key *Value) []Leaf {
	if n.isLeaf {
		leafs := []Leaf{}
		for idx, k := range n.keys {
			if k.LTE(key) {
				leafs = append(leafs, n.leafs[idx])
			}
		}
		traversePtr := n
		for traversePtr.prev != nil {
			node := traversePtr.prev
			leafs = append(leafs, node.leafs...)
			traversePtr = node
		}
		return leafs
	}
	for idx, k := range n.keys {
		if k.GT(key) {
			return n.branches[idx].searchLte(key)
		}
	}
	return n.branches[n.branchNum()-1].searchLte(key)
}

func (n *Node) searchGte(key *Value) []Leaf {
	if n.isLeaf {
		leafs := []Leaf{}
		for idx, k := range n.keys {
			if k.GTE(key) {
				leafs = append(leafs, n.leafs[idx])
			}
		}
		traversePtr := n
		for traversePtr.next != nil {
			node := traversePtr.next
			leafs = append(leafs, node.leafs...)
			traversePtr = node
		}
		return leafs
	}
	for idx, k := range n.keys {
		if key.LTE(k) {
			return n.branches[idx].searchGte(key)
		}
	}
	return n.branches[n.branchNum()-1].searchGte(key)
}

func (n *Node) balance() {
	if n.keyNum() <= KeyNum {
		return
	}
	if n.parent != nil {
		centerIndex := n.keyNum() / 2
		leftNode := n.createLeftNode(centerIndex)
		rightNode := n.createRightNode(centerIndex)
		n.parent.removeChild(n)
		n.parent.addBranch(leftNode)
		n.parent.addBranch(rightNode)
		n.parent.addKey(n.keys[centerIndex], nil)
		n.parent.leafs = []Leaf{}
		n.parent.isLeaf = false
		if n.parent != nil {
			n.parent.balance()
		}
	} else {
		centerIndex := n.keyNum() / 2
		leftNode := n.createLeftNode(centerIndex)
		rightNode := n.createRightNode(centerIndex)
		centerKey := n.keys[centerIndex]
		n.branches = []*Node{}
		n.keys = []*Value{}
		n.leafs = []Leaf{}
		n.isLeaf = false
		n.addBranch(leftNode)
		n.addBranch(rightNode)
		n.addKey(centerKey, nil)
	}
}

func (n *Node) add(key *Value, leaf Leaf) {
	if n.branchNum() > 0 {
		n.branches[len(n.branches)-1].add(key, leaf)
	} else if KeyNum > n.keyNum() {
		n.addKey(key, leaf)
	} else if n.parent != nil {
		centerIndex := (n.keyNum() + 1) / 2
		centerKey := n.keys[centerIndex]
		n.parent.addKey(centerKey, nil)
		n.parent.leafs = []Leaf{}
		n.parent.isLeaf = false
		keys := []*Value{}
		leafs := []Leaf{}
		for i := 0; i < centerIndex; i++ {
			keys = append(keys, n.keys[i])
			leafs = append(leafs, n.leafs[i])
		}
		rightNode := n.createRightNode(centerIndex - 1)
		rightNode.addKey(key, leaf)
		n.next = rightNode
		rightNode.prev = n
		n.parent.addBranch(rightNode)
		n.keys = keys
		n.leafs = leafs
		n.isLeaf = len(leafs) > 0
		n.parent.balance()
	} else {
		centerIndex := (n.keyNum() + 1) / 2
		centerKey := n.keys[centerIndex]
		leftNode := n.createLeftNode(centerIndex)
		rightNode := n.createRightNode(centerIndex - 1)
		rightNode.addKey(key, leaf)
		leftNode.next = rightNode
		rightNode.prev = leftNode
		n.addBranch(leftNode)
		n.addBranch(rightNode)
		n.keys = []*Value{}
		n.leafs = []Leaf{}
		n.isLeaf = false
		n.addKey(centerKey, nil)
	}
}

func NewNode() *Node {
	return &Node{
		keys:     []*Value{},
		leafs:    []Leaf{},
		branches: []*Node{},
	}
}
