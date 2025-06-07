package consistenthashing

import (
	"crypto/sha256"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

// HashFunction defines the interface for hash functions
type HashFunction interface {
	Hash(key string) uint32
}

// FNVHasher implements HashFunction using FNV-1a (faster than SHA-256)
type FNVHasher struct{}

func (f *FNVHasher) Hash(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

// SHA256Hasher implements HashFunction using SHA-256 (more secure)
type SHA256Hasher struct{}

func (s *SHA256Hasher) Hash(key string) uint32 {
	h := sha256.Sum256([]byte(key))
	return uint32(h[0])<<24 | uint32(h[1])<<16 | uint32(h[2])<<8 | uint32(h[3])
}

// Node represents a physical node in the distributed system
type Node struct {
	ID     string
	Host   string
	Port   int
	Weight int // Weight for weighted consistent hashing
}

// String returns a string representation of the node
func (n Node) String() string {
	return fmt.Sprintf("%s:%d", n.Host, n.Port)
}

// VirtualNode represents a virtual node on the hash ring
type VirtualNode struct {
	Hash uint32
	Node *Node
}

// HashRing represents the consistent hash ring
type HashRing struct {
	virtualNodes    []VirtualNode
	nodes           map[string]*Node
	virtualReplicas int
	hasher          HashFunction
	mu              sync.RWMutex // Thread safety
}

// Option defines configuration options for HashRing
type Option func(*HashRing)

// WithHashFunction sets a custom hash function
func WithHashFunction(hasher HashFunction) Option {
	return func(hr *HashRing) {
		hr.hasher = hasher
	}
}

// NewHashRing creates a new hash ring with the specified number of virtual replicas per node
func NewHashRing(virtualReplicas int, opts ...Option) *HashRing {
	hr := &HashRing{
		virtualNodes:    make([]VirtualNode, 0),
		nodes:           make(map[string]*Node),
		virtualReplicas: virtualReplicas,
		hasher:          &FNVHasher{}, // Default to faster FNV hash
	}
	
	// Apply options
	for _, opt := range opts {
		opt(hr)
	}
	
	return hr
}

// hash generates a hash value for the given key
func (hr *HashRing) hash(key string) uint32 {
	return hr.hasher.Hash(key)
}

// AddNode adds a new node to the hash ring
func (hr *HashRing) AddNode(node *Node) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	
	if _, exists := hr.nodes[node.ID]; exists {
		return // Node already exists
	}

	hr.nodes[node.ID] = node

	// Calculate virtual replicas based on weight (default weight = 1)
	weight := node.Weight
	if weight <= 0 {
		weight = 1
	}
	virtualCount := hr.virtualReplicas * weight

	// Add virtual nodes
	newVirtualNodes := make([]VirtualNode, virtualCount)
	for i := 0; i < virtualCount; i++ {
		virtualKey := fmt.Sprintf("%s:%d", node.ID, i)
		hash := hr.hash(virtualKey)
		newVirtualNodes[i] = VirtualNode{
			Hash: hash,
			Node: node,
		}
	}

	// Merge and sort - more efficient than appending and sorting entire slice
	hr.virtualNodes = append(hr.virtualNodes, newVirtualNodes...)
	sort.Slice(hr.virtualNodes, func(i, j int) bool {
		return hr.virtualNodes[i].Hash < hr.virtualNodes[j].Hash
	})
}

// RemoveNode removes a node from the hash ring
func (hr *HashRing) RemoveNode(nodeID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	
	if _, exists := hr.nodes[nodeID]; !exists {
		return // Node doesn't exist
	}

	delete(hr.nodes, nodeID)

	// Remove virtual nodes in-place for better performance
	writeIndex := 0
	for readIndex := 0; readIndex < len(hr.virtualNodes); readIndex++ {
		if hr.virtualNodes[readIndex].Node.ID != nodeID {
			hr.virtualNodes[writeIndex] = hr.virtualNodes[readIndex]
			writeIndex++
		}
	}
	hr.virtualNodes = hr.virtualNodes[:writeIndex]
}

// GetNode returns the node responsible for the given key
func (hr *HashRing) GetNode(key string) *Node {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	
	if len(hr.virtualNodes) == 0 {
		return nil
	}

	hash := hr.hash(key)

	// Binary search for the first virtual node with hash >= key hash
	idx := sort.Search(len(hr.virtualNodes), func(i int) bool {
		return hr.virtualNodes[i].Hash >= hash
	})

	// If no node found, wrap around to the first node
	if idx == len(hr.virtualNodes) {
		idx = 0
	}

	return hr.virtualNodes[idx].Node
}

// GetNodes returns the N nodes responsible for the given key (for replication)
func (hr *HashRing) GetNodes(key string, count int) []*Node {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	
	if len(hr.virtualNodes) == 0 || count <= 0 {
		return nil
	}

	hash := hr.hash(key)
	nodes := make([]*Node, 0, count)
	seen := make(map[string]bool)

	// Find starting position
	idx := sort.Search(len(hr.virtualNodes), func(i int) bool {
		return hr.virtualNodes[i].Hash >= hash
	})

	// Collect unique nodes
	for len(nodes) < count && len(seen) < len(hr.nodes) {
		if idx >= len(hr.virtualNodes) {
			idx = 0
		}

		node := hr.virtualNodes[idx].Node
		if !seen[node.ID] {
			nodes = append(nodes, node)
			seen[node.ID] = true
		}
		idx++
	}

	return nodes
}

// GetAllNodes returns all nodes in the ring
func (hr *HashRing) GetAllNodes() []*Node {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	
	nodes := make([]*Node, 0, len(hr.nodes))
	for _, node := range hr.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

// GetNodeByID returns a node by its ID
func (hr *HashRing) GetNodeByID(nodeID string) *Node {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	
	return hr.nodes[nodeID]
}

// HasNode checks if a node exists in the ring
func (hr *HashRing) HasNode(nodeID string) bool {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	
	_, exists := hr.nodes[nodeID]
	return exists
}

// Size returns the number of physical nodes in the ring
func (hr *HashRing) Size() int {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	
	return len(hr.nodes)
}

// VirtualSize returns the number of virtual nodes in the ring
func (hr *HashRing) VirtualSize() int {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	
	return len(hr.virtualNodes)
}

// GetLoadDistribution returns the distribution of keys across nodes
func (hr *HashRing) GetLoadDistribution(keys []string) map[string]int {
	distribution := make(map[string]int)
	
	for _, key := range keys {
		node := hr.GetNode(key)
		if node != nil {
			distribution[node.ID]++
		}
	}
	
	return distribution
}

// GetRingInfo returns detailed information about the ring
func (hr *HashRing) GetRingInfo() map[string]interface{} {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	
	info := make(map[string]interface{})
	info["physical_nodes"] = len(hr.nodes)
	info["virtual_nodes"] = len(hr.virtualNodes)
	info["virtual_replicas"] = hr.virtualReplicas
	
	// Calculate average virtual nodes per physical node
	if len(hr.nodes) > 0 {
		info["avg_virtual_per_physical"] = float64(len(hr.virtualNodes)) / float64(len(hr.nodes))
	}
	
	return info
} 