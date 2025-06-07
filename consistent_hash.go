package consistenthashing

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
)

// Common errors
var (
	ErrInvalidVirtualReplicas = errors.New("virtualReplicas must be positive")
	ErrInvalidNodeID          = errors.New("node ID cannot be empty")
	ErrInvalidNodeHost        = errors.New("node host cannot be empty")
	ErrInvalidNodePort        = errors.New("node port must be positive")
	ErrInvalidCount           = errors.New("count must be positive")
	ErrNodeNotFound           = errors.New("node not found")
)

// HashFunction defines the interface for hash functions
type HashFunction interface {
	Hash(key string) uint64 // Changed to uint64 for better collision resistance
}

// FNVHasher implements HashFunction using FNV-1a (faster than SHA-256)
type FNVHasher struct{}

func (f *FNVHasher) Hash(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

// SHA256Hasher implements HashFunction using SHA-256 (more secure)
type SHA256Hasher struct{}

func (s *SHA256Hasher) Hash(key string) uint64 {
	h := sha256.Sum256([]byte(key))
	// Use full 64 bits instead of truncating to 32 bits
	return uint64(h[0])<<56 | uint64(h[1])<<48 | uint64(h[2])<<40 | uint64(h[3])<<32 |
		uint64(h[4])<<24 | uint64(h[5])<<16 | uint64(h[6])<<8 | uint64(h[7])
}

// Node represents a physical node in the distributed system
type Node struct {
	ID     string
	Host   string
	Port   int
	Weight int // Weight for weighted consistent hashing
}

// Validate checks if the node has valid parameters
func (n *Node) Validate() error {
	if strings.TrimSpace(n.ID) == "" {
		return ErrInvalidNodeID
	}
	if strings.TrimSpace(n.Host) == "" {
		return ErrInvalidNodeHost
	}
	if n.Port <= 0 || n.Port > 65535 {
		return ErrInvalidNodePort
	}
	return nil
}

// String returns a string representation of the node
func (n Node) String() string {
	return fmt.Sprintf("%s:%d", n.Host, n.Port)
}

// VirtualNode represents a virtual node on the hash ring
type VirtualNode struct {
	Hash uint64 // Changed to uint64
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
func NewHashRing(virtualReplicas int, opts ...Option) (*HashRing, error) {
	if virtualReplicas <= 0 {
		return nil, ErrInvalidVirtualReplicas
	}

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

	return hr, nil
}

// hash generates a hash value for the given key
func (hr *HashRing) hash(key string) uint64 {
	return hr.hasher.Hash(key)
}

// generateVirtualKey creates a more varied virtual node key to improve distribution
func (hr *HashRing) generateVirtualKey(nodeID string, index int) string {
	// Use a more complex pattern to reduce clustering for similar node IDs
	// Include the hash ring's virtual replica count as a seed for uniqueness
	return fmt.Sprintf("vnode:%s:replica:%d:seed:%d", nodeID, index, hr.virtualReplicas)
}

// AddNode adds a new node to the hash ring
func (hr *HashRing) AddNode(node *Node) error {
	if node == nil {
		return errors.New("node cannot be nil")
	}

	if err := node.Validate(); err != nil {
		return fmt.Errorf("invalid node: %w", err)
	}

	hr.mu.Lock()
	defer hr.mu.Unlock()

	if _, exists := hr.nodes[node.ID]; exists {
		return nil // Node already exists, not an error
	}

	hr.nodes[node.ID] = node

	// Calculate virtual replicas based on weight (default weight = 1)
	weight := node.Weight
	if weight <= 0 {
		weight = 1
	}
	virtualCount := hr.virtualReplicas * weight

	// Add virtual nodes with improved key generation
	newVirtualNodes := make([]VirtualNode, virtualCount)
	for i := 0; i < virtualCount; i++ {
		virtualKey := hr.generateVirtualKey(node.ID, i)
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

	return nil
}

// RemoveNode removes a node from the hash ring
func (hr *HashRing) RemoveNode(nodeID string) error {
	if strings.TrimSpace(nodeID) == "" {
		return ErrInvalidNodeID
	}

	hr.mu.Lock()
	defer hr.mu.Unlock()

	if _, exists := hr.nodes[nodeID]; !exists {
		return ErrNodeNotFound
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

	return nil
}

// GetNode returns the node responsible for the given key
func (hr *HashRing) GetNode(key string) (*Node, error) {
	if key == "" {
		return nil, errors.New("key cannot be empty")
	}

	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.virtualNodes) == 0 {
		return nil, errors.New("no nodes available in the ring")
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

	return hr.virtualNodes[idx].Node, nil
}

// GetNodes returns the N nodes responsible for the given key (for replication)
func (hr *HashRing) GetNodes(key string, count int) ([]*Node, error) {
	if key == "" {
		return nil, errors.New("key cannot be empty")
	}
	if count <= 0 {
		return nil, ErrInvalidCount
	}

	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.virtualNodes) == 0 {
		return nil, errors.New("no nodes available in the ring")
	}

	hash := hr.hash(key)
	nodes := make([]*Node, 0, count)

	// Optimize for small rings: use slice-based approach instead of map
	var seen map[string]bool
	var seenSlice []string

	if len(hr.nodes) <= 10 {
		// For small rings, use slice-based deduplication (more memory efficient)
		seenSlice = make([]string, 0, len(hr.nodes))
	} else {
		// For larger rings, use map-based deduplication (faster lookup)
		seen = make(map[string]bool, len(hr.nodes))
	}

	// Find starting position
	idx := sort.Search(len(hr.virtualNodes), func(i int) bool {
		return hr.virtualNodes[i].Hash >= hash
	})

	// Collect unique nodes
	uniqueNodesFound := 0
	for len(nodes) < count && uniqueNodesFound < len(hr.nodes) {
		if idx >= len(hr.virtualNodes) {
			idx = 0
		}

		node := hr.virtualNodes[idx].Node

		// Check if we've seen this node before
		var alreadySeen bool
		if seen != nil {
			alreadySeen = seen[node.ID]
			if !alreadySeen {
				seen[node.ID] = true
				uniqueNodesFound++
			}
		} else {
			// Linear search in slice (efficient for small slices)
			for _, seenID := range seenSlice {
				if seenID == node.ID {
					alreadySeen = true
					break
				}
			}
			if !alreadySeen {
				seenSlice = append(seenSlice, node.ID)
				uniqueNodesFound++
			}
		}

		if !alreadySeen {
			nodes = append(nodes, node)
		}
		idx++
	}

	return nodes, nil
}

// GetAllNodes returns all nodes in the ring, sorted by ID for deterministic results
func (hr *HashRing) GetAllNodes() []*Node {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	nodes := make([]*Node, 0, len(hr.nodes))
	for _, node := range hr.nodes {
		nodes = append(nodes, node)
	}

	// Sort by ID for deterministic results (useful for testing/debugging)
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})

	return nodes
}

// GetNodeByID returns a node by its ID
func (hr *HashRing) GetNodeByID(nodeID string) (*Node, error) {
	if strings.TrimSpace(nodeID) == "" {
		return nil, ErrInvalidNodeID
	}

	hr.mu.RLock()
	defer hr.mu.RUnlock()

	node, exists := hr.nodes[nodeID]
	if !exists {
		return nil, ErrNodeNotFound
	}

	return node, nil
}

// HasNode checks if a node exists in the ring
func (hr *HashRing) HasNode(nodeID string) bool {
	if strings.TrimSpace(nodeID) == "" {
		return false
	}

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
func (hr *HashRing) GetLoadDistribution(keys []string) (map[string]int, error) {
	if keys == nil {
		return nil, errors.New("keys slice cannot be nil")
	}

	distribution := make(map[string]int)

	for _, key := range keys {
		if key == "" {
			continue // Skip empty keys
		}
		node, err := hr.GetNode(key)
		if err != nil {
			return nil, fmt.Errorf("failed to get node for key %s: %w", key, err)
		}
		if node != nil {
			distribution[node.ID]++
		}
	}

	return distribution, nil
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

	// Add hash function type
	switch hr.hasher.(type) {
	case *FNVHasher:
		info["hash_function"] = "FNV-1a"
	case *SHA256Hasher:
		info["hash_function"] = "SHA-256"
	default:
		info["hash_function"] = "Custom"
	}

	return info
}

// ValidateRing performs a comprehensive validation of the ring state
func (hr *HashRing) ValidateRing() error {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	// Check if virtual nodes are properly sorted
	for i := 1; i < len(hr.virtualNodes); i++ {
		if hr.virtualNodes[i-1].Hash > hr.virtualNodes[i].Hash {
			return errors.New("virtual nodes are not properly sorted")
		}
	}

	// Check if all virtual nodes point to valid physical nodes
	for _, vnode := range hr.virtualNodes {
		if vnode.Node == nil {
			return errors.New("virtual node points to nil physical node")
		}
		if _, exists := hr.nodes[vnode.Node.ID]; !exists {
			return fmt.Errorf("virtual node points to non-existent physical node: %s", vnode.Node.ID)
		}
	}

	return nil
}
