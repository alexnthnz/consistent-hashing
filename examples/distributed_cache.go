package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/alexnthnz/consistent-hashing"
)

// CacheNode represents a cache server with basic operations
type CacheNode struct {
	*consistenthashing.Node
	data map[string]string
}

// NewCacheNode creates a new cache node
func NewCacheNode(id, host string, port int) *CacheNode {
	return &CacheNode{
		Node: &consistenthashing.Node{
			ID:   id,
			Host: host,
			Port: port,
		},
		data: make(map[string]string),
	}
}

// Set stores a key-value pair in this cache node
func (cn *CacheNode) Set(key, value string) {
	cn.data[key] = value
	fmt.Printf("[%s] SET %s = %s\n", cn.ID, key, value)
}

// Get retrieves a value from this cache node
func (cn *CacheNode) Get(key string) (string, bool) {
	value, exists := cn.data[key]
	if exists {
		fmt.Printf("[%s] GET %s = %s\n", cn.ID, key, value)
	} else {
		fmt.Printf("[%s] GET %s = <not found>\n", cn.ID, key)
	}
	return value, exists
}

// Size returns the number of items in this cache node
func (cn *CacheNode) Size() int {
	return len(cn.data)
}

// DistributedCache represents a distributed cache using consistent hashing
type DistributedCache struct {
	ring       *consistenthashing.HashRing
	cacheNodes map[string]*CacheNode
}

// NewDistributedCache creates a new distributed cache
func NewDistributedCache(virtualReplicas int) *DistributedCache {
	return &DistributedCache{
		ring:       consistenthashing.NewHashRing(virtualReplicas),
		cacheNodes: make(map[string]*CacheNode),
	}
}

// AddCacheNode adds a cache node to the distributed cache
func (dc *DistributedCache) AddCacheNode(cacheNode *CacheNode) {
	dc.ring.AddNode(cacheNode.Node)
	dc.cacheNodes[cacheNode.ID] = cacheNode
	fmt.Printf("Added cache node: %s (%s)\n", cacheNode.ID, cacheNode.String())
}

// RemoveCacheNode removes a cache node from the distributed cache
func (dc *DistributedCache) RemoveCacheNode(nodeID string) {
	if cacheNode, exists := dc.cacheNodes[nodeID]; exists {
		// In a real implementation, you'd want to migrate data to other nodes
		fmt.Printf("Removing cache node: %s (had %d items)\n", nodeID, cacheNode.Size())
		dc.ring.RemoveNode(nodeID)
		delete(dc.cacheNodes, nodeID)
	}
}

// Set stores a key-value pair in the appropriate cache node
func (dc *DistributedCache) Set(key, value string) {
	node := dc.ring.GetNode(key)
	if node != nil {
		if cacheNode, exists := dc.cacheNodes[node.ID]; exists {
			cacheNode.Set(key, value)
		}
	}
}

// Get retrieves a value from the appropriate cache node
func (dc *DistributedCache) Get(key string) (string, bool) {
	node := dc.ring.GetNode(key)
	if node != nil {
		if cacheNode, exists := dc.cacheNodes[node.ID]; exists {
			return cacheNode.Get(key)
		}
	}
	return "", false
}

// SetWithReplication stores a key-value pair with replication
func (dc *DistributedCache) SetWithReplication(key, value string, replicas int) {
	nodes := dc.ring.GetNodes(key, replicas)
	fmt.Printf("Storing '%s' with %d replicas across nodes: ", key, len(nodes))
	for i, node := range nodes {
		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Print(node.ID)
		if cacheNode, exists := dc.cacheNodes[node.ID]; exists {
			cacheNode.Set(key, value)
		}
	}
	fmt.Println()
}

// GetStats returns statistics about the distributed cache
func (dc *DistributedCache) GetStats() {
	fmt.Println("\n=== Distributed Cache Statistics ===")
	fmt.Printf("Total cache nodes: %d\n", len(dc.cacheNodes))
	fmt.Printf("Virtual nodes: %d\n", dc.ring.VirtualSize())
	
	totalItems := 0
	for nodeID, cacheNode := range dc.cacheNodes {
		itemCount := cacheNode.Size()
		totalItems += itemCount
		fmt.Printf("Node %s: %d items\n", nodeID, itemCount)
	}
	fmt.Printf("Total items: %d\n", totalItems)
	
	if len(dc.cacheNodes) > 0 {
		avgItems := float64(totalItems) / float64(len(dc.cacheNodes))
		fmt.Printf("Average items per node: %.1f\n", avgItems)
	}
}

func main() {
	fmt.Println("=== Distributed Cache with Consistent Hashing ===\n")
	
	// Create distributed cache with 50 virtual nodes per physical node
	cache := NewDistributedCache(50)
	
	// Create cache nodes
	cacheNodes := []*CacheNode{
		NewCacheNode("cache1", "192.168.1.10", 6379),
		NewCacheNode("cache2", "192.168.1.11", 6379),
		NewCacheNode("cache3", "192.168.1.12", 6379),
	}
	
	// Add cache nodes
	fmt.Println("Setting up cache cluster:")
	for _, node := range cacheNodes {
		cache.AddCacheNode(node)
	}
	fmt.Println()
	
	// Simulate cache operations
	fmt.Println("Performing cache operations:")
	testData := map[string]string{
		"user:1001":    "John Doe",
		"user:1002":    "Jane Smith",
		"user:1003":    "Bob Johnson",
		"session:abc":  "active",
		"session:def":  "expired",
		"config:db":    "localhost:5432",
		"config:redis": "localhost:6379",
		"product:123":  "Laptop",
		"product:456":  "Mouse",
		"product:789":  "Keyboard",
	}
	
	// Store data
	for key, value := range testData {
		cache.Set(key, value)
	}
	fmt.Println()
	
	// Retrieve data
	fmt.Println("Retrieving data:")
	for key := range testData {
		cache.Get(key)
	}
	fmt.Println()
	
	// Show initial stats
	cache.GetStats()
	
	// Demonstrate replication
	fmt.Println("\n=== Replication Example ===")
	cache.SetWithReplication("critical:data", "important_value", 2)
	fmt.Println()
	
	// Simulate node failure and recovery
	fmt.Println("=== Simulating Node Failure ===")
	cache.RemoveCacheNode("cache2")
	cache.GetStats()
	
	fmt.Println("\nTrying to access data after node failure:")
	// Some data might be lost (in cache2), but most should still be accessible
	testKeys := []string{"user:1001", "session:abc", "product:123"}
	for _, key := range testKeys {
		cache.Get(key)
	}
	
	// Add a new node (simulating recovery or scaling)
	fmt.Println("\n=== Adding New Cache Node ===")
	newNode := NewCacheNode("cache4", "192.168.1.14", 6379)
	cache.AddCacheNode(newNode)
	
	// In a real system, you'd redistribute some data to the new node
	fmt.Println("Redistributing some data to new node...")
	redistributeData := map[string]string{
		"user:2001":    "Alice Brown",
		"user:2002":    "Charlie Wilson",
		"session:xyz":  "active",
		"product:999":  "Monitor",
	}
	
	for key, value := range redistributeData {
		cache.Set(key, value)
	}
	
	cache.GetStats()
	
	// Demonstrate load balancing
	fmt.Println("\n=== Load Balancing Analysis ===")
	simulateLoadBalancing(cache)
}

func simulateLoadBalancing(cache *DistributedCache) {
	// Generate random keys and see how they distribute
	rand.Seed(time.Now().UnixNano())
	keyCount := 1000
	
	fmt.Printf("Distributing %d random keys...\n", keyCount)
	
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("random_key_%d", rand.Intn(10000))
		value := fmt.Sprintf("value_%d", i)
		cache.Set(key, value)
	}
	
	fmt.Println("\nFinal distribution:")
	cache.GetStats()
	
	// Calculate distribution balance
	distribution := make(map[string]int)
	for nodeID, cacheNode := range cache.cacheNodes {
		distribution[nodeID] = cacheNode.Size()
	}
	
	// Find min and max
	var min, max int
	first := true
	for _, count := range distribution {
		if first {
			min, max = count, count
			first = false
		} else {
			if count < min {
				min = count
			}
			if count > max {
				max = count
			}
		}
	}
	
	if max > 0 {
		balance := float64(min) / float64(max) * 100
		fmt.Printf("\nLoad balance ratio: %.1f%% (100%% = perfect balance)\n", balance)
		fmt.Printf("Range: %d - %d items per node\n", min, max)
	}
} 