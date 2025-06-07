package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/alexnthnz/consistent-hashing"
)

// CacheNode represents a cache server node
type CacheNode struct {
	*consistenthashing.Node
	data   map[string]string
	mutex  sync.RWMutex
	active bool
}

// NewCacheNode creates a new cache node
func NewCacheNode(id, host string, port int) *CacheNode {
	return &CacheNode{
		Node: &consistenthashing.Node{
			ID:   id,
			Host: host,
			Port: port,
		},
		data:   make(map[string]string),
		active: true,
	}
}

// Set stores a key-value pair in the cache node
func (cn *CacheNode) Set(key, value string) error {
	if !cn.active {
		return fmt.Errorf("node %s is not active", cn.ID)
	}
	
	cn.mutex.Lock()
	defer cn.mutex.Unlock()
	cn.data[key] = value
	return nil
}

// Get retrieves a value from the cache node
func (cn *CacheNode) Get(key string) (string, error) {
	if !cn.active {
		return "", fmt.Errorf("node %s is not active", cn.ID)
	}
	
	cn.mutex.RLock()
	defer cn.mutex.RUnlock()
	value, exists := cn.data[key]
	if !exists {
		return "", fmt.Errorf("key %s not found in node %s", key, cn.ID)
	}
	return value, nil
}

// Delete removes a key from the cache node
func (cn *CacheNode) Delete(key string) error {
	if !cn.active {
		return fmt.Errorf("node %s is not active", cn.ID)
	}
	
	cn.mutex.Lock()
	defer cn.mutex.Unlock()
	delete(cn.data, key)
	return nil
}

// Size returns the number of items in the cache node
func (cn *CacheNode) Size() int {
	cn.mutex.RLock()
	defer cn.mutex.RUnlock()
	return len(cn.data)
}

// SetActive sets the active status of the node
func (cn *CacheNode) SetActive(active bool) {
	cn.mutex.Lock()
	defer cn.mutex.Unlock()
	cn.active = active
}

// IsActive returns whether the node is active
func (cn *CacheNode) IsActive() bool {
	cn.mutex.RLock()
	defer cn.mutex.RUnlock()
	return cn.active
}

// DistributedCache represents a distributed cache system
type DistributedCache struct {
	ring         *consistenthashing.HashRing
	nodes        map[string]*CacheNode
	replication  int
	mutex        sync.RWMutex
}

// NewDistributedCache creates a new distributed cache
func NewDistributedCache(virtualReplicas, replication int) (*DistributedCache, error) {
	if replication <= 0 {
		return nil, fmt.Errorf("replication factor must be positive")
	}
	
	ring, err := consistenthashing.NewHashRing(virtualReplicas)
	if err != nil {
		return nil, fmt.Errorf("failed to create hash ring: %w", err)
	}
	
	return &DistributedCache{
		ring:        ring,
		nodes:       make(map[string]*CacheNode),
		replication: replication,
	}, nil
}

// AddNode adds a cache node to the distributed cache
func (dc *DistributedCache) AddNode(node *CacheNode) error {
	dc.mutex.Lock()
	defer dc.mutex.Unlock()
	
	if err := dc.ring.AddNode(node.Node); err != nil {
		return fmt.Errorf("failed to add node to ring: %w", err)
	}
	
	dc.nodes[node.ID] = node
	return nil
}

// RemoveNode removes a cache node from the distributed cache
func (dc *DistributedCache) RemoveNode(nodeID string) error {
	dc.mutex.Lock()
	defer dc.mutex.Unlock()
	
	if err := dc.ring.RemoveNode(nodeID); err != nil {
		return fmt.Errorf("failed to remove node from ring: %w", err)
	}
	
	if node, exists := dc.nodes[nodeID]; exists {
		node.SetActive(false)
		delete(dc.nodes, nodeID)
	}
	
	return nil
}

// Set stores a key-value pair with replication
func (dc *DistributedCache) Set(key, value string) error {
	dc.mutex.RLock()
	defer dc.mutex.RUnlock()
	
	nodes, err := dc.ring.GetNodes(key, dc.replication)
	if err != nil {
		return fmt.Errorf("failed to get nodes for key %s: %w", key, err)
	}
	
	var errors []error
	successCount := 0
	
	for _, node := range nodes {
		if cacheNode, exists := dc.nodes[node.ID]; exists && cacheNode.IsActive() {
			if err := cacheNode.Set(key, value); err != nil {
				errors = append(errors, fmt.Errorf("failed to set on node %s: %w", node.ID, err))
			} else {
				successCount++
			}
		}
	}
	
	if successCount == 0 {
		return fmt.Errorf("failed to set key %s on any node: %v", key, errors)
	}
	
	return nil
}

// Get retrieves a value with fallback to replicas
func (dc *DistributedCache) Get(key string) (string, error) {
	dc.mutex.RLock()
	defer dc.mutex.RUnlock()
	
	nodes, err := dc.ring.GetNodes(key, dc.replication)
	if err != nil {
		return "", fmt.Errorf("failed to get nodes for key %s: %w", key, err)
	}
	
	var lastError error
	
	for _, node := range nodes {
		if cacheNode, exists := dc.nodes[node.ID]; exists && cacheNode.IsActive() {
			value, err := cacheNode.Get(key)
			if err == nil {
				return value, nil
			}
			lastError = err
		}
	}
	
	if lastError != nil {
		return "", fmt.Errorf("key %s not found in any replica: %w", key, lastError)
	}
	
	return "", fmt.Errorf("no active nodes available for key %s", key)
}

// Delete removes a key from all replicas
func (dc *DistributedCache) Delete(key string) error {
	dc.mutex.RLock()
	defer dc.mutex.RUnlock()
	
	nodes, err := dc.ring.GetNodes(key, dc.replication)
	if err != nil {
		return fmt.Errorf("failed to get nodes for key %s: %w", key, err)
	}
	
	var errors []error
	
	for _, node := range nodes {
		if cacheNode, exists := dc.nodes[node.ID]; exists && cacheNode.IsActive() {
			if err := cacheNode.Delete(key); err != nil {
				errors = append(errors, fmt.Errorf("failed to delete from node %s: %w", node.ID, err))
			}
		}
	}
	
	if len(errors) > 0 {
		return fmt.Errorf("errors during deletion: %v", errors)
	}
	
	return nil
}

// GetStats returns cache statistics
func (dc *DistributedCache) GetStats() map[string]interface{} {
	dc.mutex.RLock()
	defer dc.mutex.RUnlock()
	
	stats := make(map[string]interface{})
	stats["total_nodes"] = len(dc.nodes)
	stats["replication_factor"] = dc.replication
	
	activeNodes := 0
	totalItems := 0
	nodeStats := make(map[string]int)
	
	for nodeID, node := range dc.nodes {
		if node.IsActive() {
			activeNodes++
		}
		size := node.Size()
		totalItems += size
		nodeStats[nodeID] = size
	}
	
	stats["active_nodes"] = activeNodes
	stats["total_items"] = totalItems
	stats["node_distribution"] = nodeStats
	
	// Add ring information
	ringInfo := dc.ring.GetRingInfo()
	for key, value := range ringInfo {
		stats["ring_"+key] = value
	}
	
	return stats
}

// SimulateNodeFailure simulates a node failure
func (dc *DistributedCache) SimulateNodeFailure(nodeID string) error {
	dc.mutex.RLock()
	defer dc.mutex.RUnlock()
	
	if node, exists := dc.nodes[nodeID]; exists {
		node.SetActive(false)
		return nil
	}
	
	return fmt.Errorf("node %s not found", nodeID)
}

// SimulateNodeRecovery simulates a node recovery
func (dc *DistributedCache) SimulateNodeRecovery(nodeID string) error {
	dc.mutex.RLock()
	defer dc.mutex.RUnlock()
	
	if node, exists := dc.nodes[nodeID]; exists {
		node.SetActive(true)
		return nil
	}
	
	return fmt.Errorf("node %s not found", nodeID)
}

func main() {
	fmt.Println("=== Distributed Cache with Consistent Hashing ===\n")

	// Create distributed cache with 3 replicas
	cache, err := NewDistributedCache(50, 3)
	if err != nil {
		log.Fatalf("Failed to create distributed cache: %v", err)
	}

	// Add cache nodes
	nodes := []*CacheNode{
		NewCacheNode("cache1", "192.168.1.10", 6379),
		NewCacheNode("cache2", "192.168.1.11", 6379),
		NewCacheNode("cache3", "192.168.1.12", 6379),
		NewCacheNode("cache4", "192.168.1.13", 6379),
		NewCacheNode("cache5", "192.168.1.14", 6379),
	}

	fmt.Println("Adding cache nodes...")
	for _, node := range nodes {
		if err := cache.AddNode(node); err != nil {
			log.Printf("Failed to add node %s: %v", node.ID, err)
			continue
		}
		fmt.Printf("Added cache node: %s (%s:%d)\n", node.ID, node.Host, node.Port)
	}

	// Store some data
	fmt.Println("\nStoring data in distributed cache...")
	testData := map[string]string{
		"user:1001":    "John Doe",
		"user:1002":    "Jane Smith",
		"session:abc":  "active",
		"session:def":  "expired",
		"product:123":  "Laptop",
		"product:456":  "Mouse",
		"config:db":    "postgresql://localhost:5432/mydb",
		"config:redis": "redis://localhost:6379",
	}

	for key, value := range testData {
		if err := cache.Set(key, value); err != nil {
			log.Printf("Failed to set %s: %v", key, err)
		} else {
			fmt.Printf("Stored: %s = %s\n", key, value)
		}
	}

	// Retrieve data
	fmt.Println("\nRetrieving data from distributed cache...")
	for key := range testData {
		value, err := cache.Get(key)
		if err != nil {
			log.Printf("Failed to get %s: %v", key, err)
		} else {
			fmt.Printf("Retrieved: %s = %s\n", key, value)
		}
	}

	// Show cache statistics
	fmt.Println("\n--- Cache Statistics ---")
	stats := cache.GetStats()
	for key, value := range stats {
		fmt.Printf("%s: %v\n", key, value)
	}

	// Demonstrate replication by checking which nodes have which keys
	fmt.Println("\n--- Replication Analysis ---")
	analyzeReplication(cache, testData)

	// Simulate node failure
	fmt.Println("\n--- Simulating Node Failure ---")
	failedNode := "cache2"
	if err := cache.SimulateNodeFailure(failedNode); err != nil {
		log.Printf("Failed to simulate node failure: %v", err)
	} else {
		fmt.Printf("Simulated failure of node: %s\n", failedNode)
	}

	// Test data retrieval after failure
	fmt.Println("\nTesting data retrieval after node failure...")
	successCount := 0
	for key := range testData {
		value, err := cache.Get(key)
		if err != nil {
			log.Printf("Failed to get %s after node failure: %v", key, err)
		} else {
			fmt.Printf("Successfully retrieved: %s = %s\n", key, value)
			successCount++
		}
	}
	fmt.Printf("\nSuccessfully retrieved %d out of %d keys after node failure\n", 
		successCount, len(testData))

	// Show updated statistics
	fmt.Println("\n--- Statistics After Node Failure ---")
	stats = cache.GetStats()
	for key, value := range stats {
		fmt.Printf("%s: %v\n", key, value)
	}

	// Simulate node recovery
	fmt.Println("\n--- Simulating Node Recovery ---")
	if err := cache.SimulateNodeRecovery(failedNode); err != nil {
		log.Printf("Failed to simulate node recovery: %v", err)
	} else {
		fmt.Printf("Simulated recovery of node: %s\n", failedNode)
	}

	// Add a new node (scale out)
	fmt.Println("\n--- Scaling Out (Adding New Node) ---")
	newNode := NewCacheNode("cache6", "192.168.1.15", 6379)
	if err := cache.AddNode(newNode); err != nil {
		log.Printf("Failed to add new node: %v", err)
	} else {
		fmt.Printf("Added new cache node: %s\n", newNode.ID)
	}

	// Demonstrate consistent hashing behavior
	fmt.Println("\n--- Consistent Hashing Behavior ---")
	demonstrateConsistentHashing(cache, testData)

	// Performance test
	fmt.Println("\n--- Performance Test ---")
	performanceTest(cache)

	// Cleanup test
	fmt.Println("\n--- Cleanup Test ---")
	cleanupTest(cache, testData)

	fmt.Println("\n=== Distributed Cache Demo Complete ===")
}

func analyzeReplication(cache *DistributedCache, testData map[string]string) {
	fmt.Println("Analyzing key distribution across nodes...")
	
	for key := range testData {
		nodes, err := cache.ring.GetNodes(key, cache.replication)
		if err != nil {
			log.Printf("Failed to get nodes for key %s: %v", key, err)
			continue
		}
		
		fmt.Printf("Key '%s' replicated on nodes: ", key)
		for i, node := range nodes {
			if i > 0 {
				fmt.Print(", ")
			}
			fmt.Print(node.ID)
		}
		fmt.Println()
	}
}

func demonstrateConsistentHashing(cache *DistributedCache, testData map[string]string) {
	fmt.Println("Demonstrating consistent hashing properties...")
	
	// Record original mappings
	originalMappings := make(map[string]string)
	for key := range testData {
		node, err := cache.ring.GetNode(key)
		if err != nil {
			log.Printf("Failed to get primary node for key %s: %v", key, err)
			continue
		}
		originalMappings[key] = node.ID
	}
	
	// Add another new node
	extraNode := NewCacheNode("cache7", "192.168.1.16", 6379)
	if err := cache.AddNode(extraNode); err != nil {
		log.Printf("Failed to add extra node: %v", err)
		return
	}
	fmt.Printf("Added extra node: %s\n", extraNode.ID)
	
	// Check how many keys moved
	movedKeys := 0
	for key := range testData {
		node, err := cache.ring.GetNode(key)
		if err != nil {
			log.Printf("Failed to get primary node for key %s: %v", key, err)
			continue
		}
		
		if node.ID != originalMappings[key] {
			fmt.Printf("Key '%s' moved: %s -> %s\n", key, originalMappings[key], node.ID)
			movedKeys++
		}
	}
	
	fmt.Printf("Only %d out of %d keys moved (%.1f%%) when adding a new node\n", 
		movedKeys, len(testData), float64(movedKeys)/float64(len(testData))*100)
}

func performanceTest(cache *DistributedCache) {
	fmt.Println("Running performance test...")
	
	// Generate test data
	numOperations := 1000
	keys := make([]string, numOperations)
	values := make([]string, numOperations)
	
	for i := 0; i < numOperations; i++ {
		keys[i] = fmt.Sprintf("perf_key_%d", i)
		values[i] = fmt.Sprintf("perf_value_%d_%d", i, rand.Intn(10000))
	}
	
	// Test SET operations
	start := time.Now()
	setErrors := 0
	for i := 0; i < numOperations; i++ {
		if err := cache.Set(keys[i], values[i]); err != nil {
			setErrors++
		}
	}
	setDuration := time.Since(start)
	
	// Test GET operations
	start = time.Now()
	getErrors := 0
	for i := 0; i < numOperations; i++ {
		if _, err := cache.Get(keys[i]); err != nil {
			getErrors++
		}
	}
	getDuration := time.Since(start)
	
	fmt.Printf("Performance Results (%d operations):\n", numOperations)
	fmt.Printf("SET: %v (%.1f ops/sec, %d errors)\n", 
		setDuration, float64(numOperations)/setDuration.Seconds(), setErrors)
	fmt.Printf("GET: %v (%.1f ops/sec, %d errors)\n", 
		getDuration, float64(numOperations)/getDuration.Seconds(), getErrors)
	
	// Cleanup performance test data
	for i := 0; i < numOperations; i++ {
		cache.Delete(keys[i])
	}
}

func cleanupTest(cache *DistributedCache, testData map[string]string) {
	fmt.Println("Testing cleanup operations...")
	
	deleteErrors := 0
	for key := range testData {
		if err := cache.Delete(key); err != nil {
			log.Printf("Failed to delete %s: %v", key, err)
			deleteErrors++
		} else {
			fmt.Printf("Deleted: %s\n", key)
		}
	}
	
	if deleteErrors == 0 {
		fmt.Println("All keys deleted successfully")
	} else {
		fmt.Printf("Failed to delete %d keys\n", deleteErrors)
	}
	
	// Verify deletion
	fmt.Println("\nVerifying deletion...")
	for key := range testData {
		if _, err := cache.Get(key); err != nil {
			fmt.Printf("Confirmed deletion of: %s\n", key)
		} else {
			fmt.Printf("WARNING: Key %s still exists after deletion\n", key)
		}
	}
	
	// Final statistics
	fmt.Println("\n--- Final Cache Statistics ---")
	stats := cache.GetStats()
	for key, value := range stats {
		fmt.Printf("%s: %v\n", key, value)
	}
} 
} 