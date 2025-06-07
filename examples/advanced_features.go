package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/alexnthnz/consistent-hashing"
)

func main() {
	fmt.Println("=== Advanced Consistent Hashing Features Demo ===\n")

	// 1. Hash Function Comparison
	demonstrateHashFunctions()

	// 2. Weighted Consistent Hashing
	demonstrateWeightedHashing()

	// 3. Thread Safety
	demonstrateThreadSafety()

	// 4. Monitoring and Analytics
	demonstrateMonitoring()

	// 5. Error Handling and Validation
	demonstrateErrorHandling()

	// 6. Performance Comparison
	demonstratePerformance()
}

func demonstrateHashFunctions() {
	fmt.Println("--- Hash Function Comparison ---")

	// Create rings with different hash functions
	fnvRing, err := consistenthashing.NewHashRing(50, 
		consistenthashing.WithHashFunction(&consistenthashing.FNVHasher{}))
	if err != nil {
		log.Printf("Failed to create FNV ring: %v", err)
		return
	}

	sha256Ring, err := consistenthashing.NewHashRing(50, 
		consistenthashing.WithHashFunction(&consistenthashing.SHA256Hasher{}))
	if err != nil {
		log.Printf("Failed to create SHA256 ring: %v", err)
		return
	}

	// Add same nodes to both rings
	nodes := []*consistenthashing.Node{
		{ID: "node1", Host: "server1.example.com", Port: 8080},
		{ID: "node2", Host: "server2.example.com", Port: 8080},
		{ID: "node3", Host: "server3.example.com", Port: 8080},
	}

	for _, node := range nodes {
		if err := fnvRing.AddNode(node); err != nil {
			log.Printf("Failed to add node to FNV ring: %v", err)
		}
		if err := sha256Ring.AddNode(node); err != nil {
			log.Printf("Failed to add node to SHA256 ring: %v", err)
		}
	}

	// Compare key mappings
	testKeys := []string{"user:1001", "user:1002", "user:1003", "session:abc123", "cache:data1"}
	
	fmt.Println("Key mapping comparison:")
	fmt.Printf("%-15s %-10s %-10s %-10s\n", "Key", "FNV Hash", "SHA256", "Same?")
	fmt.Println(fmt.Sprintf("%s", "-----------------------------------------------------------"))
	
	for _, key := range testKeys {
		fnvNode, err1 := fnvRing.GetNode(key)
		sha256Node, err2 := sha256Ring.GetNode(key)
		
		if err1 != nil || err2 != nil {
			log.Printf("Error getting nodes for key %s: FNV=%v, SHA256=%v", key, err1, err2)
			continue
		}
		
		same := "No"
		if fnvNode.ID == sha256Node.ID {
			same = "Yes"
		}
		
		fmt.Printf("%-15s %-10s %-10s %-10s\n", key, fnvNode.ID, sha256Node.ID, same)
	}

	// Show ring info
	fnvInfo := fnvRing.GetRingInfo()
	sha256Info := sha256Ring.GetRingInfo()
	
	fmt.Printf("\nRing Information:\n")
	fmt.Printf("FNV Ring: %s hash function\n", fnvInfo["hash_function"])
	fmt.Printf("SHA256 Ring: %s hash function\n", sha256Info["hash_function"])
	fmt.Println()
}

func demonstrateWeightedHashing() {
	fmt.Println("--- Weighted Consistent Hashing ---")

	ring, err := consistenthashing.NewHashRing(20)
	if err != nil {
		log.Printf("Failed to create ring: %v", err)
		return
	}

	// Add nodes with different weights
	nodes := []*consistenthashing.Node{
		{ID: "small", Host: "small.example.com", Port: 8080, Weight: 1},   // 1x capacity
		{ID: "medium", Host: "medium.example.com", Port: 8080, Weight: 2}, // 2x capacity
		{ID: "large", Host: "large.example.com", Port: 8080, Weight: 4},   // 4x capacity
	}

	for _, node := range nodes {
		if err := ring.AddNode(node); err != nil {
			log.Printf("Failed to add node %s: %v", node.ID, err)
			continue
		}
		virtualCount := 20 * node.Weight
		if node.Weight <= 0 {
			virtualCount = 20
		}
		fmt.Printf("Added node '%s' with weight %d (%d virtual nodes)\n", 
			node.ID, node.Weight, virtualCount)
	}

	// Analyze load distribution
	keyCount := 10000
	keys := make([]string, keyCount)
	for i := 0; i < keyCount; i++ {
		keys[i] = fmt.Sprintf("item_%d", i)
	}

	distribution, err := ring.GetLoadDistribution(keys)
	if err != nil {
		log.Printf("Failed to get load distribution: %v", err)
		return
	}

	fmt.Printf("\nLoad distribution for %d keys:\n", keyCount)
	totalWeight := 1 + 2 + 4 // sum of weights
	for _, node := range nodes {
		count := distribution[node.ID]
		expectedRatio := float64(node.Weight) / float64(totalWeight)
		actualRatio := float64(count) / float64(keyCount)
		
		fmt.Printf("Node '%s' (weight %d): %d keys (%.1f%%, expected %.1f%%)\n",
			node.ID, node.Weight, count, actualRatio*100, expectedRatio*100)
	}
	fmt.Println()
}

func demonstrateThreadSafety() {
	fmt.Println("--- Thread Safety Test ---")

	ring, err := consistenthashing.NewHashRing(50)
	if err != nil {
		log.Printf("Failed to create ring: %v", err)
		return
	}

	// Add initial nodes
	for i := 0; i < 5; i++ {
		node := &consistenthashing.Node{
			ID:   fmt.Sprintf("initial_%d", i),
			Host: fmt.Sprintf("server%d.example.com", i),
			Port: 8080,
		}
		if err := ring.AddNode(node); err != nil {
			log.Printf("Failed to add initial node: %v", err)
		}
	}

	fmt.Printf("Starting concurrent operations with %d initial nodes...\n", ring.Size())

	var wg sync.WaitGroup
	numGoroutines := 20
	operationsPerGoroutine := 100
	
	start := time.Now()
	
	// Start multiple goroutines performing concurrent operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			
			for j := 0; j < operationsPerGoroutine; j++ {
				// Mix of operations
				switch j % 5 {
				case 0: // Add node
					node := &consistenthashing.Node{
						ID:   fmt.Sprintf("dynamic_%d_%d", goroutineID, j),
						Host: fmt.Sprintf("dynamic%d.example.com", goroutineID),
						Port: 8080 + j,
					}
					ring.AddNode(node)
					
				case 1: // Get node
					key := fmt.Sprintf("key_%d_%d", goroutineID, j)
					ring.GetNode(key)
					
				case 2: // Get multiple nodes
					key := fmt.Sprintf("multi_%d_%d", goroutineID, j)
					ring.GetNodes(key, 3)
					
				case 3: // Check if node exists
					nodeID := fmt.Sprintf("check_%d_%d", goroutineID, j)
					ring.HasNode(nodeID)
					
				case 4: // Get ring info
					ring.GetRingInfo()
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	fmt.Printf("Completed %d concurrent operations in %v\n", 
		numGoroutines*operationsPerGoroutine, duration)
	fmt.Printf("Final ring size: %d physical nodes, %d virtual nodes\n", 
		ring.Size(), ring.VirtualSize())

	// Validate ring integrity after concurrent operations
	if err := ring.ValidateRing(); err != nil {
		fmt.Printf("Ring validation failed: %v\n", err)
	} else {
		fmt.Println("Ring validation passed after concurrent operations!")
	}
	fmt.Println()
}

func demonstrateMonitoring() {
	fmt.Println("--- Monitoring and Analytics ---")

	ring, err := consistenthashing.NewHashRing(100)
	if err != nil {
		log.Printf("Failed to create ring: %v", err)
		return
	}

	// Add nodes
	nodes := []*consistenthashing.Node{
		{ID: "web1", Host: "web1.cluster.local", Port: 8080, Weight: 2},
		{ID: "web2", Host: "web2.cluster.local", Port: 8080, Weight: 1},
		{ID: "web3", Host: "web3.cluster.local", Port: 8080, Weight: 3},
		{ID: "web4", Host: "web4.cluster.local", Port: 8080, Weight: 1},
	}

	for _, node := range nodes {
		if err := ring.AddNode(node); err != nil {
			log.Printf("Failed to add node: %v", err)
		}
	}

	// Get comprehensive ring information
	info := ring.GetRingInfo()
	fmt.Println("Ring Statistics:")
	for key, value := range info {
		fmt.Printf("  %s: %v\n", key, value)
	}

	// Analyze load distribution with different key patterns
	patterns := map[string][]string{
		"Sequential": generateSequentialKeys(1000),
		"Random":     generateRandomKeys(1000),
		"Clustered":  generateClusteredKeys(1000),
	}

	for patternName, keys := range patterns {
		distribution, err := ring.GetLoadDistribution(keys)
		if err != nil {
			log.Printf("Failed to get distribution for %s pattern: %v", patternName, err)
			continue
		}

		fmt.Printf("\n%s Key Pattern Distribution:\n", patternName)
		for nodeID, count := range distribution {
			percentage := float64(count) / float64(len(keys)) * 100
			fmt.Printf("  Node %s: %d keys (%.1f%%)\n", nodeID, count, percentage)
		}
	}

	// Show all nodes in deterministic order
	allNodes := ring.GetAllNodes()
	fmt.Printf("\nAll nodes (sorted by ID):\n")
	for i, node := range allNodes {
		fmt.Printf("  %d. %s (%s) - Weight: %d\n", i+1, node.ID, node.String(), node.Weight)
	}
	fmt.Println()
}

func demonstrateErrorHandling() {
	fmt.Println("--- Error Handling and Validation ---")

	// Test various error conditions
	fmt.Println("Testing error conditions:")

	// Invalid ring creation
	_, err := consistenthashing.NewHashRing(-1)
	fmt.Printf("1. Creating ring with negative replicas: %v\n", err)

	// Create valid ring for further tests
	ring, err := consistenthashing.NewHashRing(10)
	if err != nil {
		log.Printf("Failed to create ring: %v", err)
		return
	}

	// Invalid node operations
	invalidNodes := []*consistenthashing.Node{
		nil,                                                    // nil node
		{ID: "", Host: "localhost", Port: 8080},               // empty ID
		{ID: "test", Host: "", Port: 8080},                    // empty host
		{ID: "test", Host: "localhost", Port: 0},              // invalid port
		{ID: "test", Host: "localhost", Port: -1},             // negative port
		{ID: "test", Host: "localhost", Port: 70000},          // port too high
	}

	for i, node := range invalidNodes {
		err := ring.AddNode(node)
		fmt.Printf("%d. Adding invalid node: %v\n", i+2, err)
	}

	// Add a valid node for further tests
	validNode := &consistenthashing.Node{ID: "valid", Host: "localhost", Port: 8080}
	if err := ring.AddNode(validNode); err != nil {
		log.Printf("Failed to add valid node: %v", err)
		return
	}

	// Test invalid operations
	testCases := []struct {
		name string
		fn   func() error
	}{
		{"Empty key lookup", func() error { _, err := ring.GetNode(""); return err }},
		{"Zero count GetNodes", func() error { _, err := ring.GetNodes("key", 0); return err }},
		{"Negative count GetNodes", func() error { _, err := ring.GetNodes("key", -1); return err }},
		{"Remove empty ID", func() error { return ring.RemoveNode("") }},
		{"Remove non-existent", func() error { return ring.RemoveNode("nonexistent") }},
		{"Get node by empty ID", func() error { _, err := ring.GetNodeByID(""); return err }},
		{"Get node by non-existent ID", func() error { _, err := ring.GetNodeByID("nonexistent"); return err }},
		{"Load distribution with nil keys", func() error { _, err := ring.GetLoadDistribution(nil); return err }},
	}

	for i, tc := range testCases {
		err := tc.fn()
		fmt.Printf("%d. %s: %v\n", i+len(invalidNodes)+2, tc.name, err)
	}

	// Test ring validation
	fmt.Printf("\nRing validation: ")
	if err := ring.ValidateRing(); err != nil {
		fmt.Printf("Failed - %v\n", err)
	} else {
		fmt.Println("Passed")
	}
	fmt.Println()
}

func demonstratePerformance() {
	fmt.Println("--- Performance Comparison ---")

	// Create rings with different hash functions
	fnvRing, _ := consistenthashing.NewHashRing(100, 
		consistenthashing.WithHashFunction(&consistenthashing.FNVHasher{}))
	sha256Ring, _ := consistenthashing.NewHashRing(100, 
		consistenthashing.WithHashFunction(&consistenthashing.SHA256Hasher{}))

	// Add nodes
	for i := 0; i < 10; i++ {
		node := &consistenthashing.Node{
			ID:   fmt.Sprintf("perf_node_%d", i),
			Host: fmt.Sprintf("perf%d.example.com", i),
			Port: 8080,
		}
		fnvRing.AddNode(node)
		sha256Ring.AddNode(node)
	}

	// Performance test
	iterations := 100000
	testKey := "performance_test_key"

	// FNV performance
	start := time.Now()
	for i := 0; i < iterations; i++ {
		fnvRing.GetNode(fmt.Sprintf("%s_%d", testKey, i))
	}
	fnvDuration := time.Since(start)

	// SHA256 performance
	start = time.Now()
	for i := 0; i < iterations; i++ {
		sha256Ring.GetNode(fmt.Sprintf("%s_%d", testKey, i))
	}
	sha256Duration := time.Since(start)

	fmt.Printf("Performance test (%d iterations):\n", iterations)
	fmt.Printf("FNV Hash:    %v (%.0f ns/op)\n", fnvDuration, float64(fnvDuration.Nanoseconds())/float64(iterations))
	fmt.Printf("SHA256 Hash: %v (%.0f ns/op)\n", sha256Duration, float64(sha256Duration.Nanoseconds())/float64(iterations))
	
	speedup := float64(sha256Duration) / float64(fnvDuration)
	fmt.Printf("FNV is %.1fx faster than SHA256\n", speedup)
	fmt.Println()
}

// Helper functions for generating different key patterns
func generateSequentialKeys(count int) []string {
	keys := make([]string, count)
	for i := 0; i < count; i++ {
		keys[i] = fmt.Sprintf("seq_%06d", i)
	}
	return keys
}

func generateRandomKeys(count int) []string {
	keys := make([]string, count)
	for i := 0; i < count; i++ {
		keys[i] = fmt.Sprintf("random_%d_%d", i, time.Now().UnixNano()%10000)
	}
	return keys
}

func generateClusteredKeys(count int) []string {
	keys := make([]string, count)
	clusters := []string{"user", "session", "cache", "data", "temp"}
	for i := 0; i < count; i++ {
		cluster := clusters[i%len(clusters)]
		keys[i] = fmt.Sprintf("%s_%06d", cluster, i/len(clusters))
	}
	return keys
} 