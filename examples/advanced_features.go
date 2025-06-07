package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/alexnthnz/consistent-hashing"
)

func main() {
	fmt.Println("=== Advanced Consistent Hashing Features ===\n")

	// Demonstrate different hash functions
	demonstrateHashFunctions()

	// Demonstrate weighted nodes
	demonstrateWeightedNodes()

	// Demonstrate thread safety
	demonstrateThreadSafety()

	// Demonstrate monitoring and analytics
	demonstrateMonitoring()
}

func demonstrateHashFunctions() {
	fmt.Println("1. Hash Function Comparison")
	fmt.Println(repeatString("=", 40))

	// Create rings with different hash functions
	fnvRing := consistenthashing.NewHashRing(50, consistenthashing.WithHashFunction(&consistenthashing.FNVHasher{}))
	sha256Ring := consistenthashing.NewHashRing(50, consistenthashing.WithHashFunction(&consistenthashing.SHA256Hasher{}))

	// Add same nodes to both rings
	nodes := []*consistenthashing.Node{
		{ID: "server1", Host: "192.168.1.10", Port: 8080},
		{ID: "server2", Host: "192.168.1.11", Port: 8080},
		{ID: "server3", Host: "192.168.1.12", Port: 8080},
	}

	for _, node := range nodes {
		fnvRing.AddNode(node)
		sha256Ring.AddNode(node)
	}

	// Test key distribution
	testKeys := []string{"user:1001", "user:1002", "user:1003", "session:abc", "product:123"}

	fmt.Println("Key distribution comparison:")
	fmt.Printf("%-15s %-15s %-15s\n", "Key", "FNV Hash", "SHA256 Hash")
	fmt.Println(repeatString("-", 45))

	for _, key := range testKeys {
		fnvNode := fnvRing.GetNode(key)
		sha256Node := sha256Ring.GetNode(key)
		fmt.Printf("%-15s %-15s %-15s\n", key, fnvNode.ID, sha256Node.ID)
	}

	// Benchmark hash functions
	fmt.Println("\nPerformance comparison (1M operations):")
	benchmarkHashFunction("FNV", fnvRing)
	benchmarkHashFunction("SHA256", sha256Ring)
	fmt.Println()
}

func benchmarkHashFunction(name string, ring *consistenthashing.HashRing) {
	start := time.Now()
	for i := 0; i < 1000000; i++ {
		key := fmt.Sprintf("key_%d", i)
		ring.GetNode(key)
	}
	duration := time.Since(start)
	fmt.Printf("%s: %v (%.0f ops/sec)\n", name, duration, 1000000.0/duration.Seconds())
}

func demonstrateWeightedNodes() {
	fmt.Println("2. Weighted Consistent Hashing")
	fmt.Println(repeatString("=", 40))

	ring := consistenthashing.NewHashRing(20)

	// Add nodes with different capacities/weights
	nodes := []*consistenthashing.Node{
		{ID: "small-server", Host: "192.168.1.10", Port: 8080, Weight: 1},   // Low capacity
		{ID: "medium-server", Host: "192.168.1.11", Port: 8080, Weight: 2},  // Medium capacity
		{ID: "large-server", Host: "192.168.1.12", Port: 8080, Weight: 4},   // High capacity
	}

	for _, node := range nodes {
		ring.AddNode(node)
		fmt.Printf("Added %s with weight %d\n", node.ID, node.Weight)
	}

	// Show virtual node distribution
	info := ring.GetRingInfo()
	fmt.Printf("\nRing info: %d physical nodes, %d virtual nodes\n", 
		info["physical_nodes"], info["virtual_nodes"])

	// Generate test keys and analyze distribution
	keys := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = fmt.Sprintf("key_%d", i)
	}

	distribution := ring.GetLoadDistribution(keys)
	fmt.Println("\nLoad distribution (1000 keys):")
	for nodeID, count := range distribution {
		node := ring.GetNodeByID(nodeID)
		percentage := float64(count) / 10.0
		fmt.Printf("%-15s: %3d keys (%.1f%%) [weight: %d]\n", 
			nodeID, count, percentage, node.Weight)
	}

	// Calculate expected vs actual distribution
	fmt.Println("\nWeight-based distribution analysis:")
	totalWeight := 1 + 2 + 4 // 7
	for nodeID, count := range distribution {
		node := ring.GetNodeByID(nodeID)
		expectedPercentage := float64(node.Weight) / float64(totalWeight) * 100
		actualPercentage := float64(count) / 10.0
		fmt.Printf("%-15s: Expected %.1f%%, Actual %.1f%%, Difference: %.1f%%\n",
			nodeID, expectedPercentage, actualPercentage, actualPercentage-expectedPercentage)
	}
	fmt.Println()
}

func demonstrateThreadSafety() {
	fmt.Println("3. Thread Safety & Concurrent Operations")
	fmt.Println(repeatString("=", 40))

	ring := consistenthashing.NewHashRing(10)

	// Add initial nodes
	for i := 0; i < 3; i++ {
		node := &consistenthashing.Node{
			ID:   fmt.Sprintf("initial-node-%d", i),
			Host: "192.168.1.10",
			Port: 8080 + i,
		}
		ring.AddNode(node)
	}

	fmt.Printf("Starting with %d nodes\n", ring.Size())

	// Concurrent operations
	numWorkers := 10
	operationsPerWorker := 100
	var wg sync.WaitGroup

	// Metrics
	var addOps, removeOps, getOps int64
	var mu sync.Mutex

	start := time.Now()

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rand.Seed(time.Now().UnixNano() + int64(workerID))

			for j := 0; j < operationsPerWorker; j++ {
				operation := rand.Intn(3)

				switch operation {
				case 0: // Add node
					nodeID := fmt.Sprintf("worker-%d-node-%d", workerID, j)
					node := &consistenthashing.Node{
						ID:   nodeID,
						Host: "192.168.1.100",
						Port: 8080 + workerID*1000 + j,
					}
					ring.AddNode(node)
					mu.Lock()
					addOps++
					mu.Unlock()

				case 1: // Remove node (occasionally)
					if j%20 == 0 {
						nodeID := fmt.Sprintf("worker-%d-node-%d", workerID, j-10)
						ring.RemoveNode(nodeID)
						mu.Lock()
						removeOps++
						mu.Unlock()
					}

				case 2: // Get node
					key := fmt.Sprintf("key-%d-%d", workerID, j)
					ring.GetNode(key)
					mu.Lock()
					getOps++
					mu.Unlock()
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	fmt.Printf("Completed in %v\n", duration)
	fmt.Printf("Operations: %d adds, %d removes, %d gets\n", addOps, removeOps, getOps)
	fmt.Printf("Final ring size: %d physical nodes, %d virtual nodes\n", 
		ring.Size(), ring.VirtualSize())
	fmt.Printf("Throughput: %.0f ops/sec\n", float64(addOps+removeOps+getOps)/duration.Seconds())
	fmt.Println()
}

func demonstrateMonitoring() {
	fmt.Println("4. Monitoring & Analytics")
	fmt.Println(repeatString("=", 40))

	ring := consistenthashing.NewHashRing(50)

	// Add nodes with different characteristics
	nodes := []*consistenthashing.Node{
		{ID: "us-east-1", Host: "10.0.1.10", Port: 8080, Weight: 3},
		{ID: "us-west-1", Host: "10.0.2.10", Port: 8080, Weight: 2},
		{ID: "eu-west-1", Host: "10.0.3.10", Port: 8080, Weight: 2},
		{ID: "ap-south-1", Host: "10.0.4.10", Port: 8080, Weight: 1},
	}

	for _, node := range nodes {
		ring.AddNode(node)
	}

	// Generate realistic workload
	fmt.Println("Simulating realistic workload...")
	
	// Different key patterns
	userKeys := generateKeys("user:", 1000)
	sessionKeys := generateKeys("session:", 500)
	cacheKeys := generateKeys("cache:", 2000)
	
	allKeys := append(userKeys, sessionKeys...)
	allKeys = append(allKeys, cacheKeys...)

	// Analyze distribution
	distribution := ring.GetLoadDistribution(allKeys)
	
	fmt.Printf("\nLoad Distribution Analysis (%d total keys):\n", len(allKeys))
	fmt.Printf("%-12s %-8s %-8s %-8s %-8s\n", "Node", "Keys", "Percent", "Weight", "Efficiency")
	fmt.Println(repeatString("-", 50))

	totalKeys := len(allKeys)
	totalWeight := 0
	for _, node := range nodes {
		totalWeight += node.Weight
	}

	for _, node := range nodes {
		count := distribution[node.ID]
		percentage := float64(count) / float64(totalKeys) * 100
		expectedPercentage := float64(node.Weight) / float64(totalWeight) * 100
		efficiency := percentage / expectedPercentage * 100

		fmt.Printf("%-12s %-8d %-8.1f %-8d %-8.1f\n", 
			node.ID, count, percentage, node.Weight, efficiency)
	}

	// Ring statistics
	fmt.Println("\nRing Statistics:")
	info := ring.GetRingInfo()
	for key, value := range info {
		fmt.Printf("  %s: %v\n", key, value)
	}

	// Simulate node failure and recovery
	fmt.Println("\nSimulating node failure...")
	failedNode := "us-east-1"
	ring.RemoveNode(failedNode)
	fmt.Printf("Removed node: %s\n", failedNode)

	// Analyze redistribution
	newDistribution := ring.GetLoadDistribution(allKeys)
	fmt.Println("\nLoad redistribution after failure:")
	for nodeID, newCount := range newDistribution {
		oldCount := distribution[nodeID]
		change := newCount - oldCount
		fmt.Printf("%-12s: %d -> %d (change: %+d)\n", nodeID, oldCount, newCount, change)
	}

	// Calculate keys that moved
	movedKeys := 0
	for _, key := range allKeys {
		newNode := ring.GetNode(key)
		if newNode == nil || newNode.ID == failedNode {
			movedKeys++
		}
	}
	fmt.Printf("\nKeys affected by failure: %d (%.1f%%)\n", 
		movedKeys, float64(movedKeys)/float64(totalKeys)*100)

	// Recovery
	fmt.Println("\nSimulating node recovery...")
	recoveredNode := &consistenthashing.Node{
		ID: "us-east-1-new", Host: "10.0.1.20", Port: 8080, Weight: 3,
	}
	ring.AddNode(recoveredNode)
	fmt.Printf("Added replacement node: %s\n", recoveredNode.ID)

	finalDistribution := ring.GetLoadDistribution(allKeys)
	fmt.Println("\nFinal distribution after recovery:")
	for nodeID, count := range finalDistribution {
		percentage := float64(count) / float64(totalKeys) * 100
		fmt.Printf("%-12s: %d keys (%.1f%%)\n", nodeID, count, percentage)
	}
}

func generateKeys(prefix string, count int) []string {
	keys := make([]string, count)
	for i := 0; i < count; i++ {
		keys[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return keys
}

// Helper function to repeat strings (Go doesn't have this built-in)
func repeatString(s string, count int) string {
	result := ""
	for i := 0; i < count; i++ {
		result += s
	}
	return result
}

// Use this instead of "*" operator which doesn't work with strings in Go
var _ = repeatString // Suppress unused function warning 