package main

import (
	"fmt"

	"github.com/alexnthnz/consistent-hashing"
)

func main() {
	// Create a new hash ring with 100 virtual nodes per physical node
	ring := consistenthashing.NewHashRing(100)

	// Create some nodes
	nodes := []*consistenthashing.Node{
		{ID: "server1", Host: "192.168.1.10", Port: 8080},
		{ID: "server2", Host: "192.168.1.11", Port: 8080},
		{ID: "server3", Host: "192.168.1.12", Port: 8080},
		{ID: "server4", Host: "192.168.1.13", Port: 8080},
	}

	// Add nodes to the ring
	fmt.Println("Adding nodes to the hash ring...")
	for _, node := range nodes {
		ring.AddNode(node)
		fmt.Printf("Added node: %s (%s)\n", node.ID, node.String())
	}

	fmt.Printf("\nHash ring now has %d physical nodes and %d virtual nodes\n\n", 
		ring.Size(), ring.VirtualSize())

	// Demonstrate key-to-node mapping
	testKeys := []string{"user:1001", "user:1002", "user:1003", "user:1004", "user:1005"}
	
	fmt.Println("Key-to-node mapping:")
	for _, key := range testKeys {
		node := ring.GetNode(key)
		if node != nil {
			fmt.Printf("Key '%s' -> Node '%s' (%s)\n", key, node.ID, node.String())
		}
	}

	// Demonstrate replication (getting multiple nodes for a key)
	fmt.Println("\nReplication example (3 replicas for each key):")
	for _, key := range testKeys[:2] { // Just show first 2 keys
		nodes := ring.GetNodes(key, 3)
		fmt.Printf("Key '%s' -> Nodes: ", key)
		for i, node := range nodes {
			if i > 0 {
				fmt.Print(", ")
			}
			fmt.Printf("%s", node.ID)
		}
		fmt.Println()
	}

	// Demonstrate consistent hashing behavior when adding a node
	fmt.Println("\n--- Adding a new node ---")
	
	// Store current mappings
	originalMappings := make(map[string]*consistenthashing.Node)
	for _, key := range testKeys {
		originalMappings[key] = ring.GetNode(key)
	}

	// Add a new node
	newNode := &consistenthashing.Node{ID: "server5", Host: "192.168.1.14", Port: 8080}
	ring.AddNode(newNode)
	fmt.Printf("Added new node: %s (%s)\n", newNode.ID, newNode.String())
	fmt.Printf("Hash ring now has %d physical nodes\n\n", ring.Size())

	// Check which keys moved
	fmt.Println("Key mapping changes after adding new node:")
	movedKeys := 0
	for _, key := range testKeys {
		newNode := ring.GetNode(key)
		originalNode := originalMappings[key]
		
		if newNode.ID != originalNode.ID {
			fmt.Printf("Key '%s' moved: %s -> %s\n", key, originalNode.ID, newNode.ID)
			movedKeys++
		} else {
			fmt.Printf("Key '%s' stayed: %s\n", key, originalNode.ID)
		}
	}
	
	fmt.Printf("\nOnly %d out of %d keys moved (%.1f%%) - demonstrating consistent hashing!\n", 
		movedKeys, len(testKeys), float64(movedKeys)/float64(len(testKeys))*100)

	// Demonstrate node removal
	fmt.Println("\n--- Removing a node ---")
	ring.RemoveNode("server2")
	fmt.Printf("Removed node: server2\n")
	fmt.Printf("Hash ring now has %d physical nodes\n\n", ring.Size())

	fmt.Println("Key mappings after node removal:")
	for _, key := range testKeys {
		node := ring.GetNode(key)
		if node != nil {
			fmt.Printf("Key '%s' -> Node '%s'\n", key, node.ID)
		}
	}

	// Show load distribution
	fmt.Println("\n--- Load Distribution Analysis ---")
	analyzeLoadDistribution(ring)
}

func analyzeLoadDistribution(ring *consistenthashing.HashRing) {
	// Generate many keys and see how they distribute
	keyCount := 10000
	distribution := make(map[string]int)
	
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("key_%d", i)
		node := ring.GetNode(key)
		if node != nil {
			distribution[node.ID]++
		}
	}
	
	fmt.Printf("Distribution of %d keys across nodes:\n", keyCount)
	totalKeys := 0
	for nodeID, count := range distribution {
		percentage := float64(count) / float64(keyCount) * 100
		fmt.Printf("Node %s: %d keys (%.1f%%)\n", nodeID, count, percentage)
		totalKeys += count
	}
	
	// Calculate standard deviation to show balance
	mean := float64(totalKeys) / float64(len(distribution))
	variance := 0.0
	for _, count := range distribution {
		diff := float64(count) - mean
		variance += diff * diff
	}
	variance /= float64(len(distribution))
	stdDev := fmt.Sprintf("%.1f", variance)
	
	fmt.Printf("\nLoad balance metrics:\n")
	fmt.Printf("Average keys per node: %.1f\n", mean)
	fmt.Printf("Standard deviation: %s (lower is better)\n", stdDev)
} 