package main

import (
	"fmt"
	"log"

	"github.com/alexnthnz/consistent-hashing"
)

func main() {
	// Create a new hash ring with 100 virtual nodes per physical node
	ring, err := consistenthashing.NewHashRing(100)
	if err != nil {
		log.Fatalf("Failed to create hash ring: %v", err)
	}

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
		if err := ring.AddNode(node); err != nil {
			log.Printf("Failed to add node %s: %v", node.ID, err)
			continue
		}
		fmt.Printf("Added node: %s (%s)\n", node.ID, node.String())
	}

	fmt.Printf("\nHash ring now has %d physical nodes and %d virtual nodes\n\n", 
		ring.Size(), ring.VirtualSize())

	// Demonstrate key-to-node mapping
	testKeys := []string{"user:1001", "user:1002", "user:1003", "user:1004", "user:1005"}
	
	fmt.Println("Key-to-node mapping:")
	for _, key := range testKeys {
		node, err := ring.GetNode(key)
		if err != nil {
			log.Printf("Failed to get node for key %s: %v", key, err)
			continue
		}
		if node != nil {
			fmt.Printf("Key '%s' -> Node '%s' (%s)\n", key, node.ID, node.String())
		}
	}

	// Demonstrate replication (getting multiple nodes for a key)
	fmt.Println("\nReplication example (3 replicas for each key):")
	for _, key := range testKeys[:2] { // Just show first 2 keys
		nodes, err := ring.GetNodes(key, 3)
		if err != nil {
			log.Printf("Failed to get nodes for key %s: %v", key, err)
			continue
		}
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
		node, err := ring.GetNode(key)
		if err != nil {
			log.Printf("Failed to get node for key %s: %v", key, err)
			continue
		}
		originalMappings[key] = node
	}

	// Add a new node
	newNode := &consistenthashing.Node{ID: "server5", Host: "192.168.1.14", Port: 8080}
	if err := ring.AddNode(newNode); err != nil {
		log.Printf("Failed to add new node: %v", err)
	} else {
		fmt.Printf("Added new node: %s (%s)\n", newNode.ID, newNode.String())
		fmt.Printf("Hash ring now has %d physical nodes\n\n", ring.Size())
	}

	// Check which keys moved
	fmt.Println("Key mapping changes after adding new node:")
	movedKeys := 0
	for _, key := range testKeys {
		newNode, err := ring.GetNode(key)
		if err != nil {
			log.Printf("Failed to get node for key %s: %v", key, err)
			continue
		}
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
	if err := ring.RemoveNode("server2"); err != nil {
		log.Printf("Failed to remove node: %v", err)
	} else {
		fmt.Printf("Removed node: server2\n")
		fmt.Printf("Hash ring now has %d physical nodes\n\n", ring.Size())
	}

	fmt.Println("Key mappings after node removal:")
	for _, key := range testKeys {
		node, err := ring.GetNode(key)
		if err != nil {
			log.Printf("Failed to get node for key %s: %v", key, err)
			continue
		}
		if node != nil {
			fmt.Printf("Key '%s' -> Node '%s'\n", key, node.ID)
		}
	}

	// Show load distribution
	fmt.Println("\n--- Load Distribution Analysis ---")
	analyzeLoadDistribution(ring)

	// Demonstrate error handling
	fmt.Println("\n--- Error Handling Examples ---")
	demonstrateErrorHandling()

	// Validate ring integrity
	fmt.Println("\n--- Ring Validation ---")
	if err := ring.ValidateRing(); err != nil {
		fmt.Printf("Ring validation failed: %v\n", err)
	} else {
		fmt.Println("Ring validation passed!")
	}
}

func analyzeLoadDistribution(ring *consistenthashing.HashRing) {
	// Generate many keys and see how they distribute
	keyCount := 10000
	keys := make([]string, keyCount)
	for i := 0; i < keyCount; i++ {
		keys[i] = fmt.Sprintf("key_%d", i)
	}
	
	distribution, err := ring.GetLoadDistribution(keys)
	if err != nil {
		log.Printf("Failed to get load distribution: %v", err)
		return
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

func demonstrateErrorHandling() {
	// Try to create ring with invalid parameters
	_, err := consistenthashing.NewHashRing(0)
	fmt.Printf("Creating ring with 0 virtual replicas: %v\n", err)

	// Create a valid ring
	ring, err := consistenthashing.NewHashRing(10)
	if err != nil {
		log.Printf("Failed to create ring: %v", err)
		return
	}

	// Try to add invalid node
	invalidNode := &consistenthashing.Node{ID: "", Host: "localhost", Port: 8080}
	err = ring.AddNode(invalidNode)
	fmt.Printf("Adding node with empty ID: %v\n", err)

	// Try to add nil node
	err = ring.AddNode(nil)
	fmt.Printf("Adding nil node: %v\n", err)

	// Try to get node with empty key
	_, err = ring.GetNode("")
	fmt.Printf("Getting node with empty key: %v\n", err)

	// Try to remove non-existent node
	err = ring.RemoveNode("nonexistent")
	fmt.Printf("Removing non-existent node: %v\n", err)

	// Try to get node by invalid ID
	_, err = ring.GetNodeByID("")
	fmt.Printf("Getting node with empty ID: %v\n", err)
} 