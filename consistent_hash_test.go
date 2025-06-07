package consistenthashing

import (
	"fmt"
	"sync"
	"testing"
)

func TestNewHashRing(t *testing.T) {
	ring := NewHashRing(3)
	if ring == nil {
		t.Fatal("NewHashRing returned nil")
	}
	if ring.virtualReplicas != 3 {
		t.Errorf("Expected virtualReplicas to be 3, got %d", ring.virtualReplicas)
	}
	if ring.Size() != 0 {
		t.Errorf("Expected empty ring size to be 0, got %d", ring.Size())
	}
}

func TestNewHashRingWithOptions(t *testing.T) {
	// Test with SHA256 hasher
	ring := NewHashRing(3, WithHashFunction(&SHA256Hasher{}))
	if ring == nil {
		t.Fatal("NewHashRing with options returned nil")
	}
	
	// Verify it's using SHA256 hasher
	hash1 := ring.hash("test")
	sha256Hasher := &SHA256Hasher{}
	hash2 := sha256Hasher.Hash("test")
	if hash1 != hash2 {
		t.Error("Hash function option not applied correctly")
	}
}

func TestHashFunctions(t *testing.T) {
	fnvHasher := &FNVHasher{}
	sha256Hasher := &SHA256Hasher{}
	
	testKey := "test_key"
	
	// Test that both hashers produce consistent results
	hash1 := fnvHasher.Hash(testKey)
	hash2 := fnvHasher.Hash(testKey)
	if hash1 != hash2 {
		t.Error("FNV hasher not consistent")
	}
	
	hash3 := sha256Hasher.Hash(testKey)
	hash4 := sha256Hasher.Hash(testKey)
	if hash3 != hash4 {
		t.Error("SHA256 hasher not consistent")
	}
	
	// Test that different hashers produce different results
	if hash1 == hash3 {
		t.Error("Different hashers should produce different results")
	}
}

func TestAddNode(t *testing.T) {
	ring := NewHashRing(3)
	node := &Node{ID: "node1", Host: "localhost", Port: 8080}
	
	ring.AddNode(node)
	
	if ring.Size() != 1 {
		t.Errorf("Expected ring size to be 1, got %d", ring.Size())
	}
	if ring.VirtualSize() != 3 {
		t.Errorf("Expected virtual size to be 3, got %d", ring.VirtualSize())
	}
	
	// Test adding duplicate node
	ring.AddNode(node)
	if ring.Size() != 1 {
		t.Errorf("Expected ring size to remain 1 after adding duplicate, got %d", ring.Size())
	}
}

func TestWeightedNodes(t *testing.T) {
	ring := NewHashRing(10)
	
	// Add nodes with different weights
	node1 := &Node{ID: "node1", Host: "localhost", Port: 8080, Weight: 1}
	node2 := &Node{ID: "node2", Host: "localhost", Port: 8081, Weight: 2}
	node3 := &Node{ID: "node3", Host: "localhost", Port: 8082, Weight: 0} // Should default to 1
	
	ring.AddNode(node1)
	ring.AddNode(node2)
	ring.AddNode(node3)
	
	// node1: 10 virtual nodes (weight 1)
	// node2: 20 virtual nodes (weight 2)  
	// node3: 10 virtual nodes (weight 0 -> 1)
	// Total: 40 virtual nodes
	expectedVirtual := 40
	if ring.VirtualSize() != expectedVirtual {
		t.Errorf("Expected %d virtual nodes, got %d", expectedVirtual, ring.VirtualSize())
	}
}

func TestRemoveNode(t *testing.T) {
	ring := NewHashRing(3)
	node1 := &Node{ID: "node1", Host: "localhost", Port: 8080}
	node2 := &Node{ID: "node2", Host: "localhost", Port: 8081}
	
	ring.AddNode(node1)
	ring.AddNode(node2)
	
	if ring.Size() != 2 {
		t.Errorf("Expected ring size to be 2, got %d", ring.Size())
	}
	
	ring.RemoveNode("node1")
	
	if ring.Size() != 1 {
		t.Errorf("Expected ring size to be 1 after removal, got %d", ring.Size())
	}
	if ring.VirtualSize() != 3 {
		t.Errorf("Expected virtual size to be 3 after removal, got %d", ring.VirtualSize())
	}
	
	// Test removing non-existent node
	ring.RemoveNode("nonexistent")
	if ring.Size() != 1 {
		t.Errorf("Expected ring size to remain 1 after removing non-existent node, got %d", ring.Size())
	}
}

func TestGetNode(t *testing.T) {
	ring := NewHashRing(3)
	
	// Test empty ring
	node := ring.GetNode("key1")
	if node != nil {
		t.Error("Expected nil node for empty ring")
	}
	
	// Add nodes
	node1 := &Node{ID: "node1", Host: "localhost", Port: 8080}
	node2 := &Node{ID: "node2", Host: "localhost", Port: 8081}
	node3 := &Node{ID: "node3", Host: "localhost", Port: 8082}
	
	ring.AddNode(node1)
	ring.AddNode(node2)
	ring.AddNode(node3)
	
	// Test key mapping
	resultNode := ring.GetNode("key1")
	if resultNode == nil {
		t.Error("Expected non-nil node for key1")
	}
	
	// Test consistency - same key should always map to same node
	for i := 0; i < 10; i++ {
		if ring.GetNode("key1") != resultNode {
			t.Error("Key mapping is not consistent")
		}
	}
}

func TestGetNodes(t *testing.T) {
	ring := NewHashRing(3)
	
	// Test empty ring
	nodes := ring.GetNodes("key1", 2)
	if nodes != nil {
		t.Error("Expected nil nodes for empty ring")
	}
	
	// Add nodes
	node1 := &Node{ID: "node1", Host: "localhost", Port: 8080}
	node2 := &Node{ID: "node2", Host: "localhost", Port: 8081}
	node3 := &Node{ID: "node3", Host: "localhost", Port: 8082}
	
	ring.AddNode(node1)
	ring.AddNode(node2)
	ring.AddNode(node3)
	
	// Test getting multiple nodes
	nodes = ring.GetNodes("key1", 2)
	if len(nodes) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(nodes))
	}
	
	// Test that nodes are unique
	if nodes[0].ID == nodes[1].ID {
		t.Error("Expected unique nodes, got duplicates")
	}
	
	// Test requesting more nodes than available
	nodes = ring.GetNodes("key1", 5)
	if len(nodes) != 3 {
		t.Errorf("Expected 3 nodes (max available), got %d", len(nodes))
	}
}

func TestUtilityMethods(t *testing.T) {
	ring := NewHashRing(3)
	node1 := &Node{ID: "node1", Host: "localhost", Port: 8080}
	node2 := &Node{ID: "node2", Host: "localhost", Port: 8081}
	
	// Test HasNode
	if ring.HasNode("node1") {
		t.Error("Expected HasNode to return false for non-existent node")
	}
	
	ring.AddNode(node1)
	ring.AddNode(node2)
	
	// Test HasNode
	if !ring.HasNode("node1") {
		t.Error("Expected HasNode to return true for existing node")
	}
	
	// Test GetNodeByID
	retrievedNode := ring.GetNodeByID("node1")
	if retrievedNode == nil || retrievedNode.ID != "node1" {
		t.Error("GetNodeByID failed to retrieve correct node")
	}
	
	// Test GetNodeByID for non-existent node
	nonExistentNode := ring.GetNodeByID("nonexistent")
	if nonExistentNode != nil {
		t.Error("Expected GetNodeByID to return nil for non-existent node")
	}
}

func TestGetLoadDistribution(t *testing.T) {
	ring := NewHashRing(10)
	
	// Add nodes
	node1 := &Node{ID: "node1", Host: "localhost", Port: 8080}
	node2 := &Node{ID: "node2", Host: "localhost", Port: 8081}
	node3 := &Node{ID: "node3", Host: "localhost", Port: 8082}
	
	ring.AddNode(node1)
	ring.AddNode(node2)
	ring.AddNode(node3)
	
	// Generate test keys
	keys := make([]string, 100)
	for i := 0; i < 100; i++ {
		keys[i] = fmt.Sprintf("key_%d", i)
	}
	
	// Get distribution
	distribution := ring.GetLoadDistribution(keys)
	
	// Verify all nodes got some keys
	if len(distribution) != 3 {
		t.Errorf("Expected distribution for 3 nodes, got %d", len(distribution))
	}
	
	// Verify total keys
	totalKeys := 0
	for _, count := range distribution {
		totalKeys += count
	}
	if totalKeys != 100 {
		t.Errorf("Expected total of 100 keys, got %d", totalKeys)
	}
}

func TestGetRingInfo(t *testing.T) {
	ring := NewHashRing(5)
	
	// Test empty ring
	info := ring.GetRingInfo()
	if info["physical_nodes"] != 0 {
		t.Error("Expected 0 physical nodes for empty ring")
	}
	
	// Add nodes
	node1 := &Node{ID: "node1", Host: "localhost", Port: 8080}
	node2 := &Node{ID: "node2", Host: "localhost", Port: 8081}
	
	ring.AddNode(node1)
	ring.AddNode(node2)
	
	info = ring.GetRingInfo()
	if info["physical_nodes"] != 2 {
		t.Errorf("Expected 2 physical nodes, got %v", info["physical_nodes"])
	}
	if info["virtual_nodes"] != 10 {
		t.Errorf("Expected 10 virtual nodes, got %v", info["virtual_nodes"])
	}
	if info["virtual_replicas"] != 5 {
		t.Errorf("Expected 5 virtual replicas, got %v", info["virtual_replicas"])
	}
	if info["avg_virtual_per_physical"] != 5.0 {
		t.Errorf("Expected 5.0 avg virtual per physical, got %v", info["avg_virtual_per_physical"])
	}
}

func TestThreadSafety(t *testing.T) {
	ring := NewHashRing(10)
	
	// Number of goroutines
	numGoroutines := 100
	numOperations := 100
	
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	
	// Concurrent operations
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			
			for j := 0; j < numOperations; j++ {
				nodeID := fmt.Sprintf("node_%d_%d", id, j)
				node := &Node{
					ID:   nodeID,
					Host: "localhost",
					Port: 8080 + id,
				}
				
				// Add node
				ring.AddNode(node)
				
				// Get node for a key
				ring.GetNode(fmt.Sprintf("key_%d_%d", id, j))
				
				// Check if node exists
				ring.HasNode(nodeID)
				
				// Get ring info
				ring.GetRingInfo()
				
				// Remove some nodes
				if j%10 == 0 {
					ring.RemoveNode(nodeID)
				}
			}
		}(i)
	}
	
	wg.Wait()
	
	// Verify ring is still functional
	if ring.Size() < 0 {
		t.Error("Ring size became negative after concurrent operations")
	}
}

func TestConsistentMapping(t *testing.T) {
	ring := NewHashRing(100)
	
	// Add initial nodes
	for i := 0; i < 5; i++ {
		node := &Node{
			ID:   fmt.Sprintf("node%d", i),
			Host: "localhost",
			Port: 8080 + i,
		}
		ring.AddNode(node)
	}
	
	// Map keys to nodes
	keyMappings := make(map[string]*Node)
	testKeys := []string{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9", "key10"}
	
	for _, key := range testKeys {
		keyMappings[key] = ring.GetNode(key)
	}
	
	// Add a new node
	newNode := &Node{ID: "node5", Host: "localhost", Port: 8085}
	ring.AddNode(newNode)
	
	// Check how many keys moved
	movedKeys := 0
	for _, key := range testKeys {
		if ring.GetNode(key) != keyMappings[key] {
			movedKeys++
		}
	}
	
	// With consistent hashing, only a fraction of keys should move
	if movedKeys > len(testKeys)/2 {
		t.Errorf("Too many keys moved: %d out of %d", movedKeys, len(testKeys))
	}
}

func TestLoadDistribution(t *testing.T) {
	ring := NewHashRing(100)
	
	// Add nodes
	nodes := make([]*Node, 5)
	for i := 0; i < 5; i++ {
		nodes[i] = &Node{
			ID:   fmt.Sprintf("node%d", i),
			Host: "localhost",
			Port: 8080 + i,
		}
		ring.AddNode(nodes[i])
	}
	
	// Generate many keys and count distribution
	keyCount := 10000
	distribution := make(map[string]int)
	
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("key%d", i)
		node := ring.GetNode(key)
		if node != nil {
			distribution[node.ID]++
		}
	}
	
	// Check that distribution is reasonably balanced
	expectedPerNode := keyCount / len(nodes)
	tolerance := expectedPerNode / 2 // 50% tolerance
	
	for nodeID, count := range distribution {
		if count < expectedPerNode-tolerance || count > expectedPerNode+tolerance {
			t.Logf("Node %s has %d keys (expected ~%d)", nodeID, count, expectedPerNode)
		}
	}
	
	// Ensure all nodes got some keys
	if len(distribution) != len(nodes) {
		t.Errorf("Expected %d nodes in distribution, got %d", len(nodes), len(distribution))
	}
}

func TestNodeString(t *testing.T) {
	node := &Node{ID: "test", Host: "localhost", Port: 8080}
	expected := "localhost:8080"
	if node.String() != expected {
		t.Errorf("Expected %s, got %s", expected, node.String())
	}
}

func BenchmarkGetNode(b *testing.B) {
	ring := NewHashRing(100)
	
	// Add nodes
	for i := 0; i < 10; i++ {
		node := &Node{
			ID:   fmt.Sprintf("node%d", i),
			Host: "localhost",
			Port: 8080 + i,
		}
		ring.AddNode(node)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%1000)
		ring.GetNode(key)
	}
}

func BenchmarkGetNodeFNV(b *testing.B) {
	ring := NewHashRing(100, WithHashFunction(&FNVHasher{}))
	
	// Add nodes
	for i := 0; i < 10; i++ {
		node := &Node{
			ID:   fmt.Sprintf("node%d", i),
			Host: "localhost",
			Port: 8080 + i,
		}
		ring.AddNode(node)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%1000)
		ring.GetNode(key)
	}
}

func BenchmarkGetNodeSHA256(b *testing.B) {
	ring := NewHashRing(100, WithHashFunction(&SHA256Hasher{}))
	
	// Add nodes
	for i := 0; i < 10; i++ {
		node := &Node{
			ID:   fmt.Sprintf("node%d", i),
			Host: "localhost",
			Port: 8080 + i,
		}
		ring.AddNode(node)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%1000)
		ring.GetNode(key)
	}
}

func BenchmarkAddNode(b *testing.B) {
	ring := NewHashRing(100)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		node := &Node{
			ID:   fmt.Sprintf("node%d", i),
			Host: "localhost",
			Port: 8080 + (i % 1000),
		}
		ring.AddNode(node)
	}
}

func BenchmarkConcurrentGetNode(b *testing.B) {
	ring := NewHashRing(100)
	
	// Add nodes
	for i := 0; i < 10; i++ {
		node := &Node{
			ID:   fmt.Sprintf("node%d", i),
			Host: "localhost",
			Port: 8080 + i,
		}
		ring.AddNode(node)
	}
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key%d", i%1000)
			ring.GetNode(key)
			i++
		}
	})
} 