# Consistent Hashing

A lightweight, high-performance implementation of consistent hashing for efficient data distribution in distributed systems. Supports dynamic node addition/removal, weighted nodes, multiple hash functions, and comprehensive monitoring. Ideal for distributed databases, load balancers, and caching systems.

## ✨ Features

- **🔄 Hash Ring Implementation**: Maps keys to nodes using a hash ring with minimal data movement
- **⚖️ Weighted Nodes**: Support for weighted consistent hashing based on node capacity
- **🔧 Pluggable Hash Functions**: Choose between FNV (fast) and SHA-256 (secure) hash functions
- **🚀 Dynamic Scaling**: Handles dynamic node addition and removal seamlessly
- **⚡ High Performance**: Optimized with binary search for O(log n) key lookups
- **🔄 Replication Support**: Built-in support for data replication across multiple nodes
- **🧵 Thread Safety**: Full concurrent access support with RWMutex protection
- **📊 Monitoring & Analytics**: Built-in load distribution analysis and ring statistics
- **🧪 Comprehensive Testing**: Includes unit tests, benchmarks, and concurrency tests
- **🏭 Production Ready**: Extensively tested for reliability and performance

## 📦 Installation

```bash
go get github.com/alexnthnz/consistent-hashing
```

## 🚀 Quick Start

```go
package main

import (
    "fmt"
    "github.com/alexnthnz/consistent-hashing"
)

func main() {
    // Create a hash ring with 100 virtual nodes per physical node
    ring := consistenthashing.NewHashRing(100)
    
    // Add nodes with different weights
    ring.AddNode(&consistenthashing.Node{
        ID:     "server1",
        Host:   "192.168.1.10",
        Port:   8080,
        Weight: 1, // Standard capacity
    })
    
    ring.AddNode(&consistenthashing.Node{
        ID:     "server2", 
        Host:   "192.168.1.11",
        Port:   8080,
        Weight: 2, // Double capacity
    })
    
    // Find which node should handle a key
    node := ring.GetNode("user:1001")
    fmt.Printf("Key 'user:1001' -> Node: %s (%s)\n", node.ID, node.String())
    
    // Get multiple nodes for replication
    nodes := ring.GetNodes("user:1001", 2)
    fmt.Printf("Replication nodes: %v\n", nodes)
}
```

## 🏗️ Project Structure

```
consistent-hashing/
├── 📄 consistent_hash.go          # Core implementation
├── 🧪 consistent_hash_test.go     # Comprehensive tests  
├── 📁 examples/
│   ├── 🚀 basic_usage.go         # Basic usage demonstration
│   ├── 🏗️ distributed_cache.go   # Advanced distributed cache example
│   └── ⚡ advanced_features.go   # Advanced features showcase
├── ⚙️ Makefile                   # Build and development commands
├── 📦 go.mod                     # Go module definition
├── 📖 README.md                  # This file
└── 🚫 .gitignore                # Git ignore rules
```

## 📚 API Reference

### Types

#### `Node`
Represents a physical node in the distributed system.

```go
type Node struct {
    ID     string  // Unique identifier
    Host   string  // Host address
    Port   int     // Port number
    Weight int     // Weight for load balancing (default: 1)
}
```

#### `HashRing`
The main consistent hash ring structure with thread-safe operations.

#### `HashFunction`
Interface for pluggable hash functions.

```go
type HashFunction interface {
    Hash(key string) uint32
}
```

### Functions

#### `NewHashRing(virtualReplicas int, opts ...Option) *HashRing`
Creates a new hash ring with options.

```go
// Default ring with FNV hash
ring := consistenthashing.NewHashRing(100)

// Ring with SHA-256 hash
ring := consistenthashing.NewHashRing(100, 
    consistenthashing.WithHashFunction(&consistenthashing.SHA256Hasher{}))
```

#### Core Operations
- `AddNode(node *Node)` - Adds a node (thread-safe)
- `RemoveNode(nodeID string)` - Removes a node (thread-safe)
- `GetNode(key string) *Node` - Gets responsible node (thread-safe)
- `GetNodes(key string, count int) []*Node` - Gets multiple nodes for replication

#### Utility Methods
- `HasNode(nodeID string) bool` - Checks if node exists
- `GetNodeByID(nodeID string) *Node` - Gets node by ID
- `GetAllNodes() []*Node` - Gets all nodes
- `Size() int` - Number of physical nodes
- `VirtualSize() int` - Number of virtual nodes

#### Analytics & Monitoring
- `GetLoadDistribution(keys []string) map[string]int` - Analyzes key distribution
- `GetRingInfo() map[string]interface{}` - Gets ring statistics

## 🎯 Examples

### Basic Usage
```bash
make run-basic
```

### Distributed Cache
```bash
make run-cache
```

### Advanced Features
```bash
make run-advanced
```

This demonstrates:
- Hash function comparison (FNV vs SHA-256)
- Weighted consistent hashing
- Thread safety and concurrent operations
- Monitoring and analytics

## 🛠️ Development

### Running Tests
```bash
# Run all tests
make test

# Run tests with race detection
make test-verbose

# Run benchmarks
make benchmark

# Generate coverage report
make coverage
```

### Code Quality
```bash
# Format code
make fmt

# Run static analysis
make vet

# Run all quality checks
make check
```

## ⚡ Performance

The implementation is optimized for high performance:

- **O(log n)** key lookup time using binary search
- **O(n log n)** node addition time (with sorting)
- **O(n)** node removal time (in-place filtering)
- **Thread-safe** with minimal lock contention
- **Memory efficient** data structures

### Benchmarks

Recent benchmark results on Apple M1:

```
BenchmarkGetNode-8              12864074    92.21 ns/op    13 B/op    1 allocs/op
BenchmarkGetNodeFNV-8           13238444    91.45 ns/op    13 B/op    1 allocs/op
BenchmarkGetNodeSHA256-8         8215366   142.9 ns/op     13 B/op    1 allocs/op
BenchmarkConcurrentGetNode-8     7179759   170.4 ns/op     13 B/op    1 allocs/op
```

**Key Insights:**
- **FNV hash is ~56% faster** than SHA-256 for lookups
- **Excellent concurrent performance** with minimal overhead
- **Low memory allocation** per operation

## 🎛️ Configuration

### Hash Functions

Choose the right hash function for your use case:

```go
// Fast hash (default) - good for most applications
ring := consistenthashing.NewHashRing(100)

// Secure hash - for cryptographic requirements
ring := consistenthashing.NewHashRing(100, 
    consistenthashing.WithHashFunction(&consistenthashing.SHA256Hasher{}))
```

### Virtual Nodes

Balance performance vs. distribution:

- **Low (10-50)**: Faster operations, acceptable imbalance
- **Medium (50-200)**: Good balance of performance and distribution  
- **High (200+)**: Better distribution, slightly slower operations

### Weighted Nodes

Distribute load based on node capacity:

```go
nodes := []*consistenthashing.Node{
    {ID: "small", Host: "host1", Port: 8080, Weight: 1},   // 1x capacity
    {ID: "large", Host: "host2", Port: 8080, Weight: 4},   // 4x capacity
}
```

## 🎯 Use Cases

### 🗄️ Distributed Caching
- **Redis Cluster**: Distribute cache keys across Redis instances
- **Memcached**: Load balance requests across Memcached servers
- **CDN**: Route requests to geographically distributed cache servers

### 🗃️ Database Sharding
- **Horizontal Partitioning**: Distribute data across database shards
- **Read Replicas**: Route read queries to appropriate replica servers
- **Time-series Data**: Distribute time-series data across multiple databases

### ⚖️ Load Balancing
- **API Gateway**: Route requests to backend services
- **Microservices**: Distribute service calls across instances
- **Message Queues**: Assign message processing to worker nodes

### 💾 Distributed Storage
- **Object Storage**: Distribute files across storage nodes
- **Blockchain**: Distribute blockchain data across nodes
- **P2P Networks**: Route data in peer-to-peer systems

## 🔍 Monitoring

The library provides comprehensive monitoring capabilities:

```go
// Get ring statistics
info := ring.GetRingInfo()
fmt.Printf("Physical nodes: %d\n", info["physical_nodes"])
fmt.Printf("Virtual nodes: %d\n", info["virtual_nodes"])

// Analyze load distribution
keys := []string{"key1", "key2", "key3"} // your keys
distribution := ring.GetLoadDistribution(keys)
for nodeID, count := range distribution {
    fmt.Printf("Node %s: %d keys\n", nodeID, count)
}
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests for new functionality
5. Run the test suite (`make test`)
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Inspired by the original consistent hashing paper by Karger et al.
- Built with Go's excellent standard library and concurrent primitives
- Tested extensively for production use cases
- Performance optimized based on real-world usage patterns