.PHONY: build test clean run-basic run-cache run-advanced benchmark fmt vet lint help

# Default target
help:
	@echo "Available targets:"
	@echo "  build        - Build the project"
	@echo "  test         - Run all tests"
	@echo "  test-verbose - Run tests with verbose output"
	@echo "  benchmark    - Run benchmarks"
	@echo "  run-basic    - Run basic usage example"
	@echo "  run-cache    - Run distributed cache example"
	@echo "  run-advanced - Run advanced features example"
	@echo "  fmt          - Format code"
	@echo "  vet          - Run go vet"
	@echo "  lint         - Run golint (requires golint to be installed)"
	@echo "  clean        - Clean build artifacts"
	@echo "  coverage     - Generate test coverage report"
	@echo "  help         - Show this help message"

# Build the project
build:
	@echo "Building project..."
	go build -v ./...

# Run tests
test:
	@echo "Running tests..."
	go test -v .

# Run tests with verbose output and race detection
test-verbose:
	@echo "Running tests with race detection..."
	go test -v -race .

# Run benchmarks
benchmark:
	@echo "Running benchmarks..."
	go test -bench=. -benchmem .

# Run basic usage example
run-basic:
	@echo "Running basic usage example..."
	go run examples/basic_usage.go

# Run distributed cache example
run-cache:
	@echo "Running distributed cache example..."
	go run examples/distributed_cache.go

# Run advanced features example
run-advanced:
	@echo "Running advanced features example..."
	go run examples/advanced_features.go

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...

# Run go vet
vet:
	@echo "Running go vet..."
	go vet ./...

# Run golint (requires golint to be installed)
lint:
	@echo "Running golint..."
	@command -v golint >/dev/null 2>&1 || { echo "golint not installed. Run: go install golang.org/x/lint/golint@latest"; exit 1; }
	golint ./...

# Generate test coverage
coverage:
	@echo "Generating test coverage..."
	go test -coverprofile=coverage.out .
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	go clean ./...
	rm -f coverage.out coverage.html

# Install dependencies (if any)
deps:
	@echo "Installing dependencies..."
	go mod tidy
	go mod download

# Run all quality checks
check: fmt vet test
	@echo "All checks passed!"

# Quick development cycle
dev: fmt vet test run-basic
	@echo "Development cycle complete!" 