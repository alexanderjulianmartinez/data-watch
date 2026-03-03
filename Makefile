# Project variables
BINARY_NAME=datawatch
MAIN_PATH=./cmd/datawatch
BUILD_DIR=./bin
COVERAGE_FILE=coverage.out

# Go commands
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=gofmt
GOVET=$(GOCMD) vet

# Build flags
LDFLAGS=-ldflags "-s -w"

.PHONY: all build clean test coverage lint fmt vet tidy help install run

# Default target
all: clean fmt vet lint test build

# Build the binary
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) $(MAIN_PATH)
	@echo "Build complete: $(BUILD_DIR)/$(BINARY_NAME)"

# Install dependencies
install:
	@echo "Installing dependencies..."
	$(GOGET) -v ./...
	$(GOMOD) download

# Run the application
run: build
	@echo "Running $(BINARY_NAME)..."
	$(BUILD_DIR)/$(BINARY_NAME)

# Clean build artifacts
clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	@rm -rf $(BUILD_DIR)
	@rm -f $(COVERAGE_FILE)
	@echo "Clean complete"

# Run tests
test:
	@echo "Running tests..."
	$(GOTEST) -v -race ./...

# Run tests with coverage
coverage:
	@echo "Running tests with coverage..."
	$(GOTEST) -v -race -coverprofile=$(COVERAGE_FILE) -covermode=atomic ./...
	@echo "Coverage report generated: $(COVERAGE_FILE)"
	@$(GOCMD) tool cover -func=$(COVERAGE_FILE)

# View coverage in browser
coverage-html: coverage
	@echo "Opening coverage report in browser..."
	@$(GOCMD) tool cover -html=$(COVERAGE_FILE)

# Run linter
lint:
	@echo "Running linter..."
	@which golangci-lint > /dev/null || (echo "golangci-lint not found. Installing..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)
	@golangci-lint run --timeout=5m ./...

# Format code
fmt:
	@echo "Formatting code..."
	@$(GOFMT) -w -s .
	@echo "Format complete"

# Check code formatting
fmt-check:
	@echo "Checking code formatting..."
	@test -z "$$($(GOFMT) -l .)" || (echo "Files need formatting. Run 'make fmt'" && $(GOFMT) -l . && exit 1)

# Run go vet
vet:
	@echo "Running go vet..."
	@$(GOVET) ./...

# Tidy dependencies
tidy:
	@echo "Tidying dependencies..."
	@$(GOMOD) tidy
	@echo "Dependencies tidied"

# Verify dependencies
verify:
	@echo "Verifying dependencies..."
	@$(GOMOD) verify

# Update dependencies
update:
	@echo "Updating dependencies..."
	@$(GOGET) -u ./...
	@$(GOMOD) tidy

# Run all checks (for CI)
ci: fmt-check vet lint test

# Help target
help:
	@echo "Available targets:"
	@echo "  all          - Run clean, fmt, vet, lint, test, and build"
	@echo "  build        - Build the binary"
	@echo "  install      - Install dependencies"
	@echo "  run          - Build and run the application"
	@echo "  clean        - Remove build artifacts"
	@echo "  test         - Run tests"
	@echo "  coverage     - Run tests with coverage"
	@echo "  coverage-html - View coverage report in browser"
	@echo "  lint         - Run golangci-lint"
	@echo "  fmt          - Format code"
	@echo "  fmt-check    - Check code formatting"
	@echo "  vet          - Run go vet"
	@echo "  tidy         - Tidy dependencies"
	@echo "  verify       - Verify dependencies"
	@echo "  update       - Update dependencies"
	@echo "  ci           - Run all checks (for CI)"
	@echo "  help         - Show this help message"
