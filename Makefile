.PHONY: build test lint fmt docker clean validate help

# Build variables
BINARY    := kafkamcp
CMD       := ./cmd/kafkamcp
BUILD_DIR := bin
LDFLAGS   := -s -w

# Docker variables
DOCKER_IMAGE := kafkamcp/kafkamcp
DOCKER_TAG   := latest

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

build: ## Build the binary
	go build -trimpath -ldflags="$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY) $(CMD)

test: ## Run tests with race detection
	go test -race -coverprofile=coverage.out ./...

lint: ## Run golangci-lint
	golangci-lint run ./...

fmt: ## Format code
	gofmt -s -w .
	goimports -w . 2>/dev/null || true

docker: ## Build Docker image
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .

clean: ## Remove build artifacts
	rm -rf $(BUILD_DIR) coverage.out

validate: fmt lint test build ## Run all checks (fmt, lint, test, build)
