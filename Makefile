.PHONY: build test lint fmt docker clean validate help run dev coverage setup-hooks docker-dev docker-dev-down setup test-one

# Build variables
BINARY    := kafkamcp
CMD       := ./cmd/kafkamcp
BUILD_DIR := bin
VERSION   := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS   := -s -w -X main.version=$(VERSION)

# Docker variables
DOCKER_IMAGE := kafkamcp/kafkamcp
DOCKER_TAG   := latest

# Docker Compose variables
COMPOSE_FILE := examples/docker-compose.yaml

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

build: ## Build the binary
	go build -trimpath -ldflags="$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY) $(CMD)

run: build ## Build and run with default config
	$(BUILD_DIR)/$(BINARY) --config kafkamcp.yaml

dev: ## Run with auto-reload (requires air: go install github.com/air-verse/air@latest)
	@command -v air >/dev/null 2>&1 || { echo "Installing air..."; go install github.com/air-verse/air@latest; }
	air --build.cmd "go build -trimpath -ldflags=\"$(LDFLAGS)\" -o $(BUILD_DIR)/$(BINARY) $(CMD)" \
	    --build.bin "$(BUILD_DIR)/$(BINARY)" \
	    --build.args_bin "--config,kafkamcp.yaml" \
	    --build.exclude_dir "website,examples,bin,blog"

test: ## Run tests with race detection
	go test -race -coverprofile=coverage.out ./...

coverage: test ## Generate and open HTML coverage report
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"
	@command -v open >/dev/null 2>&1 && open coverage.html || command -v xdg-open >/dev/null 2>&1 && xdg-open coverage.html || true

lint: ## Run golangci-lint
	@command -v golangci-lint >/dev/null 2>&1 || { echo "Installing golangci-lint..."; go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; }
	golangci-lint run ./...

fmt: ## Format code
	gofmt -s -w .
	goimports -w . 2>/dev/null || true

docker: ## Build Docker image
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .

docker-dev: ## Start local Kafka + KafkaMCP dev stack
	docker compose -f $(COMPOSE_FILE) up -d

docker-dev-down: ## Stop local dev stack
	docker compose -f $(COMPOSE_FILE) down

clean: ## Remove build artifacts
	rm -rf $(BUILD_DIR) coverage.out coverage.html

setup-hooks: ## Install git pre-commit hook running make validate
	@mkdir -p .git/hooks
	@echo '#!/bin/sh' > .git/hooks/pre-commit
	@echo 'make validate' >> .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "Pre-commit hook installed."

setup: ## Install all dev dependencies (golangci-lint, air, goimports)
	@echo "Installing dev dependencies..."
	@command -v golangci-lint >/dev/null 2>&1 || { echo "  Installing golangci-lint..."; go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; }
	@command -v air >/dev/null 2>&1 || { echo "  Installing air..."; go install github.com/air-verse/air@latest; }
	@command -v goimports >/dev/null 2>&1 || { echo "  Installing goimports..."; go install golang.org/x/tools/cmd/goimports@latest; }
	@echo "All dev dependencies installed."

validate: fmt lint test build ## Run all checks (fmt, lint, test, build)

test-one: ## Run a single test: make test-one TEST=TestName PKG=./internal/...
	go test -race -v -run $(TEST) $(PKG)
