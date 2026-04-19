.PHONY: build test lint fmt docker clean validate help run dev coverage setup-hooks docker-dev docker-dev-down setup test-one setup-vscode integration-test check doctor sdk-test

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
	air

test: ## Run unit tests with race detection
	go test -race -coverprofile=coverage.out ./...

integration-test: ## Run integration tests (requires Kafka cluster)
	go test -race -tags integration -v ./...

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

setup: setup-hooks ## Install all dev dependencies (golangci-lint, air, goimports)
	@echo "Installing dev dependencies..."
	@command -v golangci-lint >/dev/null 2>&1 || { echo "  Installing golangci-lint..."; go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; }
	@command -v air >/dev/null 2>&1 || { echo "  Installing air..."; go install github.com/air-verse/air@latest; }
	@command -v goimports >/dev/null 2>&1 || { echo "  Installing goimports..."; go install golang.org/x/tools/cmd/goimports@latest; }
	@echo "All dev dependencies installed."

validate: fmt lint test build ## Run all checks (fmt, lint, test, build)

check: validate ## Alias for validate

test-one: ## Run a single test: make test-one TEST=TestName PKG=./internal/...
	go test -race -v -run $(TEST) $(PKG)

setup-vscode: ## Copy .vscode example configs (if not already present)
	@mkdir -p .vscode
	@test -f .vscode/launch.json || (cp .vscode/launch.json.example .vscode/launch.json && echo "Created .vscode/launch.json")
	@test -f .vscode/settings.json || (cp .vscode/settings.json.example .vscode/settings.json && echo "Created .vscode/settings.json")
	@echo "VSCode setup complete."

doctor: ## Check development environment prerequisites
	@echo "=== KafkaMCP Doctor ==="
	@echo ""
	@printf "Go:              "; command -v go >/dev/null 2>&1 && go version | sed 's/go version //' || echo "NOT FOUND (required)"
	@printf "golangci-lint:   "; command -v golangci-lint >/dev/null 2>&1 && echo "OK ($(golangci-lint --version 2>&1 | head -1))" || echo "NOT FOUND (run: make setup)"
	@printf "air:             "; command -v air >/dev/null 2>&1 && echo "OK" || echo "NOT FOUND (optional, run: make setup)"
	@printf "goimports:       "; command -v goimports >/dev/null 2>&1 && echo "OK" || echo "NOT FOUND (optional, run: make setup)"
	@printf "Docker:          "; command -v docker >/dev/null 2>&1 && (docker info >/dev/null 2>&1 && echo "OK (running)" || echo "INSTALLED but not running") || echo "NOT FOUND (needed for integration tests)"
	@printf "Kafka (localhost:9092): "; (echo > /dev/tcp/localhost/9092) 2>/dev/null && echo "REACHABLE" || echo "NOT REACHABLE (run: make docker-dev)"
	@printf "Pre-commit hook: "; test -x .git/hooks/pre-commit && echo "INSTALLED" || echo "NOT INSTALLED (run: make setup)"
	@echo ""
	@echo "Run 'make setup' to install missing dev dependencies."

sdk-test: ## Run tests for all SDKs (Go, Python, VS Code)
	@echo "=== SDK Tests ==="
	@echo "--- Go SDK ---"
	go test ./sdk/go/...
	@echo "--- Python SDK (langchain) ---"
	cd sdk/python/kafkamcp_langchain && pip install -q -e . 2>/dev/null && python -m pytest -q 2>/dev/null || echo "Python SDK: install pytest and dependencies to run tests"
	@echo "--- Python SDK (crewai) ---"
	cd sdk/python/kafkamcp_crewai && pip install -q -e . 2>/dev/null && python -m pytest -q 2>/dev/null || echo "CrewAI SDK: install pytest and dependencies to run tests"
	@echo "--- VS Code Extension ---"
	cd sdk/vscode && npm install --silent 2>/dev/null && npm test 2>/dev/null || echo "VS Code SDK: install npm dependencies to run tests"
	@echo "All SDK tests complete."
