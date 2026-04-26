.PHONY: build test lint lint-fix fmt docker clean validate help run run-local dev dev-sse dev-http coverage setup-hooks docker-dev docker-dev-down setup test-one setup-vscode integration-test check doctor sdk-test quickstart benchmark docker-compose-full vet sdk-setup deps changelog test-coverage-check test-short

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

run-local: build ## Build and run without Docker (for remote Kafka clusters)
@echo "Starting KafkaMCP (no Docker required)..."
@echo "Configure your Kafka connection in kafkamcp.yaml"
$(BUILD_DIR)/$(BINARY) --config kafkamcp.yaml

dev: ## Run with auto-reload (requires air: go install github.com/air-verse/air@latest)
	@command -v air >/dev/null 2>&1 || { echo "Installing air..."; go install github.com/air-verse/air@latest; }
	air

dev-sse: ## Run with SSE transport and auto-reload
	@command -v air >/dev/null 2>&1 || { echo "Installing air..."; go install github.com/air-verse/air@latest; }
	KAFKAMCP_TRANSPORT=sse air

dev-http: ## Run with streamable-http transport and auto-reload
	@command -v air >/dev/null 2>&1 || { echo "Installing air..."; go install github.com/air-verse/air@latest; }
	KAFKAMCP_TRANSPORT=streamable-http air

test: ## Run unit tests with race detection
	go test -race -coverprofile=coverage.out ./...

integration-test: ## Run integration tests (requires Kafka cluster)
	go test -race -tags integration -v ./...

coverage: test ## Generate and open HTML coverage report
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"
	@command -v open >/dev/null 2>&1 && open coverage.html || command -v xdg-open >/dev/null 2>&1 && xdg-open coverage.html || true

MIN_COVERAGE ?= 70

test-coverage-check: test ## Fail if total test coverage is below MIN_COVERAGE (default: 70%)
	@total=$$(go tool cover -func=coverage.out | tail -1 | awk '{print $$NF}' | tr -d '%'); \
	echo "Total coverage: $${total}%"; \
	if [ $$(echo "$${total} < $(MIN_COVERAGE)" | bc -l) -eq 1 ]; then \
		echo "FAIL: coverage $${total}% is below minimum $(MIN_COVERAGE)%"; \
		exit 1; \
	fi; \
	echo "OK: coverage $${total}% meets minimum $(MIN_COVERAGE)%"

test-short: ## Run tests without -race for fast iteration (respects testing.Short)
	go test -short ./...

lint: ## Run golangci-lint
	@command -v golangci-lint >/dev/null 2>&1 || { echo "Installing golangci-lint..."; go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; }
	golangci-lint run ./...

lint-fix: ## Run golangci-lint with auto-fix
	@command -v golangci-lint >/dev/null 2>&1 || { echo "Installing golangci-lint..."; go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; }
	golangci-lint run --fix ./...

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

setup: setup-hooks setup-vscode ## Install all dev dependencies (golangci-lint, air, goimports)
	@echo "Installing dev dependencies..."
	@command -v golangci-lint >/dev/null 2>&1 || { echo "  Installing golangci-lint..."; go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; }
	@command -v air >/dev/null 2>&1 || { echo "  Installing air..."; go install github.com/air-verse/air@latest; }
	@command -v goimports >/dev/null 2>&1 || { echo "  Installing goimports..."; go install golang.org/x/tools/cmd/goimports@latest; }
	@echo "All dev dependencies installed."

validate: fmt vet lint test build ## Run all checks (fmt, vet, lint, test, build)

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
	@printf "Python 3.8+:     "; command -v python3 >/dev/null 2>&1 && echo "OK ($$(python3 --version 2>&1))" || echo "NOT FOUND (needed for SDK tests: make sdk-test)"
	@printf "Node.js 18+:     "; command -v node >/dev/null 2>&1 && echo "OK ($$(node --version 2>&1))" || echo "NOT FOUND (needed for SDK setup: make sdk-setup)"
	@echo ""
	@echo "Run 'make setup' to install missing dev dependencies."

quickstart: setup docker-dev build run ## One-command setup: install deps, start Kafka, build, and run

benchmark: ## Run Go benchmarks with memory stats
	go test -bench=. -benchmem -run=^$$ ./...

docker-compose-full: ## Start full Kafka + KafkaMCP stack
	docker compose -f examples/docker-compose-kafkamcp.yaml up -d

vet: ## Run go vet for static analysis
	go vet ./...

deps: ## Check for outdated and vulnerable Go dependencies
	@echo "=== Outdated Dependencies ==="
	@go list -m -u all 2>/dev/null | grep '\[' || echo "All dependencies up to date."
	@echo ""
	@echo "=== Vulnerability Check ==="
	@command -v govulncheck >/dev/null 2>&1 || { echo "Installing govulncheck..."; go install golang.org/x/vuln/cmd/govulncheck@latest; }
	govulncheck ./...

changelog: ## Generate CHANGELOG.md from Conventional Commits
	@command -v git-chglog >/dev/null 2>&1 || { echo "Installing git-chglog..."; go install github.com/git-chglog/git-chglog/cmd/git-chglog@latest; }
	git-chglog -o CHANGELOG.md
	@echo "CHANGELOG.md updated."

sdk-setup: ## Install SDK test dependencies (Python, Node.js)
	@echo "=== SDK Dependency Setup ==="
	@echo "Prerequisites: Python 3.8+, pip, Node.js 18+, npm"
	@echo "--- Python SDK (langchain) ---"
	cd sdk/python/kafkamcp_langchain && pip install -q -e ".[test]" 2>/dev/null || pip install -q -e . 2>/dev/null || echo "FAILED: pip install for kafkamcp_langchain"
	@echo "--- Python SDK (crewai) ---"
	cd sdk/python/kafkamcp_crewai && pip install -q -e ".[test]" 2>/dev/null || pip install -q -e . 2>/dev/null || echo "FAILED: pip install for kafkamcp_crewai"
	@echo "--- VS Code Extension ---"
	cd sdk/vscode && npm install --silent 2>/dev/null || echo "FAILED: npm install for vscode extension"
	@echo "SDK dependencies installed."

sdk-test: ## Run tests for all SDKs (Go, Python, VS Code)
	@echo "=== SDK Tests ==="
	@echo "--- Go SDK ---"
	go test ./sdk/go/...
	@echo "--- Python SDK (langchain) ---"
	cd sdk/python/kafkamcp_langchain && pip install -q -e . && python -m pytest -q
	@echo "--- Python SDK (crewai) ---"
	cd sdk/python/kafkamcp_crewai && pip install -q -e . && python -m pytest -q
	@echo "--- VS Code Extension ---"
	cd sdk/vscode && npm install --silent && npm test
	@echo "All SDK tests complete."
