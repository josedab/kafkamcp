.PHONY: build test lint lint-fix lint-all fmt fmt-check docker clean clean-all validate validate-ci help run run-local dev dev-sse dev-http coverage test-coverage-report setup-hooks docker-dev docker-dev-down setup test-one setup-vscode integration-test check doctor sdk-test quickstart benchmark docker-compose-full vet sdk-setup deps changelog test-coverage-check test-short test-race-short test-all new-tool check-deps

# Build variables
BINARY    := kafkamcp
CMD       := ./cmd/kafkamcp
BUILD_DIR := bin
VERSION   := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS   := -s -w -X main.version=$(VERSION)

# Tool versions
GOLANGCI_LINT_VERSION := v1.62.0

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

test-coverage-report: test ## Print per-package coverage breakdown to stdout
	@echo "=== Per-Package Coverage ==="
	@go tool cover -func=coverage.out | \
		grep -E '^[a-z]' | \
		awk '{pkg=$$1; cov=$$NF} pkg!=prev {if(prev) printf "  %-60s %s\n", prev, last; prev=pkg} {last=cov} END {if(prev) printf "  %-60s %s\n", prev, last}' | \
		sort
	@echo ""
	@echo "=== Total ==="
	@go tool cover -func=coverage.out | tail -1 | awk '{printf "  %-60s %s\n", $$1, $$NF}'

test-short: ## Run tests without -race for fast iteration (respects testing.Short)
	go test -short ./...

test-race-short: ## Run tests with -race and -short for fast-but-safe iteration
	go test -race -short ./...

test-all: test sdk-test ## Run all tests (Go unit tests + SDK tests)

lint: ## Run golangci-lint
	@command -v golangci-lint >/dev/null 2>&1 || { echo "Installing golangci-lint..."; go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); }
	golangci-lint run ./...

lint-fix: ## Run golangci-lint with auto-fix
	@command -v golangci-lint >/dev/null 2>&1 || { echo "Installing golangci-lint..."; go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); }
	golangci-lint run --fix ./...

lint-all: vet lint ## Run all static analysis (vet + golangci-lint)

fmt: ## Format code
	gofmt -s -w .
	goimports -w . 2>/dev/null || true

fmt-check: ## Check code formatting (fails if unformatted, does not modify files)
	@unformatted=$$(gofmt -s -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "ERROR: The following files are not formatted:"; \
		echo "$$unformatted"; \
		echo "Run 'make fmt' to fix."; \
		exit 1; \
	fi; \
	echo "All files formatted correctly."

docker: ## Build Docker image
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .

docker-dev: ## Start local Kafka + KafkaMCP dev stack
	docker compose -f $(COMPOSE_FILE) up -d

docker-dev-down: ## Stop local dev stack
	docker compose -f $(COMPOSE_FILE) down

clean: ## Remove build artifacts
	rm -rf $(BUILD_DIR) coverage.out coverage.html

clean-all: clean ## Remove build artifacts and Docker dev volumes
	docker compose -f $(COMPOSE_FILE) down -v 2>/dev/null || true
	@echo "Cleaned build artifacts and Docker volumes."

setup-hooks: ## Install git pre-commit hook running make validate
	@mkdir -p .git/hooks
	@echo '#!/bin/sh' > .git/hooks/pre-commit
	@echo 'make validate' >> .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "Pre-commit hook installed."

setup: setup-hooks setup-vscode ## Install all dev dependencies (golangci-lint, air, goimports)
	@echo "Installing dev dependencies..."
	@command -v golangci-lint >/dev/null 2>&1 || { echo "  Installing golangci-lint..."; go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); }
	@command -v air >/dev/null 2>&1 || { echo "  Installing air..."; go install github.com/air-verse/air@latest; }
	@command -v goimports >/dev/null 2>&1 || { echo "  Installing goimports..."; go install golang.org/x/tools/cmd/goimports@latest; }
	@echo "All dev dependencies installed."

validate: fmt-check vet lint test build ## Run all checks (fmt-check, vet, lint, test, build)

validate-ci: fmt-check vet lint test build ## Read-only validation (safe for CI and pre-push)

check-deps: ## Check go.mod/go.sum are tidy
	go mod tidy
	@git diff --exit-code go.mod go.sum || { echo "ERROR: go.mod/go.sum are not tidy. Run 'go mod tidy' and commit the changes."; exit 1; }

check: validate ## Alias for validate

test-one: ## Run a single test: make test-one TEST=TestName PKG=./internal/...
	go test -race -v -run $(TEST) $(PKG)

setup-vscode: ## Copy .vscode example configs (if not already present)
	@mkdir -p .vscode
	@test -f .vscode/launch.json || (cp .vscode/launch.json.example .vscode/launch.json && echo "Created .vscode/launch.json")
	@test -f .vscode/settings.json || (cp .vscode/settings.json.example .vscode/settings.json && echo "Created .vscode/settings.json")
	@test -f .vscode/tasks.json || (cp .vscode/tasks.json.example .vscode/tasks.json && echo "Created .vscode/tasks.json")
	@echo "VSCode setup complete."

doctor: ## Check development environment prerequisites
	@echo "=== KafkaMCP Doctor ==="
	@echo ""
	@printf "Go:              "; command -v go >/dev/null 2>&1 && go version | sed 's/go version //' || echo "NOT FOUND (required)"
	@printf "golangci-lint:   "; \
	if command -v golangci-lint >/dev/null 2>&1; then \
		installed_ver=$$(golangci-lint --version 2>&1 | grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+' | head -1); \
		if [ "$$installed_ver" = "$(GOLANGCI_LINT_VERSION)" ]; then \
			echo "OK $$installed_ver"; \
		else \
			echo "WARNING: $$installed_ver installed, expected $(GOLANGCI_LINT_VERSION) (run: go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION))"; \
		fi; \
	else \
		echo "NOT FOUND (run: make setup)"; \
	fi
	@printf "air:             "; command -v air >/dev/null 2>&1 && echo "OK" || echo "NOT FOUND (optional, run: make setup)"
	@printf "goimports:       "; command -v goimports >/dev/null 2>&1 && echo "OK" || echo "NOT FOUND (optional, run: make setup)"
	@printf "Docker:          "; command -v docker >/dev/null 2>&1 && (docker info >/dev/null 2>&1 && echo "OK (running)" || echo "INSTALLED but not running") || echo "NOT FOUND (needed for integration tests)"
	@printf "Kafka (localhost:9092): "; (echo > /dev/tcp/localhost/9092) 2>/dev/null && echo "REACHABLE" || echo "NOT REACHABLE (run: make docker-dev)"
	@printf "Pre-commit hook: "; test -x .git/hooks/pre-commit && echo "INSTALLED" || echo "NOT INSTALLED (run: make setup)"
	@printf "Python 3.8+:     "; command -v python3 >/dev/null 2>&1 && echo "OK ($$(python3 --version 2>&1))" || echo "NOT FOUND (needed for SDK tests: make sdk-test)"
	@printf "Node.js 18+:     "; command -v node >/dev/null 2>&1 && echo "OK ($$(node --version 2>&1))" || echo "NOT FOUND (needed for SDK setup: make sdk-setup)"
	@echo ""
	@echo "Run 'make setup' to install missing dev dependencies."

quickstart: ## One-command setup: install deps, start Kafka, build, and run
	@command -v docker >/dev/null 2>&1 || { echo "ERROR: Docker is not installed."; echo "Install Docker from https://docs.docker.com/get-docker/ and try again."; echo "Alternatively, use 'make run-local' if you have a remote Kafka cluster."; exit 1; }
	@docker info >/dev/null 2>&1 || { echo "ERROR: Docker is not running."; echo "Start Docker Desktop (or the Docker daemon) and try again."; echo "Alternatively, use 'make run-local' if you have a remote Kafka cluster."; exit 1; }
	@$(MAKE) setup docker-dev build run

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

new-tool: ## Scaffold a new tool: make new-tool NAME=xyz
	@if [ -z "$(NAME)" ]; then echo "Usage: make new-tool NAME=<tool_name>"; exit 1; fi
	@if [ -d "internal/$(NAME)" ]; then echo "ERROR: internal/$(NAME) already exists"; exit 1; fi
	@if [ -f "internal/server/tools_$(NAME).go" ]; then echo "ERROR: internal/server/tools_$(NAME).go already exists"; exit 1; fi
	@echo "Scaffolding tool: $(NAME)"
	@mkdir -p internal/$(NAME)
	@CAMEL=$$(echo "$(NAME)" | sed -r 's/(^|_)([a-z])/\U\2/g' 2>/dev/null || echo "$(NAME)" | sed -E 's/(^|_)([a-z])/\U\2/g' 2>/dev/null || echo "$(NAME)"); \
	echo "package $(NAME)" > internal/$(NAME)/engine.go; \
	echo "" >> internal/$(NAME)/engine.go; \
	echo "import \"log/slog\"" >> internal/$(NAME)/engine.go; \
	echo "" >> internal/$(NAME)/engine.go; \
	echo "// Engine implements $(NAME) functionality." >> internal/$(NAME)/engine.go; \
	echo "type Engine struct {" >> internal/$(NAME)/engine.go; \
	echo "	logger *slog.Logger" >> internal/$(NAME)/engine.go; \
	echo "}" >> internal/$(NAME)/engine.go; \
	echo "" >> internal/$(NAME)/engine.go; \
	echo "// NewEngine creates a new $(NAME) engine." >> internal/$(NAME)/engine.go; \
	echo "func NewEngine(logger *slog.Logger) *Engine {" >> internal/$(NAME)/engine.go; \
	echo "	return &Engine{logger: logger}" >> internal/$(NAME)/engine.go; \
	echo "}" >> internal/$(NAME)/engine.go; \
	echo "package server" > internal/server/tools_$(NAME).go; \
	echo "" >> internal/server/tools_$(NAME).go; \
	echo "import (" >> internal/server/tools_$(NAME).go; \
	echo "	\"context\"" >> internal/server/tools_$(NAME).go; \
	echo "	\"time\"" >> internal/server/tools_$(NAME).go; \
	echo "" >> internal/server/tools_$(NAME).go; \
	echo "	\"github.com/mark3labs/mcp-go/mcp\"" >> internal/server/tools_$(NAME).go; \
	echo ")" >> internal/server/tools_$(NAME).go; \
	echo "" >> internal/server/tools_$(NAME).go; \
	echo "func (s *Server) handle$${CAMEL}Example(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {" >> internal/server/tools_$(NAME).go; \
	echo "	// TODO: implement handler" >> internal/server/tools_$(NAME).go; \
	echo "	return mcp.NewToolResultText(\"not implemented\"), nil" >> internal/server/tools_$(NAME).go; \
	echo "}" >> internal/server/tools_$(NAME).go; \
	echo "package server" > internal/server/register_$(NAME).go; \
	echo "" >> internal/server/register_$(NAME).go; \
	echo "import \"github.com/mark3labs/mcp-go/mcp\"" >> internal/server/register_$(NAME).go; \
	echo "" >> internal/server/register_$(NAME).go; \
	echo "func (s *Server) register$${CAMEL}Tools() {" >> internal/server/register_$(NAME).go; \
	echo "	s.mcpServer.AddTool(mcp.NewTool(\"kafka_$(NAME)_example\"," >> internal/server/register_$(NAME).go; \
	echo "		mcp.WithDescription(\"TODO: describe this tool\")," >> internal/server/register_$(NAME).go; \
	echo "	), s.handle$${CAMEL}Example)" >> internal/server/register_$(NAME).go; \
	echo "}" >> internal/server/register_$(NAME).go
	@echo ""
	@echo "Created:"
	@echo "  internal/$(NAME)/engine.go"
	@echo "  internal/server/tools_$(NAME).go"
	@echo "  internal/server/register_$(NAME).go"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Add the engine field to the appropriate group struct in internal/server/server.go"
	@echo "  2. Initialise it in New() in internal/server/server.go"
	@echo "  3. Add s.register<Name>Tools() to registerTools() in internal/server/register_tools.go"

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
