.PHONY: build test test-parallel lint lint-fix lint-all lint-changed fmt fmt-check fmt-imports docker clean clean-all validate validate-ci help run run-local dev dev-sse dev-http coverage test-coverage-map test-coverage-report setup-hooks docker-dev docker-dev-down setup test-one setup-vscode setup-goland integration-test test-integration-setup check doctor sdk-test quickstart benchmark docker-compose-full vet sdk-setup deps changelog test-coverage-check test-short test-race-short test-all new-tool check-deps install docs docs-serve test-func security security-static helm-check ci deps-outdated pre-push profile generate test-pkg test-verbose check-gen check-codeowners readiness readiness-json incident-demo-build incident-demo-up incident-demo-up-redpanda incident-demo-setup incident-demo-verify incident-demo-cleanup incident-demo-down incident-demo-down-redpanda

# Build variables
BINARY    := kafkamcp
CMD       := ./cmd/kafkamcp
PYTHON    ?= python3
BUILD_DIR := bin
VERSION   := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS   := -s -w -X main.version=$(VERSION)

# Tool versions
GOLANGCI_LINT_VERSION := v2.12.2
GOVULNCHECK_VERSION   := v1.6.0
GORELEASER_VERSION    := v2.17.0

# Docker variables
DOCKER_IMAGE := ghcr.io/josedab/kafkamcp
DOCKER_TAG   := latest

# Docker Compose variables
COMPOSE_FILE := examples/docker-compose.yaml

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

build: ## Build the binary
	go build -trimpath -ldflags="$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY) $(CMD)

install: ## Install binary to $GOPATH/bin
	go install -trimpath -ldflags="$(LDFLAGS)" $(CMD)

run: build ## Build and run with default config
	$(BUILD_DIR)/$(BINARY) --config kafkamcp.yaml

run-local: build ## Build and run without Docker (for remote Kafka clusters)
	@echo "Starting KafkaMCP (no Docker required)..."
	@echo "Configure your Kafka connection in kafkamcp.yaml"
	$(BUILD_DIR)/$(BINARY) --config kafkamcp.yaml

dev: ## Run with auto-reload (requires air: go install github.com/air-verse/air@latest)
	@command -v air >/dev/null 2>&1 || { echo "Installing air..."; go install github.com/air-verse/air@latest; }
	@set -a && [ -f .env ] && . ./.env && set +a; air

dev-sse: ## Run with SSE transport and auto-reload
	@command -v air >/dev/null 2>&1 || { echo "Installing air..."; go install github.com/air-verse/air@latest; }
	@set -a && [ -f .env ] && . ./.env && set +a; KAFKAMCP_TRANSPORT=sse KAFKAMCP_UNSAFE_ALLOW_UNAUTHENTICATED_NETWORK=true air

dev-http: ## Run with streamable-http transport and auto-reload
	@command -v air >/dev/null 2>&1 || { echo "Installing air..."; go install github.com/air-verse/air@latest; }
	@set -a && [ -f .env ] && . ./.env && set +a; KAFKAMCP_TRANSPORT=streamable-http KAFKAMCP_UNSAFE_ALLOW_UNAUTHENTICATED_NETWORK=true air

test: ## Run unit tests with race detection
	go test -race -coverprofile=coverage.out ./...

test-parallel: ## Run unit tests in parallel with race detection
	go test -race -parallel 4 -coverprofile=coverage.out ./...

integration-test: ## Run integration tests (requires Kafka cluster)
	go test -race -tags integration -v ./...

test-integration-setup: ## Check Kafka connectivity and start docker-dev if needed
	@echo "=== Integration Test Setup ==="
	@printf "Kafka (localhost:9092): "
	@if (echo > /dev/tcp/localhost/9092) 2>/dev/null; then \
		echo "REACHABLE — ready to run integration tests."; \
	else \
		echo "NOT REACHABLE"; \
		echo ""; \
		if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then \
			echo "Starting local Kafka dependencies..."; \
			docker compose -f $(COMPOSE_FILE) up -d kafka schema-registry; \
			echo "Waiting for Kafka to become reachable..."; \
			for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do \
				if (echo > /dev/tcp/localhost/9092) 2>/dev/null; then \
					echo "Kafka is up after $${i}s."; \
					break; \
				fi; \
				sleep 3; \
			done; \
			if ! (echo > /dev/tcp/localhost/9092) 2>/dev/null; then \
				echo "ERROR: Kafka did not become reachable within 60s."; \
				echo "Check 'docker compose -f $(COMPOSE_FILE) logs' for details."; \
				exit 1; \
			fi; \
		else \
			echo "ERROR: Docker is not available. Cannot auto-start Kafka."; \
			echo ""; \
			echo "Options:"; \
			echo "  1. Install and start Docker, then re-run 'make test-integration-setup'"; \
			echo "  2. Point to an existing Kafka cluster in kafkamcp.yaml"; \
			echo "  3. Run 'make test' for unit tests only"; \
			exit 1; \
		fi; \
	fi
	@echo ""
	@echo "Run 'make integration-test' to execute integration tests."

coverage: test ## Generate and open HTML coverage report
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"
	@command -v open >/dev/null 2>&1 && open coverage.html || command -v xdg-open >/dev/null 2>&1 && xdg-open coverage.html || true

MIN_COVERAGE ?= 70

test-coverage-check: test ## Fail if total repository coverage is below MIN_COVERAGE (default: 70%)
	@total=$$(go tool cover -func=coverage.out | tail -1 | awk '{print $$NF}' | tr -d '%'); \
	echo "Total coverage: $${total}%"; \
	if [ $$(echo "$${total} < $(MIN_COVERAGE)" | bc -l) -eq 1 ]; then \
		echo "FAIL: coverage $${total}% is below minimum $(MIN_COVERAGE)%"; \
		exit 1; \
	fi; \
	echo "OK: coverage $${total}% meets minimum $(MIN_COVERAGE)%"

test-coverage-map: ## Print sorted file-level coverage table (highlights files below 50%)
	@if [ ! -f coverage.out ]; then echo "No coverage.out found. Run 'make test' first."; exit 1; fi
	@echo "=== File Coverage Map ==="
	@go tool cover -func=coverage.out | \
		grep -v '^total:' | \
		awk '{ file=$$1; cov=$$NF } \
		     file != prev { if (prev) printf "%s\t%s\n", last, prev; prev=file } \
		     { last=cov } \
		     END { if (prev) printf "%s\t%s\n", last, prev }' | \
		sort -t'%' -k1 -n | \
		awk -F'\t' '{ \
			pct = $$1 + 0; \
			if (pct < 50.0) \
				printf "  \033[31m%-6s\033[0m %s\n", $$1, $$2; \
			else \
				printf "  %-6s %s\n", $$1, $$2; \
		}'
	@echo ""
	@echo "=== Total ==="
	@go tool cover -func=coverage.out | tail -1 | awk '{printf "  %-6s %s\n", $$NF, $$1}'
	@echo ""
	@echo "(Files below 50% are shown in red)"

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
	@command -v golangci-lint >/dev/null 2>&1 || { echo "Installing golangci-lint..."; go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); }
	golangci-lint run --allow-parallel-runners ./...

lint-fix: ## Run golangci-lint with auto-fix
	@command -v golangci-lint >/dev/null 2>&1 || { echo "Installing golangci-lint..."; go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); }
	golangci-lint run --allow-parallel-runners --fix ./...

lint-changed: ## Run golangci-lint only on files changed since HEAD~1
	@command -v golangci-lint >/dev/null 2>&1 || { echo "Installing golangci-lint..."; go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); }
	golangci-lint run --allow-parallel-runners --new-from-rev=HEAD~1 ./...

lint-all: vet lint ## Run all static analysis (vet + golangci-lint)

fmt: ## Format code
	gofmt -s -w .
	goimports -w . 2>/dev/null || true

fmt-check: ## Check code formatting and import ordering (fails if unformatted)
	@unformatted=$$(gofmt -s -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "ERROR: The following files are not formatted:"; \
		echo "$$unformatted"; \
		echo "Run 'make fmt' to fix."; \
		exit 1; \
	fi
	@if command -v goimports >/dev/null 2>&1; then \
		bad_imports=$$(goimports -l .); \
		if [ -n "$$bad_imports" ]; then \
			echo "ERROR: The following files have incorrect import ordering:"; \
			echo "$$bad_imports"; \
			echo "Run 'make fmt' to fix."; \
			exit 1; \
		fi; \
	fi
	@echo "All files formatted correctly."

fmt-imports: ## Check import ordering only (fails if goimports would change files)
	@command -v goimports >/dev/null 2>&1 || { echo "ERROR: goimports not found. Run 'make setup' to install."; exit 1; }
	@bad_imports=$$(goimports -l .); \
	if [ -n "$$bad_imports" ]; then \
		echo "ERROR: The following files have incorrect import ordering:"; \
		echo "$$bad_imports"; \
		echo "Run 'make fmt' to fix."; \
		exit 1; \
	fi; \
	echo "All imports ordered correctly."

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

setup-hooks: ## Install git pre-commit hook (uses version-controlled .githooks/)
	git config core.hooksPath .githooks
	@echo "Pre-commit hook installed (using .githooks/ directory)."

setup: setup-hooks setup-vscode ## Install all dev dependencies (lint, reload, imports, release checks)
	@echo "Installing dev dependencies..."
	@command -v golangci-lint >/dev/null 2>&1 || { echo "  Installing golangci-lint..."; go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); }
	@command -v air >/dev/null 2>&1 || { echo "  Installing air..."; go install github.com/air-verse/air@latest; }
	@command -v goimports >/dev/null 2>&1 || { echo "  Installing goimports..."; go install golang.org/x/tools/cmd/goimports@latest; }
	@command -v goreleaser >/dev/null 2>&1 || { echo "  Installing GoReleaser..."; mkdir -p "$$HOME/.local/bin"; GOBIN="$$HOME/.local/bin" go install github.com/goreleaser/goreleaser/v2@$(GORELEASER_VERSION); }
	@echo "All dev dependencies installed."

validate: fmt-check vet lint test build ## Run all checks (fmt-check, vet, lint, test, build)

# validate-ci is semantically identical to validate but signals intent:
# use 'validate' during local development, 'validate-ci' in CI pipelines and pre-push.
validate-ci: fmt-check vet lint test build ## Read-only validation (safe for CI and pre-push)

check-deps: ## Check go.mod/go.sum are tidy
	go mod tidy
	@git diff --exit-code go.mod go.sum || { echo "ERROR: go.mod/go.sum are not tidy. Run 'go mod tidy' and commit the changes."; exit 1; }

check: validate ## Alias for validate

test-one: ## Run a single test: make test-one TEST=TestName PKG=./internal/...
	go test -race -v -run $(TEST) $(PKG)

test-func: ## Run a single test by name: make test-func FUNC=TestName
	@if [ -z "$(FUNC)" ]; then echo "Usage: make test-func FUNC=TestName"; exit 1; fi
	@pkg=$$(grep -rl 'func $(FUNC)' --include='*_test.go' . | head -1 | xargs -I{} dirname {} 2>/dev/null); \
	if [ -z "$$pkg" ]; then echo "ERROR: test function '$(FUNC)' not found in any *_test.go file"; exit 1; fi; \
	echo "Found $(FUNC) in $$pkg"; \
	go test -race -v -run $(FUNC) $$pkg

test-pkg: ## Run tests for a single package: make test-pkg PKG=./internal/kafka
	@if [ -z "$(PKG)" ]; then echo "Usage: make test-pkg PKG=./internal/kafka"; exit 1; fi
	go test -race -v $(PKG)

test-verbose: ## Run tests with verbose output for debugging failures
	go test -race -v $(if $(PKG),$(PKG),./...)

setup-vscode: ## Copy .vscode example configs (if not already present)
	@mkdir -p .vscode
	@test -f .vscode/launch.json || (cp .vscode/launch.json.example .vscode/launch.json && echo "Created .vscode/launch.json")
	@test -f .vscode/settings.json || (cp .vscode/settings.json.example .vscode/settings.json && echo "Created .vscode/settings.json")
	@test -f .vscode/tasks.json || (cp .vscode/tasks.json.example .vscode/tasks.json && echo "Created .vscode/tasks.json")
	@echo "VSCode setup complete."

setup-goland: ## Confirm GoLand/IntelliJ run configurations are ready
	@echo "=== GoLand / IntelliJ Setup ==="
	@echo ""
	@if [ -d .run ]; then \
		echo "Run configurations found in .run/:"; \
		ls -1 .run/*.xml 2>/dev/null | sed 's/^/  /'; \
		echo ""; \
		echo "GoLand automatically discovers .run/ configs — no setup needed."; \
		echo "Open the project in GoLand and select a configuration from the Run dropdown."; \
	else \
		echo "WARNING: .run/ directory not found."; \
		echo "Run configurations may not be available."; \
	fi

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
			echo "WARNING: $$installed_ver installed, expected $(GOLANGCI_LINT_VERSION) (run: go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION))"; \
		fi; \
	else \
		echo "NOT FOUND (run: make setup)"; \
	fi
	@printf "air:             "; command -v air >/dev/null 2>&1 && echo "OK" || echo "NOT FOUND (optional, run: make setup)"
	@printf "goimports:       "; command -v goimports >/dev/null 2>&1 && echo "OK" || echo "NOT FOUND (optional, run: make setup)"
	@printf "GoReleaser:      "; \
	if command -v goreleaser >/dev/null 2>&1; then \
		installed_ver=$$(goreleaser --version 2>&1 | grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+' | head -1); \
		if [ "$$installed_ver" = "$(GORELEASER_VERSION)" ]; then \
			echo "OK $$installed_ver"; \
		else \
			echo "WARNING: $$installed_ver installed, expected $(GORELEASER_VERSION) (run: GOBIN=$$HOME/.local/bin go install github.com/goreleaser/goreleaser/v2@$(GORELEASER_VERSION))"; \
		fi; \
	else \
		echo "NOT FOUND (run: make setup; ensure $$HOME/.local/bin is on PATH)"; \
	fi
	@printf "Docker:          "; command -v docker >/dev/null 2>&1 && (docker info >/dev/null 2>&1 && echo "OK (running)" || echo "INSTALLED but not running") || echo "NOT FOUND (needed for integration tests)"
	@printf "Kafka (localhost:9092): "; (echo > /dev/tcp/localhost/9092) 2>/dev/null && echo "REACHABLE" || echo "NOT REACHABLE (run: make docker-dev)"
	@printf "Pre-commit hook: "; git config core.hooksPath 2>/dev/null | grep -q .githooks && echo "INSTALLED (.githooks/)" || echo "NOT INSTALLED (run: make setup)"
	@printf "Python 3.10+:    "; command -v $(PYTHON) >/dev/null 2>&1 && $(PYTHON) -c 'import sys; raise SystemExit(sys.version_info < (3, 10))' && echo "OK ($$($(PYTHON) --version 2>&1))" || echo "NOT FOUND/TOO OLD (needed for SDK tests: make sdk-test)"
	@printf "Node.js 18+:     "; command -v node >/dev/null 2>&1 && echo "OK ($$(node --version 2>&1))" || echo "NOT FOUND (needed for SDK setup: make sdk-setup)"
	@echo ""
	@echo "=== KafkaMCP module deps (required for new subsystems) ==="
	@printf "SQLite (persistence backend):     "; go list -m modernc.org/sqlite >/dev/null 2>&1 && echo "OK ($$(go list -m modernc.org/sqlite | awk '{print $$2}'))" || echo "NOT FOUND (needed for server.persistence.backend: sqlite)"
	@printf "go-oidc (OIDC identity):          "; go list -m github.com/coreos/go-oidc/v3 >/dev/null 2>&1 && echo "OK ($$(go list -m github.com/coreos/go-oidc/v3 | awk '{print $$2}'))" || echo "NOT FOUND (needed for policy.identity.provider: oidc)"
	@printf "hamba/avro (Avro codec):          "; go list -m github.com/hamba/avro/v2 >/dev/null 2>&1 && echo "OK ($$(go list -m github.com/hamba/avro/v2 | awk '{print $$2}'))" || echo "NOT FOUND (needed for Avro wire-format codec)"
	@printf "protocompile (Protobuf codec):    "; go list -m github.com/bufbuild/protocompile >/dev/null 2>&1 && echo "OK ($$(go list -m github.com/bufbuild/protocompile | awk '{print $$2}'))" || echo "NOT FOUND (needed for Protobuf wire-format codec)"
	@echo ""
	@echo "Run 'make setup' to install missing dev dependencies."

quickstart: ## One-command setup: install deps, start Kafka, build, and run
	@command -v docker >/dev/null 2>&1 || { echo "ERROR: Docker is not installed."; echo "Install Docker from https://docs.docker.com/get-docker/ and try again."; echo "Alternatively, use 'make run-local' if you have a remote Kafka cluster."; exit 1; }
	@docker info >/dev/null 2>&1 || { echo "ERROR: Docker is not running."; echo "Start Docker Desktop (or the Docker daemon) and try again."; echo "Alternatively, use 'make run-local' if you have a remote Kafka cluster."; exit 1; }
	@$(MAKE) setup docker-dev build run

benchmark: ## Run Go benchmarks with memory stats
	go test -bench=. -benchmem -run=^$$ ./...

.PHONY: tool-eval
tool-eval: ## Run the deterministic AI tool-routing evaluation harness
	go test -v ./internal/tooleval/...

.PHONY: readiness
readiness: ## Run the objective v1 readiness reporter (human output; exits nonzero if mandatory gates fail)
	go run ./cmd/readiness/ -dir .

.PHONY: readiness-json
readiness-json: ## Run the objective v1 readiness reporter with JSON output
	go run ./cmd/readiness/ -dir . -json

# ---------------------------------------------------------------------------
# Incident-Demo targets
# ---------------------------------------------------------------------------
INCIDENT_DEMO_COMPOSE := examples/incident-demo/docker-compose.yaml
INCIDENT_DEMO_BROKERS ?= localhost:9092
INCIDENT_DEMO_PREFIX  ?= incident-demo.
INCIDENT_DEMO_BINARY  := $(BUILD_DIR)/incident-demo

$(INCIDENT_DEMO_BINARY): cmd/incident-demo/*.go
	go build -o $(INCIDENT_DEMO_BINARY) ./cmd/incident-demo/

.PHONY: incident-demo-build
incident-demo-build: ## Build the incident-demo binary
	go build -o $(INCIDENT_DEMO_BINARY) ./cmd/incident-demo/

.PHONY: incident-demo-up
incident-demo-up: ## Start the incident-demo Kafka environment (Apache Kafka + Schema Registry)
	docker compose -f $(INCIDENT_DEMO_COMPOSE) --profile kafka up -d
	@echo "Waiting for Kafka to become ready..."
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
		if (echo > /dev/tcp/localhost/9092) 2>/dev/null; then \
			echo "Kafka is ready (attempt $$i)."; break; \
		fi; \
		echo "Waiting... ($$i/10)"; sleep 5; \
	done; \
	if ! (echo > /dev/tcp/localhost/9092) 2>/dev/null; then echo "ERROR: Kafka did not become ready."; exit 1; fi

.PHONY: incident-demo-up-redpanda
incident-demo-up-redpanda: ## Start the incident-demo environment with Redpanda (compatibility profile)
	docker compose -f $(INCIDENT_DEMO_COMPOSE) --profile redpanda up -d
	@echo "Waiting for Redpanda to become ready..."
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
		if (echo > /dev/tcp/localhost/9092) 2>/dev/null; then \
			echo "Redpanda is ready (attempt $$i)."; break; \
		fi; \
		echo "Waiting... ($$i/10)"; sleep 5; \
	done; \
	if ! (echo > /dev/tcp/localhost/9092) 2>/dev/null; then echo "ERROR: Redpanda did not become ready."; exit 1; fi

.PHONY: incident-demo-setup
incident-demo-setup: incident-demo-build ## Create demo topics and populate with scenario data
	$(INCIDENT_DEMO_BINARY) -brokers $(INCIDENT_DEMO_BROKERS) -prefix $(INCIDENT_DEMO_PREFIX) setup

.PHONY: incident-demo-verify
incident-demo-verify: incident-demo-build ## Verify demo topics and message counts
	$(INCIDENT_DEMO_BINARY) -brokers $(INCIDENT_DEMO_BROKERS) -prefix $(INCIDENT_DEMO_PREFIX) verify

.PHONY: incident-demo-cleanup
incident-demo-cleanup: incident-demo-build ## Delete demo topics (safe: only deletes incident-demo.* prefix)
	$(INCIDENT_DEMO_BINARY) -brokers $(INCIDENT_DEMO_BROKERS) -prefix $(INCIDENT_DEMO_PREFIX) cleanup

.PHONY: incident-demo-down
incident-demo-down: ## Stop the incident-demo Docker environment
	docker compose -f $(INCIDENT_DEMO_COMPOSE) --profile kafka down -v

.PHONY: incident-demo-down-redpanda
incident-demo-down-redpanda: ## Stop the Redpanda incident-demo environment
	docker compose -f $(INCIDENT_DEMO_COMPOSE) --profile redpanda down -v

PROFILE_PKG ?= ./internal/kafka

profile: ## Run benchmarks with CPU and memory profiling: make profile PKG=./internal/kafka
	@mkdir -p $(BUILD_DIR)/profiles
	go test -bench=. -benchmem -run=^$$ -cpuprofile=$(BUILD_DIR)/profiles/cpu.prof -memprofile=$(BUILD_DIR)/profiles/mem.prof $(PROFILE_PKG)
	@echo ""
	@echo "Profiles saved to $(BUILD_DIR)/profiles/"
	@echo "  CPU:    go tool pprof $(BUILD_DIR)/profiles/cpu.prof"
	@echo "  Memory: go tool pprof $(BUILD_DIR)/profiles/mem.prof"
	@echo ""
	@echo "Opening CPU profile..."
	go tool pprof -http=:6060 $(BUILD_DIR)/profiles/cpu.prof || go tool pprof $(BUILD_DIR)/profiles/cpu.prof

docker-compose-full: ## Start full Kafka + KafkaMCP stack
	docker compose -f examples/docker-compose-kafkamcp.yaml up -d

vet: ## Run go vet for static analysis
	go vet ./...

deps: ## Check for outdated and vulnerable Go dependencies
	@echo "=== Outdated Dependencies ==="
	@go list -m -u all 2>/dev/null | grep '\[' || echo "All dependencies up to date."
	@echo ""
	@echo "=== Vulnerability Check ==="
	go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

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

generate: ## Run all code generation (tool docs, go generate)
	@echo "=== Code Generation ==="
	@echo "--- Tool reference docs ---"
	go run ./cmd/gen-tool-docs/
	@echo "--- go generate ---"
	go generate ./...
	@echo "All code generation complete."

check-gen: ## Verify generated code is up to date (fails if stale)
	@echo "=== Checking generated code ==="
	$(MAKE) generate
	@if ! git diff --exit-code docs/tool-reference.md >/dev/null 2>&1; then \
		echo "ERROR: docs/tool-reference.md is out of date."; \
		echo "Run 'make generate' and commit the updated file."; \
		exit 1; \
	fi
	@echo "Generated code is up to date."

sdk-setup: ## Install SDK test dependencies (Python, Node.js)
	@echo "=== SDK Dependency Setup ==="
	@echo "Prerequisites: Python 3.10+, pip, Node.js 18+, npm"
	@echo "--- Python SDK (langchain) ---"
	cd sdk/python/kafkamcp_langchain && $(PYTHON) -m pip install -q -e ".[test]" 2>/dev/null || $(PYTHON) -m pip install -q -e . 2>/dev/null || echo "FAILED: pip install for kafkamcp_langchain"
	@echo "--- Python SDK (crewai) ---"
	cd sdk/python/kafkamcp_crewai && $(PYTHON) -m pip install -q --no-deps -e . 2>/dev/null || echo "FAILED: editable install for kafkamcp_crewai"
	@echo "--- TypeScript SDK ---"
	cd sdk/typescript && npm install --silent
	@echo "SDK dependencies installed."

sdk-test: ## Run tests for all SDKs (Go, Python, TypeScript)
	@echo "=== SDK Tests ==="
	@echo "--- Go SDK ---"
	go test ./sdk/go/...
	@echo "--- Python SDK (langchain) ---"
	cd sdk/python/kafkamcp_langchain && $(PYTHON) -m pip install -q -e . && $(PYTHON) -m pytest -q
	@echo "--- Python SDK (crewai) ---"
	cd sdk/python/kafkamcp_crewai && PYTHONPATH=. $(PYTHON) -m pytest -q
	@echo "--- TypeScript SDK ---"
	cd sdk/typescript && npm install --silent && npm test && npm run lint
	@echo "All SDK tests complete."

docs: ## Build the Docusaurus documentation site
	cd website && npm run build

docs-serve: ## Serve the documentation site locally for preview
	cd website && npm run start

helm-check: ## Lint and smoke-test production Helm rendering and safety guards
	./scripts/check-helm.sh

security-static: ## Run the pinned gosec production gate (HIGH/HIGH findings block)
	go run ./cmd/gosec-gate $(if $(GOSEC_REPORT),-out $(GOSEC_REPORT),)

security: security-static ## Run static analysis and govulncheck
	go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

ci: validate-ci ## Alias for validate-ci

deps-outdated: ## Show Go modules with available updates
	@go list -m -u all 2>/dev/null | grep '\[' || echo "All dependencies are up to date."

pre-push: validate-ci ## Run all checks before pushing (alias for validate-ci)

check-codeowners: ## Warn about internal/ packages missing from CODEOWNERS
	@echo "=== CODEOWNERS Coverage Check ==="
	@missing=0; \
	for dir in internal/*/; do \
		pkg=$$(basename "$$dir"); \
		if ! grep -q "/internal/$$pkg/" .github/CODEOWNERS 2>/dev/null; then \
			echo "WARNING: /internal/$$pkg/ is not listed in .github/CODEOWNERS"; \
			missing=$$((missing + 1)); \
		fi; \
	done; \
	if [ $$missing -gt 0 ]; then \
		echo ""; \
		echo "$$missing package(s) missing from CODEOWNERS."; \
		echo "Add entries to .github/CODEOWNERS for the packages above."; \
		exit 1; \
	fi; \
	echo "All internal/ packages are covered by CODEOWNERS."
