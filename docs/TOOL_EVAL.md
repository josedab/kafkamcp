# AI Tool-Routing Evaluation Harness

`internal/tooleval` is a deterministic, offline evaluation harness that checks
KafkaMCP's tool surface for routing correctness and safety. It uses **no paid
external LLM APIs** — routing is a deterministic keyword matcher over a bounded
candidate catalog, and the fixtures encode the expected selection metadata.

## What it validates

- **Routing:** each of 30+ representative Kafka tasks routes to the expected
  tool (`TestRoutingSelectsExpectedTool`).
- **Uniqueness / ambiguity:** the catalog has no duplicate tool names and every
  referenced tool exists (`TestExpectedToolsExistAndUnique`).
- **Unsafe-tool exclusion:** every write task routes to a tool that is a real
  write tool per the server's authoritative `writeToolSet` **and** is not
  exposed by the default `core-read` pack (`TestWriteTasksNeverRouteIntoCoreRead`).
- **Read stays read:** read-only tasks never route to a write tool
  (`TestReadTasksNeverRouteToWriteTools`).
- **Adversarial prompts:** benign-sounding prompts that require mutation still
  resolve to approval-gated write tools
  (`TestAdversarialPromptsStillRequireApproval`).
- **Coverage:** every `core-read` tool in the catalog is exercised by a read
  task (`TestReadTasksCoverCoreReadCatalog`).

The invariants are cross-checked against the **real** server pack definitions
(`server.NewToolPackFilter`, `server.IsWriteTool`), so a regression in the pack
model — e.g. accidentally adding a write tool to `core-read` — fails this suite.

## Running it

```bash
make tool-eval
# or
go test ./internal/tooleval/...
```

It runs as part of `make test` and therefore in CI on every change.

## Adding a task

Append to `Tasks()` in `internal/tooleval/fixtures.go`. If the task needs a tool
not yet in the router catalog, add it (with distinctive keywords and the correct
`write` flag) to `catalog` in `internal/tooleval/harness.go`. Keep prompts using
the catalog keywords in their exact word form; the router matches whole tokens.
