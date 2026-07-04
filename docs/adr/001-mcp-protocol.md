# ADR-001: Use Model Context Protocol (MCP)

## Status

Accepted

## Context

KafkaMCP needs a standardized protocol to expose Kafka operations to AI agents. Several approaches were considered:

1. **Custom REST API** — Build a bespoke REST API that AI agents call directly.
2. **GraphQL** — Expose Kafka operations via a GraphQL schema.
3. **Model Context Protocol (MCP)** — Use Anthropic's open MCP standard for tool/resource exposure.
4. **OpenAI Function Calling format** — Align with OpenAI's tool schema directly.

The core requirement is that any AI agent (Claude, GPT, open-source LLMs) should be able to discover and use Kafka operations without custom integration code per agent.

## Decision

We adopt the **Model Context Protocol (MCP)** as the primary interface between AI agents and Kafka.

Key reasons:

- **Agent-agnostic** — MCP is an open standard supported by multiple AI platforms (Claude Desktop, Cursor, VS Code, custom agents).
- **Built-in discovery** — MCP's tool and resource primitives let agents discover available Kafka operations at runtime without hardcoded knowledge.
- **Dual transport** — MCP supports both stdio (local, single-agent) and SSE/HTTP (remote, multi-agent), covering all deployment scenarios.
- **Structured tool definitions** — MCP tools include typed parameters with descriptions, enabling LLMs to generate correct tool calls without documentation lookup.
- **Resource URIs** — MCP resources provide a natural mapping for Kafka entities (`kafka://topics/{name}`, `kafka://groups/{id}`).

We use the [mcp-go](https://github.com/mark3labs/mcp-go) library as the Go MCP server implementation.

## Consequences

### Positive

- Any MCP-compatible agent can use KafkaMCP with zero custom code.
- Tool definitions serve as both API schema and documentation.
- stdio transport enables zero-config local development (e.g., Claude Desktop).
- SSE/HTTP transport enables multi-agent and remote deployments.

### Negative

- MCP is a relatively new protocol — ecosystem maturity is still growing.
- Agents that don't support MCP natively require an MCP client wrapper (addressed by our SDKs).
- MCP's request-response model favors bounded tool calls; KafkaMCP therefore
  exposes bounded consume and search operations rather than long-lived streams.

### Neutral

- We maintain SDK wrappers (Go, Python/LangChain, Python/CrewAI) for agents that prefer native SDK integration over raw MCP.
