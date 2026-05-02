---
sidebar_position: 10
title: Python SDK Overview
---

# Python SDK Overview

KafkaMCP provides Python SDKs for two popular AI agent frameworks: **LangChain** and **CrewAI**. Both SDKs connect to a running KafkaMCP server via the MCP protocol and expose Kafka operations as framework-native tools.

## Choosing an SDK

| SDK | Framework | Best For |
|-----|-----------|----------|
| [`kafkamcp-langchain`](langchain-sdk.md) | LangChain | Single-agent workflows, chains, and OpenAI function-calling agents |
| [`kafkamcp-crewai`](crewai-sdk.md) | CrewAI | Multi-agent crews with role-based collaboration |

Both SDKs share the same MCP transport layer and connect to the same KafkaMCP server.

## Quick Install

```bash
# LangChain integration
pip install kafkamcp-langchain

# CrewAI integration
pip install kafkamcp-crewai
```

## Prerequisites

1. A running KafkaMCP server (see [Getting Started](/docs/getting-started))
2. Python 3.10+
3. The `mcp` Python package for MCP client connectivity

## Guides

- [LangChain SDK Guide](langchain-sdk.md) — Full setup and usage with LangChain agents
- [CrewAI SDK Guide](crewai-sdk.md) — Full setup and usage with CrewAI crews

## See Also

- [Go SDK](go-sdk.md) — Native Go client
- [TypeScript SDK](typescript-sdk.md) — TypeScript/Node.js client
- [API Reference: Tools](/docs/api-reference/tools) — Full list of MCP tools
