---
sidebar_position: 8
title: Python SDK (LangChain)
---

# Python SDK — LangChain Integration

KafkaMCP provides a LangChain toolkit at [`sdk/python/kafkamcp_langchain`](https://github.com/kafkamcp/kafkamcp/tree/main/sdk/python/kafkamcp_langchain) that exposes all KafkaMCP tools as LangChain-compatible tools for use with any LangChain agent.

## Installation

```bash
pip install kafkamcp-langchain
```

## Quick Start

```python
from kafkamcp_langchain import KafkaMCPToolkit
from langchain.agents import initialize_agent, AgentType
from langchain_openai import ChatOpenAI

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

server_params = StdioServerParameters(
    command="kafkamcp",
    args=["--config", "kafkamcp.yaml"]
)

async with stdio_client(server_params) as (read, write):
    async with ClientSession(read, write) as session:
        await session.initialize()

        # Create LangChain tools from KafkaMCP
        tools = KafkaMCPToolkit(mcp_client=session)

        # Use with any LangChain agent
        llm = ChatOpenAI(model="gpt-4")
        agent = initialize_agent(tools, llm, agent=AgentType.OPENAI_FUNCTIONS)
        result = agent.run(
            "List all topics and show me the latest 5 messages from the orders topic"
        )
```

## Available Toolkits

| Toolkit | Description |
|---------|-------------|
| `KafkaMCPToolkit(client)` | All KafkaMCP tools |
| `KafkaConsumeToolkit(client)` | Consume and search tools |
| `KafkaProduceToolkit(client)` | Produce tools |
| `KafkaTopicToolkit(client)` | Topic management tools |
| `KafkaSchemaToolkit(client)` | Schema Registry tools |

Use `KafkaMCPToolkit` for full access, or select a subset toolkit to limit the agent's capabilities.

## Configuration

The LangChain toolkit connects to a running KafkaMCP server via MCP. Configure your `kafkamcp.yaml` as described in the [Configuration](/docs/configuration) guide, then pass the MCP session to the toolkit constructor.

For access control, set up [policies](/docs/guides/access-control) in your config to restrict what the agent can do.

## See Also

- [CrewAI SDK](crewai-sdk.md) — CrewAI integration
- [Go SDK](go-sdk.md) — Native Go client
- [TypeScript SDK](typescript-sdk.md) — TypeScript/Node.js client
- [API Reference: Tools](/docs/api-reference/tools) — Full list of MCP tools
