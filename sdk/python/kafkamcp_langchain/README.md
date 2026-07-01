# kafkamcp-langchain

LangChain integration for [KafkaMCP](https://github.com/josedab/kafkamcp) — use Kafka as AI agent tools.

## Installation

```bash
pip install "git+https://github.com/josedab/kafkamcp.git#subdirectory=sdk/python/kafkamcp_langchain"
```

The package is source-distributed from this repository and is not yet
published to PyPI.

## Quick Start

```python
from kafkamcp_langchain import KafkaMCPToolkit
from langchain.agents import initialize_agent, AgentType
from langchain_openai import ChatOpenAI

# Connect to KafkaMCP server
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

        # MCP's ClientSession is asynchronous, so use async agent invocation.
        llm = ChatOpenAI(model="gpt-4")
        agent = initialize_agent(tools, llm, agent=AgentType.OPENAI_FUNCTIONS)
        result = await agent.ainvoke({
            "input": "List all topics and show me the latest 5 messages from the orders topic"
        })
```

## Available Toolkits

- `KafkaMCPToolkit(client)` — Safe read-only wrapper set (8 tools)
- `KafkaConsumeToolkit(client)` — Consume + Search
- `KafkaProduceToolkit(client)` — Produce
- `KafkaTopicToolkit(client)` — Topic management
- `KafkaSchemaToolkit(client)` — Schema Registry

Pass an asynchronous MCP `ClientSession` to LangChain's `ainvoke`/`arun`
paths. Synchronous invocation is supported only when the supplied client has a
synchronous `call_tool` method.

## License

Apache-2.0
