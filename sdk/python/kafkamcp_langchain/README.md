# kafkamcp-langchain

LangChain integration for [KafkaMCP](https://github.com/kafkamcp/kafkamcp) — use Kafka as AI agent tools.

## Installation

```bash
pip install kafkamcp-langchain
```

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

        # Use with any LangChain agent
        llm = ChatOpenAI(model="gpt-4")
        agent = initialize_agent(tools, llm, agent=AgentType.OPENAI_FUNCTIONS)
        result = agent.run("List all topics and show me the latest 5 messages from the orders topic")
```

## Available Toolkits

- `KafkaMCPToolkit(client)` — All tools
- `KafkaConsumeToolkit(client)` — Consume + Search
- `KafkaProduceToolkit(client)` — Produce
- `KafkaTopicToolkit(client)` — Topic management
- `KafkaSchemaToolkit(client)` — Schema Registry

## License

Apache-2.0
