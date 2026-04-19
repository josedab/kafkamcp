# kafkamcp-crewai

CrewAI integration for [KafkaMCP](https://github.com/kafkamcp/kafkamcp) — build Kafka-aware AI agent crews.

## Installation

```bash
pip install kafkamcp-crewai
```

## Quick Start

```python
from kafkamcp_crewai import create_kafka_tools
from crewai import Agent, Task, Crew

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

server_params = StdioServerParameters(
    command="kafkamcp",
    args=["--config", "kafkamcp.yaml"]
)

async with stdio_client(server_params) as (read, write):
    async with ClientSession(read, write) as session:
        await session.initialize()

        # Create CrewAI tools from KafkaMCP
        kafka_tools = create_kafka_tools(mcp_client=session)

        # Build an agent with Kafka capabilities
        kafka_agent = Agent(
            role="Kafka Analyst",
            goal="Analyze Kafka topics and consumer groups",
            backstory="Expert in Apache Kafka operations and data analysis.",
            tools=kafka_tools,
        )

        # Define a task
        task = Task(
            description="List all topics and report consumer group lag for each.",
            agent=kafka_agent,
        )

        # Run the crew
        crew = Crew(agents=[kafka_agent], tasks=[task])
        result = crew.kickoff()
        print(result)
```

## Available Tools

| Tool Class | MCP Tool | Description |
|------------|----------|-------------|
| `KafkaConsumeTool` | `kafka_consume` | Read messages from a topic |
| `KafkaProduceTool` | `kafka_produce` | Publish messages to a topic |
| `KafkaListTopicsTool` | `kafka_list_topics` | List all topics with metadata |
| `KafkaSearchTool` | `kafka_search` | Search messages by content |
| `KafkaWatchTool` | `kafka_watch` | Subscribe to topic patterns for notifications |

Use `create_kafka_tools(mcp_client)` to get all tools at once, or instantiate individual tool classes for selective use.

## Individual Tool Usage

```python
from kafkamcp_crewai import KafkaConsumeTool, KafkaSearchTool

consume = KafkaConsumeTool(mcp_client=session)
search = KafkaSearchTool(mcp_client=session)

agent = Agent(
    role="Data Investigator",
    tools=[consume, search],
    ...
)
```

## Links

- [Main Project](https://github.com/kafkamcp/kafkamcp)
- [LangChain SDK](../kafkamcp_langchain/)
- [Documentation](https://kafkamcp.dev)

## License

Apache-2.0
