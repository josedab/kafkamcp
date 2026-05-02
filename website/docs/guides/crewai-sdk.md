---
sidebar_position: 9
title: Python SDK (CrewAI)
---

# Python SDK — CrewAI Integration

KafkaMCP provides a CrewAI integration at [`sdk/python/kafkamcp_crewai`](https://github.com/kafkamcp/kafkamcp/tree/main/sdk/python/kafkamcp_crewai) for building Kafka-aware AI agent crews.

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

        task = Task(
            description="List all topics and report consumer group lag for each.",
            agent=kafka_agent,
        )

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
| `KafkaDescribeTopicTool` | `kafka_describe_topic` | Get detailed topic metadata |
| `KafkaCreateTopicTool` | `kafka_create_topic` | Create a new topic |
| `KafkaDeleteTopicTool` | `kafka_delete_topic` | Delete a Kafka topic |
| `KafkaSearchTool` | `kafka_search` | Search messages by content |
| `KafkaWatchTool` | `kafka_watch` | Subscribe to topic patterns |
| `KafkaDescribeGroupTool` | `kafka_describe_consumer_group` | Get consumer group state and lag |
| `KafkaListGroupsTool` | `kafka_list_consumer_groups` | List all consumer groups |
| `KafkaGetSchemaTool` | `kafka_get_schema` | Get schema definition |
| `KafkaDLQRemediateTool` | `kafka_remediate_dlq` | Analyze and remediate DLQ messages |
| `KafkaAggregateTool` | `kafka_aggregate` | Aggregate messages (count, sum, avg, min, max) |

## Factory Functions

| Function | Description |
|----------|-------------|
| `create_kafka_tools(mcp_client)` | Create all 13 tools |
| `create_monitoring_tools(mcp_client)` | Monitoring subset (list, describe, groups, search, watch) |
| `create_remediation_tools(mcp_client)` | DLQ remediation subset (consume, remediate, search, schema) |

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

## See Also

- [LangChain SDK](langchain-sdk.md) — LangChain integration
- [Go SDK](go-sdk.md) — Native Go client
- [TypeScript SDK](typescript-sdk.md) — TypeScript/Node.js client
- [API Reference: Tools](/docs/api-reference/tools) — Full list of MCP tools
