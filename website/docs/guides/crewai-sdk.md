---
sidebar_position: 9
title: Python SDK (CrewAI)
---

# Python SDK — CrewAI Integration

KafkaMCP provides a CrewAI integration at [`sdk/python/kafkamcp_crewai`](https://github.com/josedab/kafkamcp/tree/main/sdk/python/kafkamcp_crewai) for building Kafka-aware AI agent crews.

## Installation

```bash
pip install "git+https://github.com/josedab/kafkamcp.git#subdirectory=sdk/python/kafkamcp_crewai"
```

The package is not yet published to PyPI.

## Quick Start

```python
from kafkamcp_crewai import StdioMCPClient, create_kafka_tools
from crewai import Agent, Task, Crew

with StdioMCPClient(args=["--config", "kafkamcp.yaml"]) as client:
    kafka_tools = create_kafka_tools(mcp_client=client)

    kafka_agent = Agent(
        role="Kafka Analyst",
        goal="Analyze Kafka topics and consumer groups",
        backstory="Expert in Apache Kafka operations and data analysis.",
        tools=kafka_tools,
    )

    task = Task(
        description="List all topics and report consumer group lag for each.",
        expected_output="A topic and consumer lag report.",
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
| `KafkaDescribeGroupTool` | `kafka_describe_consumer_group` | Get consumer group state and lag |
| `KafkaListGroupsTool` | `kafka_list_consumer_groups` | List all consumer groups |
| `KafkaGetSchemaTool` | `kafka_get_schema` | Get schema definition |
| `KafkaAnalyzeDLQTool` | `kafka_analyze_dlq` | Analyze dead-letter queue failures |

## Factory Functions

| Function | Description |
|----------|-------------|
| `create_kafka_tools(mcp_client)` | Create the eight read-only default wrappers |
| `create_guarded_tools(mcp_client)` | Create three approval-gated mutation wrappers |
| `create_monitoring_tools(mcp_client)` | Read-only monitoring subset |
| `create_remediation_tools(mcp_client)` | DLQ remediation subset (consume, remediate, search, schema) |

## Individual Tool Usage

```python
from kafkamcp_crewai import KafkaConsumeTool, KafkaSearchTool, StdioMCPClient

with StdioMCPClient(args=["--config", "kafkamcp.yaml"]) as client:
    consume = KafkaConsumeTool(mcp_client=client)
    search = KafkaSearchTool(mcp_client=client)
    agent = Agent(role="Data Investigator", tools=[consume, search], ...)
```

## See Also

- [LangChain SDK](langchain-sdk.md) — LangChain integration
- [Go SDK](go-sdk.md) — Native Go client
- [TypeScript SDK](typescript-sdk.md) — TypeScript/Node.js client
- [API Reference: Tools](/docs/api-reference/tools) — Full list of MCP tools
