# kafkamcp-crewai

CrewAI integration for [KafkaMCP](https://github.com/josedab/kafkamcp) — build Kafka-aware AI agent crews.

## Installation

```bash
pip install "git+https://github.com/josedab/kafkamcp.git#subdirectory=sdk/python/kafkamcp_crewai"
```

The package is source-distributed from this repository and is not yet
published to PyPI.

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
| `KafkaDescribeTopicTool` | `kafka_describe_topic` | Get detailed topic metadata including partitions and config |
| `KafkaCreateTopicTool` | `kafka_create_topic` | Create a new topic with partitions and replication factor |
| `KafkaDeleteTopicTool` | `kafka_delete_topic` | Delete a Kafka topic (destructive) |
| `KafkaSearchTool` | `kafka_search` | Search messages by content |
| `KafkaDescribeGroupTool` | `kafka_describe_consumer_group` | Get consumer group state, members, and lag |
| `KafkaListGroupsTool` | `kafka_list_consumer_groups` | List all consumer groups with state and lag |
| `KafkaGetSchemaTool` | `kafka_get_schema` | Get schema definition for a Schema Registry subject |
| `KafkaAnalyzeDLQTool` | `kafka_analyze_dlq` | Analyze DLQ messages and failure categories |

### Factory Functions

| Function | Description |
|----------|-------------|
| `create_kafka_tools(mcp_client)` | Create the eight read-only default wrappers |
| `create_guarded_tools(mcp_client)` | Create three approval-gated mutation wrappers |
| `create_monitoring_tools(mcp_client)` | Read-only monitoring subset |
| `create_remediation_tools(mcp_client)` | DLQ analysis/search/schema subset |

Use `create_kafka_tools(mcp_client)` for the safe default. Add guarded wrappers
only when the agent is expected to supply short-lived approval tokens.

## Individual Tool Usage

```python
from kafkamcp_crewai import KafkaConsumeTool, KafkaSearchTool, StdioMCPClient

with StdioMCPClient(args=["--config", "kafkamcp.yaml"]) as client:
    consume = KafkaConsumeTool(mcp_client=client)
    search = KafkaSearchTool(mcp_client=client)
    agent = Agent(role="Data Investigator", tools=[consume, search], ...)
```

## Links

- [Main Project](https://github.com/josedab/kafkamcp)
- [LangChain SDK](../kafkamcp_langchain/)
- [Documentation](https://josedab.github.io/kafkamcp/)

## License

Apache-2.0
