"""CrewAI tool wrappers for KafkaMCP operations."""

from typing import Any

from crewai.tools import BaseTool
from pydantic import Field


class KafkaConsumeTool(BaseTool):
    """Consume messages from a Kafka topic."""
    name: str = "kafka_consume"
    description: str = "Read messages from a Kafka topic. Provide topic name, optional limit, and offset."
    mcp_client: Any = None

    def _run(self, topic: str, limit: int = 10, offset: str = "latest") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        return self.mcp_client.call_tool("kafka_consume", {
            "topic": topic, "limit": limit, "offset": offset
        })


class KafkaProduceTool(BaseTool):
    """Produce messages to a Kafka topic."""
    name: str = "kafka_produce"
    description: str = "Publish messages to a Kafka topic. Provide topic name and messages array."
    mcp_client: Any = None

    def _run(self, topic: str, messages: list[dict[str, Any]]) -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        return self.mcp_client.call_tool("kafka_produce", {
            "topic": topic, "messages": messages
        })


class KafkaListTopicsTool(BaseTool):
    """List all Kafka topics."""
    name: str = "kafka_list_topics"
    description: str = "List all Kafka topics with metadata."
    mcp_client: Any = None

    def _run(self, filter: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        return self.mcp_client.call_tool("kafka_list_topics", {"filter": filter})


class KafkaSearchTool(BaseTool):
    """Search messages in a Kafka topic."""
    name: str = "kafka_search"
    description: str = "Search messages in a Kafka topic by content, key, or headers."
    mcp_client: Any = None

    def _run(self, topic: str, value_contains: str = "", limit: int = 10) -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        return self.mcp_client.call_tool("kafka_search", {
            "topic": topic, "value_contains": value_contains, "limit": limit
        })


class KafkaWatchTool(BaseTool):
    """Watch Kafka topics for real-time events."""
    name: str = "kafka_watch"
    description: str = "Subscribe to topic patterns for event-driven notifications."
    mcp_client: Any = None

    def _run(self, topic_pattern: str, key_filter: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        return self.mcp_client.call_tool("kafka_watch", {
            "action": "subscribe", "topic_pattern": topic_pattern, "key_filter": key_filter
        })


def create_kafka_tools(mcp_client: Any) -> list[BaseTool]:
    """Create all KafkaMCP tools for use with CrewAI agents."""
    return [
        KafkaConsumeTool(mcp_client=mcp_client),
        KafkaProduceTool(mcp_client=mcp_client),
        KafkaListTopicsTool(mcp_client=mcp_client),
        KafkaSearchTool(mcp_client=mcp_client),
        KafkaWatchTool(mcp_client=mcp_client),
    ]
