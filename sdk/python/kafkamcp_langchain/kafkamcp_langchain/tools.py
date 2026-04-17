"""Pre-built LangChain tool wrappers for KafkaMCP operations."""

from typing import Any, Optional, Type

from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field


class ConsumeInput(BaseModel):
    """Input for consuming Kafka messages."""
    topic: str = Field(description="Kafka topic name")
    limit: int = Field(default=10, description="Max messages to consume (1-1000)")
    offset: str = Field(default="latest", description="Start position: earliest, latest, or offset number")
    timeout_ms: int = Field(default=5000, description="Timeout in milliseconds")


class ProduceInput(BaseModel):
    """Input for producing Kafka messages."""
    topic: str = Field(description="Kafka topic name")
    messages: list[dict[str, Any]] = Field(description="Messages to produce: [{key, value, headers}]")


class ListTopicsInput(BaseModel):
    """Input for listing Kafka topics."""
    filter: str = Field(default="", description="Glob pattern to filter topic names")
    include_internal: bool = Field(default=False, description="Include internal topics")


class SearchInput(BaseModel):
    """Input for searching Kafka messages."""
    topic: str = Field(description="Kafka topic name")
    value_contains: str = Field(default="", description="Filter messages containing this string")
    key_filter: str = Field(default="", description="Filter by message key")
    limit: int = Field(default=10, description="Max matching messages to return")


class KafkaConsumeTool(BaseTool):
    """Tool for consuming messages from a Kafka topic via KafkaMCP."""
    name: str = "kafka_consume"
    description: str = "Read messages from a Kafka topic. Returns message key, value, headers, and metadata."
    args_schema: Type[BaseModel] = ConsumeInput
    mcp_client: Any = None

    def _run(self, topic: str, limit: int = 10, offset: str = "latest", timeout_ms: int = 5000) -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        return self.mcp_client.call_tool("kafka_consume", {
            "topic": topic, "limit": limit, "offset": offset, "timeout_ms": timeout_ms
        })


class KafkaProduceTool(BaseTool):
    """Tool for producing messages to a Kafka topic via KafkaMCP."""
    name: str = "kafka_produce"
    description: str = "Publish messages to a Kafka topic. Returns delivery confirmation."
    args_schema: Type[BaseModel] = ProduceInput
    mcp_client: Any = None

    def _run(self, topic: str, messages: list[dict[str, Any]]) -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        return self.mcp_client.call_tool("kafka_produce", {
            "topic": topic, "messages": messages
        })


class KafkaListTopicsTool(BaseTool):
    """Tool for listing Kafka topics via KafkaMCP."""
    name: str = "kafka_list_topics"
    description: str = "List all Kafka topics with metadata including partition count and message count."
    args_schema: Type[BaseModel] = ListTopicsInput
    mcp_client: Any = None

    def _run(self, filter: str = "", include_internal: bool = False) -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        return self.mcp_client.call_tool("kafka_list_topics", {
            "filter": filter, "include_internal": include_internal
        })


class KafkaSearchTool(BaseTool):
    """Tool for searching messages in a Kafka topic via KafkaMCP."""
    name: str = "kafka_search"
    description: str = "Search messages in a Kafka topic by key, value content, or headers."
    args_schema: Type[BaseModel] = SearchInput
    mcp_client: Any = None

    def _run(self, topic: str, value_contains: str = "", key_filter: str = "", limit: int = 10) -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        return self.mcp_client.call_tool("kafka_search", {
            "topic": topic, "value_contains": value_contains, "key_filter": key_filter, "limit": limit
        })


class KafkaWatchTool(BaseTool):
    """Tool for registering event-driven watches on Kafka topics."""
    name: str = "kafka_watch"
    description: str = "Subscribe to topic patterns for real-time notifications when matching messages arrive."
    mcp_client: Any = None

    def _run(self, topic_pattern: str, key_filter: str = "", webhook_url: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        return self.mcp_client.call_tool("kafka_watch", {
            "action": "subscribe", "topic_pattern": topic_pattern,
            "key_filter": key_filter, "webhook_url": webhook_url
        })


def KafkaConsumeToolkit(mcp_client: Any) -> list[BaseTool]:
    """Create consume-focused tools."""
    return [
        KafkaConsumeTool(mcp_client=mcp_client),
        KafkaSearchTool(mcp_client=mcp_client),
    ]


def KafkaProduceToolkit(mcp_client: Any) -> list[BaseTool]:
    """Create produce-focused tools."""
    return [KafkaProduceTool(mcp_client=mcp_client)]


def KafkaTopicToolkit(mcp_client: Any) -> list[BaseTool]:
    """Create topic management tools."""
    return [KafkaListTopicsTool(mcp_client=mcp_client)]


def KafkaSchemaToolkit(mcp_client: Any) -> list[BaseTool]:
    """Create schema registry tools (placeholder for extension)."""
    return []


def KafkaMCPToolkit(mcp_client: Any) -> list[BaseTool]:
    """Create the full KafkaMCP toolkit with all available tools."""
    return [
        KafkaConsumeTool(mcp_client=mcp_client),
        KafkaProduceTool(mcp_client=mcp_client),
        KafkaListTopicsTool(mcp_client=mcp_client),
        KafkaSearchTool(mcp_client=mcp_client),
        KafkaWatchTool(mcp_client=mcp_client),
    ]
