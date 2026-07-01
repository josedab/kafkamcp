"""CrewAI tool wrappers for KafkaMCP operations."""

import inspect
import json
from typing import Any

from crewai.tools import BaseTool


def _format_tool_result(result: Any) -> Any:
    """Convert MCP content results to agent-readable text."""
    content = getattr(result, "content", None)
    if content is None:
        return result

    texts = [item.text for item in content if isinstance(getattr(item, "text", None), str)]
    if texts:
        return "\n".join(texts)

    structured = getattr(result, "structuredContent", None)
    if structured is not None:
        return json.dumps(structured, separators=(",", ":"), default=str)

    if hasattr(result, "model_dump"):
        return json.dumps(result.model_dump(mode="json", by_alias=True), separators=(",", ":"), default=str)
    return str(result)


def _call_tool(mcp_client: Any, name: str, args: dict[str, Any]) -> Any:
    if mcp_client is None:
        raise RuntimeError("MCP client not configured")

    result = mcp_client.call_tool(name, args)
    if inspect.isawaitable(result):
        if inspect.iscoroutine(result):
            result.close()
        raise RuntimeError(
            "CrewAI tools require a synchronous MCP client; use StdioMCPClient"
        )
    return _format_tool_result(result)


class KafkaConsumeTool(BaseTool):
    """Consume messages from a Kafka topic."""
    name: str = "kafka_consume"
    description: str = "Read messages from a Kafka topic. Provide topic name, optional limit, and offset."
    mcp_client: Any = None

    def _run(self, topic: str, limit: int = 10, offset: str = "latest", cluster: str = "") -> str:
        args = {"topic": topic, "limit": limit, "offset": offset}
        if cluster:
            args["cluster"] = cluster
        return _call_tool(self.mcp_client, "kafka_consume", args)


class KafkaProduceTool(BaseTool):
    """Produce messages to a Kafka topic."""
    name: str = "kafka_produce"
    description: str = "Publish messages to a Kafka topic. Provide topic name and messages array."
    mcp_client: Any = None

    def _run(self, topic: str, messages: list[dict[str, Any]], approval_token: str, cluster: str = "") -> str:
        args = {"topic": topic, "messages": messages, "approval_token": approval_token}
        if cluster:
            args["cluster"] = cluster
        return _call_tool(self.mcp_client, "kafka_produce", args)


class KafkaListTopicsTool(BaseTool):
    """List all Kafka topics."""
    name: str = "kafka_list_topics"
    description: str = "List all Kafka topics with metadata."
    mcp_client: Any = None

    def _run(self, filter: str = "", cluster: str = "") -> str:
        args = {"filter": filter}
        if cluster:
            args["cluster"] = cluster
        return _call_tool(self.mcp_client, "kafka_list_topics", args)


class KafkaDescribeTopicTool(BaseTool):
    """Get detailed topic metadata."""
    name: str = "kafka_describe_topic"
    description: str = "Get detailed metadata for a Kafka topic including partitions, replicas, and configuration."
    mcp_client: Any = None

    def _run(self, topic: str, cluster: str = "") -> str:
        args = {"topic": topic}
        if cluster:
            args["cluster"] = cluster
        return _call_tool(self.mcp_client, "kafka_describe_topic", args)


class KafkaSearchTool(BaseTool):
    """Search messages in a Kafka topic."""
    name: str = "kafka_search"
    description: str = "Search messages in a Kafka topic by content, key, or headers."
    mcp_client: Any = None

    def _run(self, topic: str, value_contains: str = "", limit: int = 10, cluster: str = "") -> str:
        args = {"topic": topic, "value_contains": value_contains, "limit": limit}
        if cluster:
            args["cluster"] = cluster
        return _call_tool(self.mcp_client, "kafka_search", args)


class KafkaCreateTopicTool(BaseTool):
    """Create a new Kafka topic."""
    name: str = "kafka_create_topic"
    description: str = "Create a new Kafka topic with partitions and replication factor."
    mcp_client: Any = None

    def _run(self, name: str, approval_token: str, idempotency_key: str = "",
             partitions: int = 1, replication_factor: int = 1, cluster: str = "") -> str:
        args = {
            "name": name,
            "partitions": partitions,
            "replication_factor": replication_factor,
            "approval_token": approval_token,
        }
        if idempotency_key:
            args["idempotency_key"] = idempotency_key
        if cluster:
            args["cluster"] = cluster
        return _call_tool(self.mcp_client, "kafka_create_topic", args)


class KafkaDeleteTopicTool(BaseTool):
    """Delete a Kafka topic."""
    name: str = "kafka_delete_topic"
    description: str = "Delete a Kafka topic. This is a destructive operation."
    mcp_client: Any = None

    def _run(self, name: str, approval_token: str, idempotency_key: str = "", cluster: str = "") -> str:
        args = {"name": name, "approval_token": approval_token}
        if idempotency_key:
            args["idempotency_key"] = idempotency_key
        if cluster:
            args["cluster"] = cluster
        return _call_tool(self.mcp_client, "kafka_delete_topic", args)


class KafkaDescribeGroupTool(BaseTool):
    """Describe a consumer group."""
    name: str = "kafka_describe_consumer_group"
    description: str = "Get consumer group state, members, and per-partition lag."
    mcp_client: Any = None

    def _run(self, group_id: str, cluster: str = "") -> str:
        args = {"group_id": group_id}
        if cluster:
            args["cluster"] = cluster
        return _call_tool(self.mcp_client, "kafka_describe_consumer_group", args)


class KafkaListGroupsTool(BaseTool):
    """List all consumer groups."""
    name: str = "kafka_list_consumer_groups"
    description: str = "List all consumer groups with state and lag summary."
    mcp_client: Any = None

    def _run(self, state_filter: str = "", cluster: str = "") -> str:
        args = {}
        if state_filter:
            args["state_filter"] = state_filter
        if cluster:
            args["cluster"] = cluster
        return _call_tool(self.mcp_client, "kafka_list_consumer_groups", args)


class KafkaGetSchemaTool(BaseTool):
    """Get a schema from Schema Registry."""
    name: str = "kafka_get_schema"
    description: str = "Get schema definition for a Schema Registry subject."
    mcp_client: Any = None

    def _run(self, subject: str, version: str = "latest") -> str:
        return _call_tool(
            self.mcp_client,
            "kafka_get_schema",
            {"subject": subject, "version": version},
        )


class KafkaAnalyzeDLQTool(BaseTool):
    """Analyze DLQ messages."""
    name: str = "kafka_analyze_dlq"
    description: str = "Analyze DLQ messages and return failure categories and samples."
    mcp_client: Any = None

    def _run(self, topic: str, limit: int = 100, cluster: str = "") -> str:
        args = {"topic": topic, "limit": limit}
        if cluster:
            args["cluster"] = cluster
        return _call_tool(self.mcp_client, "kafka_analyze_dlq", args)


def create_kafka_tools(mcp_client: Any) -> list[BaseTool]:
    """Create the safe, read-only default KafkaMCP tools."""
    return [
        KafkaConsumeTool(mcp_client=mcp_client),
        KafkaListTopicsTool(mcp_client=mcp_client),
        KafkaDescribeTopicTool(mcp_client=mcp_client),
        KafkaSearchTool(mcp_client=mcp_client),
        KafkaDescribeGroupTool(mcp_client=mcp_client),
        KafkaListGroupsTool(mcp_client=mcp_client),
        KafkaGetSchemaTool(mcp_client=mcp_client),
        KafkaAnalyzeDLQTool(mcp_client=mcp_client),
    ]


def create_guarded_tools(mcp_client: Any) -> list[BaseTool]:
    """Create approval-gated mutation wrappers."""
    return [
        KafkaProduceTool(mcp_client=mcp_client),
        KafkaCreateTopicTool(mcp_client=mcp_client),
        KafkaDeleteTopicTool(mcp_client=mcp_client),
    ]


def create_monitoring_tools(mcp_client: Any) -> list[BaseTool]:
    """Create monitoring-focused tools for CrewAI monitoring agents."""
    return [
        KafkaListTopicsTool(mcp_client=mcp_client),
        KafkaDescribeTopicTool(mcp_client=mcp_client),
        KafkaDescribeGroupTool(mcp_client=mcp_client),
        KafkaListGroupsTool(mcp_client=mcp_client),
        KafkaSearchTool(mcp_client=mcp_client),
    ]


def create_remediation_tools(mcp_client: Any) -> list[BaseTool]:
    """Create DLQ remediation-focused tools for CrewAI remediation agents."""
    return [
        KafkaConsumeTool(mcp_client=mcp_client),
        KafkaAnalyzeDLQTool(mcp_client=mcp_client),
        KafkaSearchTool(mcp_client=mcp_client),
        KafkaGetSchemaTool(mcp_client=mcp_client),
    ]
