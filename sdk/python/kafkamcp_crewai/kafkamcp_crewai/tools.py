"""CrewAI tool wrappers for KafkaMCP operations."""

from typing import Any

from crewai.tools import BaseTool
from pydantic import Field


class KafkaConsumeTool(BaseTool):
    """Consume messages from a Kafka topic."""
    name: str = "kafka_consume"
    description: str = "Read messages from a Kafka topic. Provide topic name, optional limit, and offset."
    mcp_client: Any = None

    def _run(self, topic: str, limit: int = 10, offset: str = "latest", cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"topic": topic, "limit": limit, "offset": offset}
        if cluster:
            args["cluster"] = cluster
        return self.mcp_client.call_tool("kafka_consume", args)


class KafkaProduceTool(BaseTool):
    """Produce messages to a Kafka topic."""
    name: str = "kafka_produce"
    description: str = "Publish messages to a Kafka topic. Provide topic name and messages array."
    mcp_client: Any = None

    def _run(self, topic: str, messages: list[dict[str, Any]], cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"topic": topic, "messages": messages}
        if cluster:
            args["cluster"] = cluster
        return self.mcp_client.call_tool("kafka_produce", args)


class KafkaListTopicsTool(BaseTool):
    """List all Kafka topics."""
    name: str = "kafka_list_topics"
    description: str = "List all Kafka topics with metadata."
    mcp_client: Any = None

    def _run(self, filter: str = "", cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"filter": filter}
        if cluster:
            args["cluster"] = cluster
        return self.mcp_client.call_tool("kafka_list_topics", args)


class KafkaDescribeTopicTool(BaseTool):
    """Get detailed topic metadata."""
    name: str = "kafka_describe_topic"
    description: str = "Get detailed metadata for a Kafka topic including partitions, replicas, and configuration."
    mcp_client: Any = None

    def _run(self, topic: str, cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"topic": topic}
        if cluster:
            args["cluster"] = cluster
        return self.mcp_client.call_tool("kafka_describe_topic", args)


class KafkaSearchTool(BaseTool):
    """Search messages in a Kafka topic."""
    name: str = "kafka_search"
    description: str = "Search messages in a Kafka topic by content, key, or headers."
    mcp_client: Any = None

    def _run(self, topic: str, value_contains: str = "", limit: int = 10, cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"topic": topic, "value_contains": value_contains, "limit": limit}
        if cluster:
            args["cluster"] = cluster
        return self.mcp_client.call_tool("kafka_search", args)


class KafkaWatchTool(BaseTool):
    """Watch Kafka topics for real-time events."""
    name: str = "kafka_watch"
    description: str = "Subscribe to topic patterns for event-driven notifications."
    mcp_client: Any = None

    def _run(self, topic_pattern: str, key_filter: str = "", webhook_url: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"action": "subscribe", "topic_pattern": topic_pattern}
        if key_filter:
            args["key_filter"] = key_filter
        if webhook_url:
            args["webhook_url"] = webhook_url
        return self.mcp_client.call_tool("kafka_watch", args)


class KafkaCreateTopicTool(BaseTool):
    """Create a new Kafka topic."""
    name: str = "kafka_create_topic"
    description: str = "Create a new Kafka topic with partitions and replication factor."
    mcp_client: Any = None

    def _run(self, name: str, partitions: int = 1, replication_factor: int = 1, cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"name": name, "partitions": partitions, "replication_factor": replication_factor}
        if cluster:
            args["cluster"] = cluster
        return self.mcp_client.call_tool("kafka_create_topic", args)


class KafkaDeleteTopicTool(BaseTool):
    """Delete a Kafka topic."""
    name: str = "kafka_delete_topic"
    description: str = "Delete a Kafka topic. This is a destructive operation."
    mcp_client: Any = None

    def _run(self, name: str, cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"name": name}
        if cluster:
            args["cluster"] = cluster
        return self.mcp_client.call_tool("kafka_delete_topic", args)


class KafkaDescribeGroupTool(BaseTool):
    """Describe a consumer group."""
    name: str = "kafka_describe_consumer_group"
    description: str = "Get consumer group state, members, and per-partition lag."
    mcp_client: Any = None

    def _run(self, group_id: str, cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"group_id": group_id}
        if cluster:
            args["cluster"] = cluster
        return self.mcp_client.call_tool("kafka_describe_consumer_group", args)


class KafkaListGroupsTool(BaseTool):
    """List all consumer groups."""
    name: str = "kafka_list_consumer_groups"
    description: str = "List all consumer groups with state and lag summary."
    mcp_client: Any = None

    def _run(self, state_filter: str = "", cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {}
        if state_filter:
            args["state_filter"] = state_filter
        if cluster:
            args["cluster"] = cluster
        return self.mcp_client.call_tool("kafka_list_consumer_groups", args)


class KafkaGetSchemaTool(BaseTool):
    """Get a schema from Schema Registry."""
    name: str = "kafka_get_schema"
    description: str = "Get schema definition for a Schema Registry subject."
    mcp_client: Any = None

    def _run(self, subject: str, version: str = "latest") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        return self.mcp_client.call_tool("kafka_get_schema", {"subject": subject, "version": version})


class KafkaDLQRemediateTool(BaseTool):
    """Analyze and remediate DLQ messages."""
    name: str = "kafka_remediate_dlq"
    description: str = "Analyze DLQ messages and suggest or apply remediation actions."
    mcp_client: Any = None

    def _run(self, topic: str, action: str = "analyze", limit: int = 100, cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"topic": topic, "action": action, "limit": limit}
        if cluster:
            args["cluster"] = cluster
        return self.mcp_client.call_tool("kafka_remediate_dlq", args)


class KafkaAggregateTool(BaseTool):
    """Aggregate messages from a topic."""
    name: str = "kafka_aggregate"
    description: str = "Aggregate messages with count, sum, avg, min, max operations."
    mcp_client: Any = None

    def _run(self, topic: str, operation: str = "count", field: str = "", limit: int = 1000, cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"topic": topic, "operation": operation, "limit": limit}
        if field:
            args["field"] = field
        if cluster:
            args["cluster"] = cluster
        return self.mcp_client.call_tool("kafka_aggregate", args)


def create_kafka_tools(mcp_client: Any) -> list[BaseTool]:
    """Create all KafkaMCP tools for use with CrewAI agents."""
    return [
        KafkaConsumeTool(mcp_client=mcp_client),
        KafkaProduceTool(mcp_client=mcp_client),
        KafkaListTopicsTool(mcp_client=mcp_client),
        KafkaDescribeTopicTool(mcp_client=mcp_client),
        KafkaCreateTopicTool(mcp_client=mcp_client),
        KafkaDeleteTopicTool(mcp_client=mcp_client),
        KafkaSearchTool(mcp_client=mcp_client),
        KafkaWatchTool(mcp_client=mcp_client),
        KafkaDescribeGroupTool(mcp_client=mcp_client),
        KafkaListGroupsTool(mcp_client=mcp_client),
        KafkaGetSchemaTool(mcp_client=mcp_client),
        KafkaDLQRemediateTool(mcp_client=mcp_client),
        KafkaAggregateTool(mcp_client=mcp_client),
    ]


def create_monitoring_tools(mcp_client: Any) -> list[BaseTool]:
    """Create monitoring-focused tools for CrewAI monitoring agents."""
    return [
        KafkaListTopicsTool(mcp_client=mcp_client),
        KafkaDescribeTopicTool(mcp_client=mcp_client),
        KafkaDescribeGroupTool(mcp_client=mcp_client),
        KafkaListGroupsTool(mcp_client=mcp_client),
        KafkaSearchTool(mcp_client=mcp_client),
        KafkaWatchTool(mcp_client=mcp_client),
    ]


def create_remediation_tools(mcp_client: Any) -> list[BaseTool]:
    """Create DLQ remediation-focused tools for CrewAI remediation agents."""
    return [
        KafkaConsumeTool(mcp_client=mcp_client),
        KafkaDLQRemediateTool(mcp_client=mcp_client),
        KafkaSearchTool(mcp_client=mcp_client),
        KafkaGetSchemaTool(mcp_client=mcp_client),
    ]
