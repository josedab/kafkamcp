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
    cluster: str = Field(default="", description="Cluster name for multi-cluster setups")


class ProduceInput(BaseModel):
    """Input for producing Kafka messages."""
    topic: str = Field(description="Kafka topic name")
    messages: list[dict[str, Any]] = Field(description="Messages to produce: [{key, value, headers}]")
    cluster: str = Field(default="", description="Cluster name for multi-cluster setups")


class ListTopicsInput(BaseModel):
    """Input for listing Kafka topics."""
    filter: str = Field(default="", description="Glob pattern to filter topic names")
    include_internal: bool = Field(default=False, description="Include internal topics")
    cluster: str = Field(default="", description="Cluster name for multi-cluster setups")


class SearchInput(BaseModel):
    """Input for searching Kafka messages."""
    topic: str = Field(description="Kafka topic name")
    value_contains: str = Field(default="", description="Filter messages containing this string")
    key_filter: str = Field(default="", description="Filter by message key")
    limit: int = Field(default=10, description="Max matching messages to return")
    cluster: str = Field(default="", description="Cluster name for multi-cluster setups")


class DescribeTopicInput(BaseModel):
    """Input for describing a Kafka topic."""
    topic: str = Field(description="Topic name")
    cluster: str = Field(default="", description="Cluster name for multi-cluster setups")


class CreateTopicInput(BaseModel):
    """Input for creating a Kafka topic."""
    name: str = Field(description="Topic name")
    partitions: int = Field(default=1, description="Number of partitions")
    replication_factor: int = Field(default=1, description="Replication factor")
    configs: dict[str, str] = Field(default_factory=dict, description="Topic configurations")
    cluster: str = Field(default="", description="Cluster name for multi-cluster setups")


class AlterTopicInput(BaseModel):
    """Input for altering a Kafka topic."""
    name: str = Field(description="Topic name")
    configs: dict[str, str] = Field(description="Configs to change")
    cluster: str = Field(default="", description="Cluster name for multi-cluster setups")


class DeleteTopicInput(BaseModel):
    """Input for deleting a Kafka topic."""
    name: str = Field(description="Topic name to delete")
    cluster: str = Field(default="", description="Cluster name for multi-cluster setups")


class DescribeGroupInput(BaseModel):
    """Input for describing a consumer group."""
    group_id: str = Field(description="Consumer group ID")
    cluster: str = Field(default="", description="Cluster name for multi-cluster setups")


class ListGroupsInput(BaseModel):
    """Input for listing consumer groups."""
    state_filter: str = Field(default="", description="Filter by state (Stable, Empty, Dead)")
    cluster: str = Field(default="", description="Cluster name for multi-cluster setups")


class ResetOffsetsInput(BaseModel):
    """Input for resetting consumer group offsets."""
    group_id: str = Field(description="Consumer group ID")
    topic: str = Field(description="Topic name")
    to: str = Field(default="latest", description="Reset to: earliest, latest, or ISO 8601 timestamp")
    cluster: str = Field(default="", description="Cluster name for multi-cluster setups")


class WatchInput(BaseModel):
    """Input for watching Kafka topics."""
    action: str = Field(default="subscribe", description="Action: subscribe, unsubscribe, list")
    topic_pattern: str = Field(default="", description="Topic pattern for subscribe")
    key_filter: str = Field(default="", description="Key filter expression")
    value_filter: str = Field(default="", description="Value filter expression")
    webhook_url: str = Field(default="", description="Webhook URL for notifications")
    subscription_id: str = Field(default="", description="Subscription ID for unsubscribe")


class SchemaInput(BaseModel):
    """Input for getting a schema."""
    subject: str = Field(description="Schema subject name")
    version: str = Field(default="latest", description="Version: latest or number")


class SchemaDiffInput(BaseModel):
    """Input for diffing schemas."""
    subject: str = Field(description="Schema subject name")
    version_from: int = Field(description="Source version")
    version_to: int = Field(description="Target version")


class DLQAnalyzeInput(BaseModel):
    """Input for DLQ analysis."""
    topic: str = Field(description="DLQ topic name")
    action: str = Field(default="analyze", description="Action: analyze, retry, route, suggest")
    target_topic: str = Field(default="", description="Target topic for route action")
    limit: int = Field(default=100, description="Max messages to process")
    cluster: str = Field(default="", description="Cluster name for multi-cluster setups")


class AggregateInput(BaseModel):
    """Input for aggregating messages."""
    topic: str = Field(description="Topic name")
    operation: str = Field(default="count", description="Aggregation: count, sum, avg, min, max")
    field: str = Field(default="", description="JSON field to aggregate on")
    group_by: str = Field(default="", description="JSON field to group by")
    limit: int = Field(default=1000, description="Max messages to scan")
    cluster: str = Field(default="", description="Cluster name for multi-cluster setups")


class NLQueryInput(BaseModel):
    """Input for natural language query."""
    query: str = Field(description="Natural language query about Kafka")
    cluster: str = Field(default="", description="Cluster name for multi-cluster setups")


def _build_args(model: BaseModel) -> dict[str, Any]:
    """Build tool arguments dict, excluding empty/default values."""
    args = {}
    for k, v in model.model_dump().items():
        if v != "" and v != 0 and v is not None and v != {} and v != []:
            args[k] = v
    return args


class KafkaConsumeTool(BaseTool):
    """Tool for consuming messages from a Kafka topic via KafkaMCP."""
    name: str = "kafka_consume"
    description: str = "Read messages from a Kafka topic. Returns message key, value, headers, and metadata."
    args_schema: Type[BaseModel] = ConsumeInput
    mcp_client: Any = None

    def _run(self, topic: str, limit: int = 10, offset: str = "latest", timeout_ms: int = 5000, cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"topic": topic, "limit": limit, "offset": offset, "timeout_ms": timeout_ms}
        if cluster:
            args["cluster"] = cluster
        return self.mcp_client.call_tool("kafka_consume", args)

    async def _arun(self, topic: str, limit: int = 10, offset: str = "latest", timeout_ms: int = 5000, cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"topic": topic, "limit": limit, "offset": offset, "timeout_ms": timeout_ms}
        if cluster:
            args["cluster"] = cluster
        return await self.mcp_client.call_tool("kafka_consume", args)


class KafkaProduceTool(BaseTool):
    """Tool for producing messages to a Kafka topic via KafkaMCP."""
    name: str = "kafka_produce"
    description: str = "Publish messages to a Kafka topic. Returns delivery confirmation."
    args_schema: Type[BaseModel] = ProduceInput
    mcp_client: Any = None

    def _run(self, topic: str, messages: list[dict[str, Any]], cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"topic": topic, "messages": messages}
        if cluster:
            args["cluster"] = cluster
        return self.mcp_client.call_tool("kafka_produce", args)

    async def _arun(self, topic: str, messages: list[dict[str, Any]], cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"topic": topic, "messages": messages}
        if cluster:
            args["cluster"] = cluster
        return await self.mcp_client.call_tool("kafka_produce", args)


class KafkaListTopicsTool(BaseTool):
    """Tool for listing Kafka topics via KafkaMCP."""
    name: str = "kafka_list_topics"
    description: str = "List all Kafka topics with metadata including partition count and message count."
    args_schema: Type[BaseModel] = ListTopicsInput
    mcp_client: Any = None

    def _run(self, filter: str = "", include_internal: bool = False, cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"filter": filter, "include_internal": include_internal}
        if cluster:
            args["cluster"] = cluster
        return self.mcp_client.call_tool("kafka_list_topics", args)


class KafkaSearchTool(BaseTool):
    """Tool for searching messages in a Kafka topic via KafkaMCP."""
    name: str = "kafka_search"
    description: str = "Search messages in a Kafka topic by key, value content, or headers."
    args_schema: Type[BaseModel] = SearchInput
    mcp_client: Any = None

    def _run(self, topic: str, value_contains: str = "", key_filter: str = "", limit: int = 10, cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"topic": topic, "value_contains": value_contains, "key_filter": key_filter, "limit": limit}
        if cluster:
            args["cluster"] = cluster
        return self.mcp_client.call_tool("kafka_search", args)


class KafkaDescribeTopicTool(BaseTool):
    """Tool for getting detailed topic metadata via KafkaMCP."""
    name: str = "kafka_describe_topic"
    description: str = "Get detailed metadata for a Kafka topic including partitions, replicas, ISR, and configuration."
    args_schema: Type[BaseModel] = DescribeTopicInput
    mcp_client: Any = None

    def _run(self, topic: str, cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"topic": topic}
        if cluster:
            args["cluster"] = cluster
        return self.mcp_client.call_tool("kafka_describe_topic", args)


class KafkaCreateTopicTool(BaseTool):
    """Tool for creating a Kafka topic via KafkaMCP."""
    name: str = "kafka_create_topic"
    description: str = "Create a new Kafka topic with specified partitions and replication factor."
    args_schema: Type[BaseModel] = CreateTopicInput
    mcp_client: Any = None

    def _run(self, name: str, partitions: int = 1, replication_factor: int = 1, configs: dict = None, cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"name": name, "partitions": partitions, "replication_factor": replication_factor}
        if configs:
            args["configs"] = configs
        if cluster:
            args["cluster"] = cluster
        return self.mcp_client.call_tool("kafka_create_topic", args)


class KafkaAlterTopicTool(BaseTool):
    """Tool for modifying Kafka topic configuration via KafkaMCP."""
    name: str = "kafka_alter_topic"
    description: str = "Modify configuration of an existing Kafka topic."
    args_schema: Type[BaseModel] = AlterTopicInput
    mcp_client: Any = None

    def _run(self, name: str, configs: dict[str, str] = None, cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"name": name, "configs": configs or {}}
        if cluster:
            args["cluster"] = cluster
        return self.mcp_client.call_tool("kafka_alter_topic", args)


class KafkaDeleteTopicTool(BaseTool):
    """Tool for deleting a Kafka topic via KafkaMCP."""
    name: str = "kafka_delete_topic"
    description: str = "Delete a Kafka topic. This is destructive."
    args_schema: Type[BaseModel] = DeleteTopicInput
    mcp_client: Any = None

    def _run(self, name: str, cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"name": name}
        if cluster:
            args["cluster"] = cluster
        return self.mcp_client.call_tool("kafka_delete_topic", args)


class KafkaDescribeGroupTool(BaseTool):
    """Tool for describing a consumer group via KafkaMCP."""
    name: str = "kafka_describe_consumer_group"
    description: str = "Get state, members, assignments, and per-partition lag of a consumer group."
    args_schema: Type[BaseModel] = DescribeGroupInput
    mcp_client: Any = None

    def _run(self, group_id: str, cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"group_id": group_id}
        if cluster:
            args["cluster"] = cluster
        return self.mcp_client.call_tool("kafka_describe_consumer_group", args)


class KafkaListGroupsTool(BaseTool):
    """Tool for listing consumer groups via KafkaMCP."""
    name: str = "kafka_list_consumer_groups"
    description: str = "List all consumer groups with state and lag summary."
    args_schema: Type[BaseModel] = ListGroupsInput
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


class KafkaResetOffsetsTool(BaseTool):
    """Tool for resetting consumer group offsets via KafkaMCP."""
    name: str = "kafka_reset_offsets"
    description: str = "Reset consumer group offsets to earliest, latest, or a specific timestamp."
    args_schema: Type[BaseModel] = ResetOffsetsInput
    mcp_client: Any = None

    def _run(self, group_id: str, topic: str, to: str = "latest", cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"group_id": group_id, "topic": topic, "to": to}
        if cluster:
            args["cluster"] = cluster
        return self.mcp_client.call_tool("kafka_reset_offsets", args)


class KafkaWatchTool(BaseTool):
    """Tool for registering event-driven watches on Kafka topics."""
    name: str = "kafka_watch"
    description: str = "Subscribe to topic patterns for real-time notifications when matching messages arrive."
    args_schema: Type[BaseModel] = WatchInput
    mcp_client: Any = None

    def _run(self, action: str = "subscribe", topic_pattern: str = "", key_filter: str = "",
             value_filter: str = "", webhook_url: str = "", subscription_id: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"action": action}
        if topic_pattern:
            args["topic_pattern"] = topic_pattern
        if key_filter:
            args["key_filter"] = key_filter
        if value_filter:
            args["value_filter"] = value_filter
        if webhook_url:
            args["webhook_url"] = webhook_url
        if subscription_id:
            args["subscription_id"] = subscription_id
        return self.mcp_client.call_tool("kafka_watch", args)


class KafkaGetSchemaTool(BaseTool):
    """Tool for getting a schema from Schema Registry via KafkaMCP."""
    name: str = "kafka_get_schema"
    description: str = "Get the full schema definition for a Schema Registry subject."
    args_schema: Type[BaseModel] = SchemaInput
    mcp_client: Any = None

    def _run(self, subject: str, version: str = "latest") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        return self.mcp_client.call_tool("kafka_get_schema", {"subject": subject, "version": version})


class KafkaSchemaDiffTool(BaseTool):
    """Tool for diffing schema versions via KafkaMCP."""
    name: str = "kafka_schema_diff"
    description: str = "Compare two schema versions and show field-level changes."
    args_schema: Type[BaseModel] = SchemaDiffInput
    mcp_client: Any = None

    def _run(self, subject: str, version_from: int = 1, version_to: int = 2) -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        return self.mcp_client.call_tool("kafka_schema_diff", {
            "subject": subject, "version_from": version_from, "version_to": version_to
        })


class KafkaDLQRemediateTool(BaseTool):
    """Tool for DLQ analysis and remediation via KafkaMCP."""
    name: str = "kafka_remediate_dlq"
    description: str = "Analyze and remediate dead letter queue messages with retry, route, or fix suggestions."
    args_schema: Type[BaseModel] = DLQAnalyzeInput
    mcp_client: Any = None

    def _run(self, topic: str, action: str = "analyze", target_topic: str = "", limit: int = 100, cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"topic": topic, "action": action, "limit": limit}
        if target_topic:
            args["target_topic"] = target_topic
        if cluster:
            args["cluster"] = cluster
        return self.mcp_client.call_tool("kafka_remediate_dlq", args)


class KafkaAggregateTool(BaseTool):
    """Tool for aggregating Kafka messages via KafkaMCP."""
    name: str = "kafka_aggregate"
    description: str = "Aggregate messages from a topic with count, sum, avg, min, max operations."
    args_schema: Type[BaseModel] = AggregateInput
    mcp_client: Any = None

    def _run(self, topic: str, operation: str = "count", field: str = "", group_by: str = "", limit: int = 1000, cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"topic": topic, "operation": operation, "limit": limit}
        if field:
            args["field"] = field
        if group_by:
            args["group_by"] = group_by
        if cluster:
            args["cluster"] = cluster
        return self.mcp_client.call_tool("kafka_aggregate", args)


class KafkaNLQueryTool(BaseTool):
    """Tool for natural language queries about Kafka via KafkaMCP."""
    name: str = "kafka_natural_language_query"
    description: str = "Ask questions about Kafka in natural language."
    args_schema: Type[BaseModel] = NLQueryInput
    mcp_client: Any = None

    def _run(self, query: str, cluster: str = "") -> str:
        if self.mcp_client is None:
            return "Error: MCP client not configured"
        args = {"query": query}
        if cluster:
            args["cluster"] = cluster
        return self.mcp_client.call_tool("kafka_natural_language_query", args)


# --- Toolkit factory functions ---

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
    return [
        KafkaListTopicsTool(mcp_client=mcp_client),
        KafkaDescribeTopicTool(mcp_client=mcp_client),
        KafkaCreateTopicTool(mcp_client=mcp_client),
        KafkaAlterTopicTool(mcp_client=mcp_client),
        KafkaDeleteTopicTool(mcp_client=mcp_client),
    ]


def KafkaSchemaToolkit(mcp_client: Any) -> list[BaseTool]:
    """Create schema registry tools."""
    return [
        KafkaGetSchemaTool(mcp_client=mcp_client),
        KafkaSchemaDiffTool(mcp_client=mcp_client),
    ]


def KafkaConsumerGroupToolkit(mcp_client: Any) -> list[BaseTool]:
    """Create consumer group management tools."""
    return [
        KafkaDescribeGroupTool(mcp_client=mcp_client),
        KafkaListGroupsTool(mcp_client=mcp_client),
        KafkaResetOffsetsTool(mcp_client=mcp_client),
    ]


def KafkaOperationsToolkit(mcp_client: Any) -> list[BaseTool]:
    """Create operational tools for DLQ and aggregation."""
    return [
        KafkaDLQRemediateTool(mcp_client=mcp_client),
        KafkaAggregateTool(mcp_client=mcp_client),
        KafkaNLQueryTool(mcp_client=mcp_client),
    ]


def KafkaMCPToolkit(mcp_client: Any) -> list[BaseTool]:
    """Create the full KafkaMCP toolkit with all available tools."""
    return [
        KafkaConsumeTool(mcp_client=mcp_client),
        KafkaProduceTool(mcp_client=mcp_client),
        KafkaListTopicsTool(mcp_client=mcp_client),
        KafkaDescribeTopicTool(mcp_client=mcp_client),
        KafkaCreateTopicTool(mcp_client=mcp_client),
        KafkaAlterTopicTool(mcp_client=mcp_client),
        KafkaDeleteTopicTool(mcp_client=mcp_client),
        KafkaSearchTool(mcp_client=mcp_client),
        KafkaDescribeGroupTool(mcp_client=mcp_client),
        KafkaListGroupsTool(mcp_client=mcp_client),
        KafkaResetOffsetsTool(mcp_client=mcp_client),
        KafkaWatchTool(mcp_client=mcp_client),
        KafkaGetSchemaTool(mcp_client=mcp_client),
        KafkaSchemaDiffTool(mcp_client=mcp_client),
        KafkaDLQRemediateTool(mcp_client=mcp_client),
        KafkaAggregateTool(mcp_client=mcp_client),
        KafkaNLQueryTool(mcp_client=mcp_client),
    ]
