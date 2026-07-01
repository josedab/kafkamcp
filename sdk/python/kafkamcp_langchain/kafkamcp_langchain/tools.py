"""Pre-built LangChain tool wrappers for KafkaMCP operations."""

import inspect
import json
from typing import Any, Type

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
    approval_token: str = Field(description="Short-lived approval token for this exact request")
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
    approval_token: str = Field(description="Short-lived approval token for this exact request")
    idempotency_key: str = Field(default="", description="Optional retry key (8-128 characters)")
    partitions: int = Field(default=1, description="Number of partitions")
    replication_factor: int = Field(default=1, description="Replication factor")
    configs: dict[str, str] = Field(default_factory=dict, description="Topic configurations")
    cluster: str = Field(default="", description="Cluster name for multi-cluster setups")


class AlterTopicInput(BaseModel):
    """Input for altering a Kafka topic."""
    name: str = Field(description="Topic name")
    configs: dict[str, str] = Field(description="Configs to change")
    approval_token: str = Field(description="Short-lived approval token for this exact request")
    cluster: str = Field(default="", description="Cluster name for multi-cluster setups")


class DeleteTopicInput(BaseModel):
    """Input for deleting a Kafka topic."""
    name: str = Field(description="Topic name to delete")
    approval_token: str = Field(description="Short-lived approval token for this exact request")
    idempotency_key: str = Field(default="", description="Optional retry key (8-128 characters)")
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
    approval_token: str = Field(description="Short-lived approval token for this exact request")
    strategy: str = Field(default="latest", description="Reset strategy: earliest, latest, or timestamp")
    value: str = Field(default="", description="ISO 8601 timestamp when strategy=timestamp")
    cluster: str = Field(default="", description="Cluster name for multi-cluster setups")


class SchemaInput(BaseModel):
    """Input for getting a schema."""
    subject: str = Field(description="Schema subject name")
    version: str = Field(default="latest", description="Version: latest or number")


class DLQAnalyzeInput(BaseModel):
    """Input for DLQ analysis."""
    topic: str = Field(description="DLQ topic name")
    limit: int = Field(default=100, description="Max messages to process")
    cluster: str = Field(default="", description="Cluster name for multi-cluster setups")


def _build_args(model: BaseModel) -> dict[str, Any]:
    """Build tool arguments dict, excluding empty/default values."""
    args = {}
    for k, v in model.model_dump().items():
        if v != "" and v != 0 and v is not None and v != {} and v != []:
            args[k] = v
    return args


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


def _call_sync(mcp_client: Any, name: str, args: dict[str, Any]) -> Any:
    if mcp_client is None:
        raise RuntimeError("MCP client not configured")

    result = mcp_client.call_tool(name, args)
    if inspect.isawaitable(result):
        if inspect.iscoroutine(result):
            result.close()
        raise RuntimeError(
            "the configured MCP client is asynchronous; invoke this LangChain tool with ainvoke"
        )
    return _format_tool_result(result)


async def _call_async(mcp_client: Any, name: str, args: dict[str, Any]) -> Any:
    if mcp_client is None:
        raise RuntimeError("MCP client not configured")

    result = mcp_client.call_tool(name, args)
    if inspect.isawaitable(result):
        result = await result
    return _format_tool_result(result)


class _KafkaMCPTool(BaseTool):
    mcp_client: Any = Field(default=None, exclude=True)

    async def _arun(self, *args: Any, **kwargs: Any) -> Any:
        if self.args_schema is None:
            raise RuntimeError(f"{self.name} does not define an argument schema")

        values = dict(kwargs)
        field_names = list(self.args_schema.model_fields)
        if len(args) > len(field_names):
            raise TypeError(f"{self.name} received too many positional arguments")
        for field_name, value in zip(field_names, args):
            if field_name in values:
                raise TypeError(f"{self.name} received multiple values for {field_name}")
            values[field_name] = value

        return await _call_async(
            self.mcp_client,
            self.name,
            _build_args(self.args_schema(**values)),
        )


class KafkaConsumeTool(_KafkaMCPTool):
    """Tool for consuming messages from a Kafka topic via KafkaMCP."""
    name: str = "kafka_consume"
    description: str = "Read messages from a Kafka topic. Returns message key, value, headers, and metadata."
    args_schema: Type[BaseModel] = ConsumeInput
    mcp_client: Any = None

    def _run(self, topic: str, limit: int = 10, offset: str = "latest", timeout_ms: int = 5000, cluster: str = "") -> str:
        args = {"topic": topic, "limit": limit, "offset": offset, "timeout_ms": timeout_ms}
        if cluster:
            args["cluster"] = cluster
        return _call_sync(self.mcp_client, "kafka_consume", args)


class KafkaProduceTool(_KafkaMCPTool):
    """Tool for producing messages to a Kafka topic via KafkaMCP."""
    name: str = "kafka_produce"
    description: str = "Publish messages to a Kafka topic. Returns delivery confirmation."
    args_schema: Type[BaseModel] = ProduceInput
    mcp_client: Any = None

    def _run(self, topic: str, messages: list[dict[str, Any]], approval_token: str, cluster: str = "") -> str:
        args = {"topic": topic, "messages": messages, "approval_token": approval_token}
        if cluster:
            args["cluster"] = cluster
        return _call_sync(self.mcp_client, "kafka_produce", args)


class KafkaListTopicsTool(_KafkaMCPTool):
    """Tool for listing Kafka topics via KafkaMCP."""
    name: str = "kafka_list_topics"
    description: str = "List all Kafka topics with metadata including partition count and message count."
    args_schema: Type[BaseModel] = ListTopicsInput
    mcp_client: Any = None

    def _run(self, filter: str = "", include_internal: bool = False, cluster: str = "") -> str:
        args = {"filter": filter, "include_internal": include_internal}
        if cluster:
            args["cluster"] = cluster
        return _call_sync(self.mcp_client, "kafka_list_topics", args)


class KafkaSearchTool(_KafkaMCPTool):
    """Tool for searching messages in a Kafka topic via KafkaMCP."""
    name: str = "kafka_search"
    description: str = "Search messages in a Kafka topic by key, value content, or headers."
    args_schema: Type[BaseModel] = SearchInput
    mcp_client: Any = None

    def _run(self, topic: str, value_contains: str = "", key_filter: str = "", limit: int = 10, cluster: str = "") -> str:
        args = {"topic": topic, "value_contains": value_contains, "key_filter": key_filter, "limit": limit}
        if cluster:
            args["cluster"] = cluster
        return _call_sync(self.mcp_client, "kafka_search", args)


class KafkaDescribeTopicTool(_KafkaMCPTool):
    """Tool for getting detailed topic metadata via KafkaMCP."""
    name: str = "kafka_describe_topic"
    description: str = "Get detailed metadata for a Kafka topic including partitions, replicas, ISR, and configuration."
    args_schema: Type[BaseModel] = DescribeTopicInput
    mcp_client: Any = None

    def _run(self, topic: str, cluster: str = "") -> str:
        args = {"topic": topic}
        if cluster:
            args["cluster"] = cluster
        return _call_sync(self.mcp_client, "kafka_describe_topic", args)


class KafkaCreateTopicTool(_KafkaMCPTool):
    """Tool for creating a Kafka topic via KafkaMCP."""
    name: str = "kafka_create_topic"
    description: str = "Create a new Kafka topic with specified partitions and replication factor."
    args_schema: Type[BaseModel] = CreateTopicInput
    mcp_client: Any = None

    def _run(self, name: str, approval_token: str, idempotency_key: str = "",
             partitions: int = 1, replication_factor: int = 1,
             configs: dict = None, cluster: str = "") -> str:
        args = {
            "name": name,
            "partitions": partitions,
            "replication_factor": replication_factor,
            "approval_token": approval_token,
        }
        if idempotency_key:
            args["idempotency_key"] = idempotency_key
        if configs:
            args["configs"] = configs
        if cluster:
            args["cluster"] = cluster
        return _call_sync(self.mcp_client, "kafka_create_topic", args)


class KafkaAlterTopicTool(_KafkaMCPTool):
    """Tool for modifying Kafka topic configuration via KafkaMCP."""
    name: str = "kafka_alter_topic"
    description: str = "Modify configuration of an existing Kafka topic."
    args_schema: Type[BaseModel] = AlterTopicInput
    mcp_client: Any = None

    def _run(self, name: str, approval_token: str, configs: dict[str, str] = None, cluster: str = "") -> str:
        args = {"name": name, "configs": configs or {}, "approval_token": approval_token}
        if cluster:
            args["cluster"] = cluster
        return _call_sync(self.mcp_client, "kafka_alter_topic", args)


class KafkaDeleteTopicTool(_KafkaMCPTool):
    """Tool for deleting a Kafka topic via KafkaMCP."""
    name: str = "kafka_delete_topic"
    description: str = "Delete a Kafka topic. This is destructive."
    args_schema: Type[BaseModel] = DeleteTopicInput
    mcp_client: Any = None

    def _run(self, name: str, approval_token: str, idempotency_key: str = "", cluster: str = "") -> str:
        args = {"name": name, "approval_token": approval_token}
        if idempotency_key:
            args["idempotency_key"] = idempotency_key
        if cluster:
            args["cluster"] = cluster
        return _call_sync(self.mcp_client, "kafka_delete_topic", args)


class KafkaDescribeGroupTool(_KafkaMCPTool):
    """Tool for describing a consumer group via KafkaMCP."""
    name: str = "kafka_describe_consumer_group"
    description: str = "Get state, members, assignments, and per-partition lag of a consumer group."
    args_schema: Type[BaseModel] = DescribeGroupInput
    mcp_client: Any = None

    def _run(self, group_id: str, cluster: str = "") -> str:
        args = {"group_id": group_id}
        if cluster:
            args["cluster"] = cluster
        return _call_sync(self.mcp_client, "kafka_describe_consumer_group", args)


class KafkaListGroupsTool(_KafkaMCPTool):
    """Tool for listing consumer groups via KafkaMCP."""
    name: str = "kafka_list_consumer_groups"
    description: str = "List all consumer groups with state and lag summary."
    args_schema: Type[BaseModel] = ListGroupsInput
    mcp_client: Any = None

    def _run(self, state_filter: str = "", cluster: str = "") -> str:
        args = {}
        if state_filter:
            args["state_filter"] = state_filter
        if cluster:
            args["cluster"] = cluster
        return _call_sync(self.mcp_client, "kafka_list_consumer_groups", args)


class KafkaResetOffsetsTool(_KafkaMCPTool):
    """Tool for resetting consumer group offsets via KafkaMCP."""
    name: str = "kafka_reset_offsets"
    description: str = "Reset consumer group offsets to earliest, latest, or a specific timestamp."
    args_schema: Type[BaseModel] = ResetOffsetsInput
    mcp_client: Any = None

    def _run(self, group_id: str, topic: str, approval_token: str,
             strategy: str = "latest", value: str = "", cluster: str = "") -> str:
        args = {
            "group_id": group_id,
            "topic": topic,
            "strategy": strategy,
            "approval_token": approval_token,
        }
        if value:
            args["value"] = value
        if cluster:
            args["cluster"] = cluster
        return _call_sync(self.mcp_client, "kafka_reset_offsets", args)


class KafkaGetSchemaTool(_KafkaMCPTool):
    """Tool for getting a schema from Schema Registry via KafkaMCP."""
    name: str = "kafka_get_schema"
    description: str = "Get the full schema definition for a Schema Registry subject."
    args_schema: Type[BaseModel] = SchemaInput
    mcp_client: Any = None

    def _run(self, subject: str, version: str = "latest") -> str:
        return _call_sync(
            self.mcp_client,
            "kafka_get_schema",
            {"subject": subject, "version": version},
        )


class KafkaAnalyzeDLQTool(_KafkaMCPTool):
    """Tool for read-only DLQ analysis via KafkaMCP."""
    name: str = "kafka_analyze_dlq"
    description: str = "Analyze dead letter queue failures and return categories, counts, and samples."
    args_schema: Type[BaseModel] = DLQAnalyzeInput
    mcp_client: Any = None

    def _run(self, topic: str, limit: int = 100, cluster: str = "") -> str:
        args = {"topic": topic, "limit": limit}
        if cluster:
            args["cluster"] = cluster
        return _call_sync(self.mcp_client, "kafka_analyze_dlq", args)


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
    """Create read-only topic tools."""
    return [
        KafkaListTopicsTool(mcp_client=mcp_client),
        KafkaDescribeTopicTool(mcp_client=mcp_client),
    ]


def KafkaTopicMutationToolkit(mcp_client: Any) -> list[BaseTool]:
    """Create approval-gated topic mutation tools."""
    return [
        KafkaCreateTopicTool(mcp_client=mcp_client),
        KafkaAlterTopicTool(mcp_client=mcp_client),
        KafkaDeleteTopicTool(mcp_client=mcp_client),
    ]


def KafkaSchemaToolkit(mcp_client: Any) -> list[BaseTool]:
    """Create supported schema registry tools."""
    return [KafkaGetSchemaTool(mcp_client=mcp_client)]


def KafkaConsumerGroupToolkit(mcp_client: Any) -> list[BaseTool]:
    """Create read-only consumer group tools."""
    return [
        KafkaDescribeGroupTool(mcp_client=mcp_client),
        KafkaListGroupsTool(mcp_client=mcp_client),
    ]


def KafkaConsumerGroupMutationToolkit(mcp_client: Any) -> list[BaseTool]:
    """Create approval-gated consumer group tools."""
    return [KafkaResetOffsetsTool(mcp_client=mcp_client)]


def KafkaOperationsToolkit(mcp_client: Any) -> list[BaseTool]:
    """Create supported read-only operational tools."""
    return [KafkaAnalyzeDLQTool(mcp_client=mcp_client)]


def KafkaGuardedToolkit(mcp_client: Any) -> list[BaseTool]:
    """Create all approval-gated mutation wrappers."""
    return [
        KafkaProduceTool(mcp_client=mcp_client),
        *KafkaTopicMutationToolkit(mcp_client),
        *KafkaConsumerGroupMutationToolkit(mcp_client),
    ]


def KafkaMCPToolkit(mcp_client: Any) -> list[BaseTool]:
    """Create the safe, read-only default KafkaMCP toolkit."""
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
