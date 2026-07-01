import asyncio
import sys

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.types import CallToolResult, TextContent

from kafkamcp_langchain import KafkaGuardedToolkit, KafkaMCPToolkit


class FakeClient:
    def call_tool(self, name, args):
        return {"name": name, "args": args}


class AsyncFakeClient:
    async def call_tool(self, name, args):
        return {"name": name, "args": args}


def test_default_toolkit_is_read_only():
    names = {tool.name for tool in KafkaMCPToolkit(FakeClient())}
    assert names == {
        "kafka_consume",
        "kafka_list_topics",
        "kafka_describe_topic",
        "kafka_search",
        "kafka_describe_consumer_group",
        "kafka_list_consumer_groups",
        "kafka_get_schema",
        "kafka_analyze_dlq",
    }


def test_guarded_toolkit_requires_approval_fields():
    tools = {tool.name: tool for tool in KafkaGuardedToolkit(FakeClient())}
    assert set(tools) == {
        "kafka_produce",
        "kafka_create_topic",
        "kafka_alter_topic",
        "kafka_delete_topic",
        "kafka_reset_offsets",
    }
    for tool in tools.values():
        assert "approval_token" in tool.args_schema.model_fields


# The 29 tools KafkaMCP actually exposes (core-read + ops-write + governance).
SUPPORTED_TOOLS = {
    "kafka_list_topics", "kafka_describe_topic", "kafka_list_schemas",
    "kafka_get_schema", "kafka_check_schema_compatibility", "kafka_consume",
    "kafka_search", "kafka_list_consumer_groups", "kafka_describe_consumer_group",
    "kafka_diagnose_lag", "kafka_analyze_dlq", "kafka_health", "kafka_metrics",
    "kafka_discover_clusters", "kafka_discover_tools", "kafka_produce",
    "kafka_create_topic", "kafka_delete_topic", "kafka_alter_topic",
    "kafka_reset_offsets", "kafka_acl_manage", "kafka_create_connector",
    "kafka_update_connector", "kafka_delete_connector", "kafka_restart_connector",
    "kafka_approval", "kafka_governance_decision", "kafka_policy_evaluate",
    "kafka_policy_simulate",
}


def test_every_emitted_tool_is_supported():
    from kafkamcp_langchain import tools as _tools

    emitted = set()
    for factory_name in _tools.__dict__:
        factory = getattr(_tools, factory_name)
        if callable(factory) and factory_name.endswith("Toolkit"):
            for tool in factory(FakeClient()):
                emitted.add(tool.name)
    assert emitted, "no tools emitted by any toolkit"
    unsupported = emitted - SUPPORTED_TOOLS
    assert not unsupported, f"SDK emits unsupported tools: {unsupported}"


def test_guarded_argument_contracts_match_server():
    client = FakeClient()
    tools = {tool.name: tool for tool in KafkaGuardedToolkit(client)}

    created = tools["kafka_create_topic"]._run(
        name="orders",
        approval_token="approval",
        idempotency_key="request-123",
        partitions=3,
        replication_factor=2,
    )
    assert created["args"]["name"] == "orders"
    assert created["args"]["approval_token"] == "approval"
    assert created["args"]["idempotency_key"] == "request-123"

    reset = tools["kafka_reset_offsets"]._run(
        group_id="orders-consumer",
        topic="orders",
        approval_token="approval",
        strategy="timestamp",
        value="2026-07-27T00:00:00Z",
    )
    assert reset["args"]["strategy"] == "timestamp"
    assert "to" not in reset["args"]
    assert reset["args"]["value"] == "2026-07-27T00:00:00Z"


def test_async_mcp_clients_use_langchain_async_invocation():
    tool = next(
        tool
        for tool in KafkaMCPToolkit(AsyncFakeClient())
        if tool.name == "kafka_list_topics"
    )

    result = asyncio.run(tool._arun(filter="orders.*"))

    assert result == {
        "name": "kafka_list_topics",
        "args": {"filter": "orders.*"},
    }
    with pytest.raises(RuntimeError, match="ainvoke"):
        tool._run(filter="orders.*")


def test_mcp_content_is_returned_as_agent_text():
    class ContentClient:
        def call_tool(self, name, args):
            return CallToolResult(
                content=[TextContent(type="text", text='{"topics":[]}')]
            )

    tool = next(
        tool
        for tool in KafkaMCPToolkit(ContentClient())
        if tool.name == "kafka_list_topics"
    )

    assert tool._run() == '{"topics":[]}'


def test_async_tool_invokes_a_real_mcp_process():
    server_code = """
from mcp.server.fastmcp import FastMCP

server = FastMCP("langchain-adapter-test")

@server.tool()
def kafka_list_topics(filter: str = "") -> str:
    return filter

server.run(transport="stdio")
"""

    async def invoke():
        params = StdioServerParameters(
            command=sys.executable,
            args=["-c", server_code],
        )
        async with stdio_client(params) as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                tool = next(
                    tool
                    for tool in KafkaMCPToolkit(session)
                    if tool.name == "kafka_list_topics"
                )
                return await tool._arun(filter="orders.*")

    assert asyncio.run(invoke()) == "orders.*"
