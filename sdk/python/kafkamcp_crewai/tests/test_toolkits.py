import json
import sys
import types
from pathlib import Path

import pytest
from mcp.types import CallToolResult, TextContent
from pydantic import BaseModel, ConfigDict


class StubBaseTool(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    name: str = ""
    description: str = ""
    mcp_client: object = None


crewai_module = types.ModuleType("crewai")
crewai_tools_module = types.ModuleType("crewai.tools")
crewai_tools_module.BaseTool = StubBaseTool
crewai_module.tools = crewai_tools_module
sys.modules.setdefault("crewai", crewai_module)
sys.modules.setdefault("crewai.tools", crewai_tools_module)

from kafkamcp_crewai import StdioMCPClient, create_guarded_tools, create_kafka_tools


class FakeClient:
    def call_tool(self, name, args):
        return {"name": name, "args": args}


class AsyncFakeClient:
    async def call_tool(self, name, args):
        return {"name": name, "args": args}


def test_default_tools_are_read_only():
    names = {tool.name for tool in create_kafka_tools(FakeClient())}
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


def test_guarded_tools_expose_approval_parameter():
    tools = {tool.name: tool for tool in create_guarded_tools(FakeClient())}
    assert set(tools) == {
        "kafka_produce",
        "kafka_create_topic",
        "kafka_delete_topic",
    }
    for tool in tools.values():
        assert "approval_token" in tool._run.__annotations__


CONTRACT_PATH = Path(__file__).resolve().parents[3] / "tool-contract.json"
SUPPORTED_TOOLS = {
    tool["name"]
    for tool in json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))["tools"]
}


def test_every_emitted_tool_is_supported():
    from kafkamcp_crewai import tools as _tools

    emitted = set()
    for factory_name in _tools.__dict__:
        factory = getattr(_tools, factory_name)
        if callable(factory) and factory_name.startswith("create_") and factory_name.endswith("_tools"):
            for tool in factory(FakeClient()):
                emitted.add(tool.name)
    assert emitted, "no tools emitted by any factory"
    unsupported = emitted - SUPPORTED_TOOLS
    assert not unsupported, f"SDK emits unsupported tools: {unsupported}"


def test_guarded_argument_contracts_match_server():
    client = FakeClient()
    tools = {tool.name: tool for tool in create_guarded_tools(client)}
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


def test_async_clients_are_rejected_with_adapter_guidance():
    tool = create_kafka_tools(AsyncFakeClient())[0]

    with pytest.raises(RuntimeError, match="StdioMCPClient"):
        tool._run(topic="orders")


def test_mcp_content_is_returned_as_agent_text():
    class ContentClient:
        def call_tool(self, name, args):
            return CallToolResult(
                content=[TextContent(type="text", text='{"messages":[]}')]
            )

    tool = create_kafka_tools(ContentClient())[0]

    assert tool._run(topic="orders") == '{"messages":[]}'


def test_stdio_client_requires_context_manager():
    client = StdioMCPClient()

    with pytest.raises(RuntimeError, match="context manager"):
        client.call_tool("kafka_list_topics", {})


def test_stdio_client_bridges_a_real_mcp_process():
    server_code = """
from mcp.server.fastmcp import FastMCP

server = FastMCP("adapter-test")

@server.tool()
def echo(value: str) -> str:
    return value

server.run(transport="stdio")
"""

    with StdioMCPClient(command=sys.executable, args=["-c", server_code]) as client:
        result = client.call_tool("echo", {"value": "ok"})

    assert result.content[0].text == "ok"
