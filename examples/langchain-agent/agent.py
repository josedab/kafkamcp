"""Example LangChain agent using KafkaMCP for Kafka operations.

This demonstrates how to create an AI agent that can interact with Kafka
using KafkaMCP as the bridge between the LLM and Kafka.

Usage:
    pip install -r requirements.txt
    export OPENAI_API_KEY=your-key
    python agent.py
"""

import asyncio
import os
import sys

try:
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
except ImportError:
    print("Missing dependency: pip install mcp")
    sys.exit(1)

try:
    from langchain_openai import ChatOpenAI
    from langchain.agents import initialize_agent, AgentType
except ImportError:
    print("Missing dependency: pip install langchain langchain-openai")
    sys.exit(1)

try:
    from kafkamcp_langchain import KafkaMCPToolkit
except ImportError:
    KafkaMCPToolkit = None


async def main():
    """Run a LangChain agent with KafkaMCP tools."""
    config_path = os.environ.get("KAFKAMCP_CONFIG", "kafkamcp.yaml")

    if not os.environ.get("OPENAI_API_KEY"):
        print("Error: OPENAI_API_KEY environment variable is required")
        sys.exit(1)

    server_params = StdioServerParameters(
        command="kafkamcp",
        args=["--config", config_path],
    )

    print("Connecting to KafkaMCP server...")
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            print("Connected to KafkaMCP")

            # List available tools
            tools_result = await session.list_tools()
            print(f"\nAvailable tools ({len(tools_result.tools)}):")
            for tool in tools_result.tools:
                print(f"  - {tool.name}: {tool.description[:60]}...")

            # If the LangChain toolkit is installed, run a full agent
            if KafkaMCPToolkit is not None:
                tools = KafkaMCPToolkit(mcp_client=session)
                llm = ChatOpenAI(model="gpt-4")
                agent = initialize_agent(
                    tools.get_tools(),
                    llm,
                    agent=AgentType.OPENAI_FUNCTIONS,
                    verbose=True,
                )

                query = "What topics are available in the cluster?"
                print(f"\nAgent query: {query}")
                result = agent.run(query)
                print(f"Result: {result}")
            else:
                # Direct MCP tool call without LangChain toolkit
                print("\nCalling list_topics directly via MCP...")
                result = await session.call_tool("list_topics", {"filter": ""})
                print(f"Topics: {result}")

    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
