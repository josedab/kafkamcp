"""Example LangChain agent using KafkaMCP for Kafka operations.

This demonstrates how to create an AI agent that can interact with Kafka
using KafkaMCP as the bridge between the LLM and Kafka.

Usage:
    pip install kafkamcp-langchain langchain-openai
    export OPENAI_API_KEY=your-key
    python agent.py
"""

import asyncio

from kafkamcp_langchain import KafkaMCPToolkit


async def main():
    """Run a LangChain agent with KafkaMCP tools.

    This example shows the pattern for connecting to KafkaMCP
    and using it with LangChain agents.

    In production, you would:
    1. Connect to KafkaMCP via MCP client
    2. Create tools from the toolkit
    3. Use with any LangChain agent type
    """
    # Example: Connect to KafkaMCP server
    # from mcp import ClientSession, StdioServerParameters
    # from mcp.client.stdio import stdio_client
    #
    # server_params = StdioServerParameters(
    #     command="kafkamcp",
    #     args=["--config", "kafkamcp.yaml"]
    # )
    #
    # async with stdio_client(server_params) as (read, write):
    #     async with ClientSession(read, write) as session:
    #         await session.initialize()
    #
    #         tools = KafkaMCPToolkit(mcp_client=session)
    #
    #         from langchain_openai import ChatOpenAI
    #         from langchain.agents import initialize_agent, AgentType
    #
    #         llm = ChatOpenAI(model="gpt-4")
    #         agent = initialize_agent(tools, llm, agent=AgentType.OPENAI_FUNCTIONS)
    #
    #         # Example queries the agent can handle:
    #         result = agent.run("What topics are available in the cluster?")
    #         result = agent.run("Show me the last 5 messages from the orders topic")
    #         result = agent.run("Search for failed payment messages in the last hour")
    #         result = agent.run("Watch for new error messages on any DLQ topic")

    print("KafkaMCP LangChain agent example")
    print("See comments in this file for full implementation")
    print()
    print("Available toolkits:")
    print("  KafkaMCPToolkit     - All Kafka tools")
    print("  KafkaConsumeToolkit - Consume + Search")
    print("  KafkaProduceToolkit - Produce messages")
    print("  KafkaTopicToolkit   - Topic management")
    print("  KafkaSchemaToolkit  - Schema Registry")


if __name__ == "__main__":
    asyncio.run(main())
