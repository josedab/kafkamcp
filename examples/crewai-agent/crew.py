"""Example CrewAI agent crew using KafkaMCP for Kafka operations.

This demonstrates how to create a multi-agent crew that collaborates
on Kafka-related tasks using KafkaMCP tools.

Usage:
    pip install kafkamcp-crewai crewai langchain-openai
    export OPENAI_API_KEY=your-key
    python crew.py
"""

from kafkamcp_crewai import create_kafka_tools


def main():
    """Run a CrewAI crew with KafkaMCP tools.

    This example shows the pattern for creating a multi-agent crew
    where each agent has specific Kafka responsibilities.

    In production, you would:
    1. Connect to KafkaMCP via MCP client
    2. Create tools from create_kafka_tools()
    3. Assign tools to specialized agents in a crew
    """
    # Example: Connect to KafkaMCP and create a monitoring crew
    #
    # from crewai import Agent, Task, Crew
    #
    # mcp_client = ...  # Connect to KafkaMCP
    # kafka_tools = create_kafka_tools(mcp_client)
    #
    # # Specialized agents
    # monitor_agent = Agent(
    #     role="Kafka Monitor",
    #     goal="Monitor Kafka cluster health and detect anomalies",
    #     backstory="Expert in Kafka operations and monitoring",
    #     tools=kafka_tools,
    # )
    #
    # analyst_agent = Agent(
    #     role="Data Analyst",
    #     goal="Analyze message patterns and generate insights",
    #     backstory="Data analysis expert specializing in streaming data",
    #     tools=kafka_tools,
    # )
    #
    # remediation_agent = Agent(
    #     role="DLQ Remediation Specialist",
    #     goal="Analyze and remediate dead letter queue messages",
    #     backstory="Expert in error handling and message recovery",
    #     tools=kafka_tools,
    # )
    #
    # # Tasks
    # monitor_task = Task(
    #     description="Check cluster health, list topics, and report any issues",
    #     expected_output="Cluster health report with any anomalies",
    #     agent=monitor_agent,
    # )
    #
    # analysis_task = Task(
    #     description="Analyze the orders topic for recent failure patterns",
    #     expected_output="Analysis report with failure patterns and recommendations",
    #     agent=analyst_agent,
    # )
    #
    # crew = Crew(agents=[monitor_agent, analyst_agent], tasks=[monitor_task, analysis_task])
    # result = crew.kickoff()

    print("KafkaMCP CrewAI example")
    print("See comments in this file for full crew implementation")
    print()
    print("Example crew roles:")
    print("  Kafka Monitor         - Cluster health monitoring")
    print("  Data Analyst          - Message pattern analysis")
    print("  DLQ Remediation       - Dead letter queue management")


if __name__ == "__main__":
    main()
