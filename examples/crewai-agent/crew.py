"""Example CrewAI agent crew using KafkaMCP for Kafka operations.

This demonstrates how to create a multi-agent crew that collaborates
on Kafka-related tasks using KafkaMCP tools.

Usage:
    pip install -r requirements.txt
    export OPENAI_API_KEY=your-key
    python crew.py
"""

import os
import sys

try:
    from crewai import Agent, Task, Crew
except ImportError:
    print("Missing dependency: pip install crewai")
    sys.exit(1)

try:
    from kafkamcp_crewai import StdioMCPClient, create_kafka_tools
except ImportError:
    StdioMCPClient = None
    create_kafka_tools = None


def main():
    """Run a CrewAI crew with KafkaMCP tools."""
    if not os.environ.get("OPENAI_API_KEY"):
        print("Error: OPENAI_API_KEY environment variable is required")
        sys.exit(1)

    if create_kafka_tools is None:
        print("Error: kafkamcp-crewai not installed.")
        print("Install with: pip install -r requirements.txt")
        sys.exit(1)

    config_path = os.environ.get("KAFKAMCP_CONFIG", "kafkamcp.yaml")
    with StdioMCPClient(args=["--config", config_path]) as mcp_client:
        kafka_tools = create_kafka_tools(mcp_client)

        monitor_agent = Agent(
            role="Kafka Monitor",
            goal="Monitor Kafka cluster health and detect anomalies",
            backstory="Expert in Kafka operations and monitoring",
            tools=kafka_tools,
        )

        analyst_agent = Agent(
            role="Data Analyst",
            goal="Analyze message patterns and generate insights",
            backstory="Data analysis expert specializing in streaming data",
            tools=kafka_tools,
        )

        remediation_agent = Agent(
            role="DLQ Remediation Specialist",
            goal="Analyze dead letter queue messages",
            backstory="Expert in error handling and message investigation",
            tools=kafka_tools,
        )

        monitor_task = Task(
            description="Check cluster health, list topics, and report any issues",
            expected_output="Cluster health report with any anomalies",
            agent=monitor_agent,
        )

        analysis_task = Task(
            description="Analyze the orders topic for recent failure patterns",
            expected_output="Analysis report with failure patterns and recommendations",
            agent=analyst_agent,
        )

        dlq_task = Task(
            description="Analyze recent messages in configured DLQ topics",
            expected_output="DLQ failure categories, samples, and recommended next steps",
            agent=remediation_agent,
        )

        crew = Crew(
            agents=[monitor_agent, analyst_agent, remediation_agent],
            tasks=[monitor_task, analysis_task, dlq_task],
            verbose=True,
        )

        print("KafkaMCP CrewAI Example")
        print("Starting crew execution...")
        result = crew.kickoff()
        print(f"\nCrew result:\n{result}")


if __name__ == "__main__":
    main()
