"""KafkaMCP CrewAI integration — Kafka-aware AI agent crews."""

from kafkamcp_crewai.client import StdioMCPClient
from kafkamcp_crewai.tools import (
    KafkaConsumeTool,
    KafkaProduceTool,
    KafkaListTopicsTool,
    KafkaDescribeTopicTool,
    KafkaCreateTopicTool,
    KafkaDeleteTopicTool,
    KafkaSearchTool,
    KafkaDescribeGroupTool,
    KafkaListGroupsTool,
    KafkaGetSchemaTool,
    KafkaAnalyzeDLQTool,
    create_kafka_tools,
    create_guarded_tools,
    create_monitoring_tools,
    create_remediation_tools,
)

__version__ = "0.1.0"
__all__ = [
    "StdioMCPClient",
    "KafkaConsumeTool",
    "KafkaProduceTool",
    "KafkaListTopicsTool",
    "KafkaDescribeTopicTool",
    "KafkaCreateTopicTool",
    "KafkaDeleteTopicTool",
    "KafkaSearchTool",
    "KafkaDescribeGroupTool",
    "KafkaListGroupsTool",
    "KafkaGetSchemaTool",
    "KafkaAnalyzeDLQTool",
    "create_kafka_tools",
    "create_guarded_tools",
    "create_monitoring_tools",
    "create_remediation_tools",
]
