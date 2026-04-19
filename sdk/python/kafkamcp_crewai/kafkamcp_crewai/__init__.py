"""KafkaMCP CrewAI integration — Kafka-aware AI agent crews."""

from kafkamcp_crewai.tools import (
    KafkaConsumeTool,
    KafkaProduceTool,
    KafkaListTopicsTool,
    KafkaDescribeTopicTool,
    KafkaCreateTopicTool,
    KafkaDeleteTopicTool,
    KafkaSearchTool,
    KafkaWatchTool,
    KafkaDescribeGroupTool,
    KafkaListGroupsTool,
    KafkaGetSchemaTool,
    KafkaDLQRemediateTool,
    KafkaAggregateTool,
    create_kafka_tools,
    create_monitoring_tools,
    create_remediation_tools,
)

__version__ = "0.1.0"
__all__ = [
    "KafkaConsumeTool",
    "KafkaProduceTool",
    "KafkaListTopicsTool",
    "KafkaDescribeTopicTool",
    "KafkaCreateTopicTool",
    "KafkaDeleteTopicTool",
    "KafkaSearchTool",
    "KafkaWatchTool",
    "KafkaDescribeGroupTool",
    "KafkaListGroupsTool",
    "KafkaGetSchemaTool",
    "KafkaDLQRemediateTool",
    "KafkaAggregateTool",
    "create_kafka_tools",
    "create_monitoring_tools",
    "create_remediation_tools",
]
