# LangChain Agent Example

Demonstrates building a LangChain AI agent that interacts with Kafka through KafkaMCP.

## Prerequisites

- Python 3.10+
- `kafkamcp` binary installed and on `PATH`
- A running Kafka broker (e.g., `make docker-dev` from the repo root)
- An OpenAI API key

## Setup

```bash
# Install Python dependencies
pip install -r requirements.txt

# Set your OpenAI API key
export OPENAI_API_KEY=your-key

# Optional: point to a custom config
export KAFKAMCP_CONFIG=/path/to/kafkamcp.yaml
```

## Usage

```bash
python agent.py
```

## Expected Output

```
Connecting to KafkaMCP server...
Connected to KafkaMCP

Available tools (155):
  - kafka_list_topics: List all topics in the Kafka cluster...
  - kafka_consume: Consume messages from a topic...
  ...

Calling list_topics directly via MCP...
Topics: [...]

Done.
```

If `kafkamcp-langchain` is installed, the agent will use GPT-4 to answer natural language queries about your Kafka cluster.

## How It Works

1. Connects to KafkaMCP over stdio using the MCP Python client
2. Discovers available tools via `session.list_tools()`
3. Either runs a full LangChain agent (if `kafkamcp-langchain` is installed) or calls tools directly via MCP
