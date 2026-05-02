# KafkaMCP Demo Recording
# Install: https://github.com/charmbracelet/vhs
# Run: vhs demo.tape

Output demo.gif

Set FontSize 14
Set Width 1200
Set Height 600
Set Theme "Dracula"
Set TypingSpeed 40ms

Type "# KafkaMCP — AI Agent ↔ Kafka in seconds"
Enter
Sleep 1s

Type "# Start the MCP server"
Enter
Type "kafkamcp --config kafkamcp.yaml &"
Enter
Sleep 2s

Type "# List available Kafka topics"
Enter
Type `echo '{"method":"tools/call","params":{"name":"kafka_list_topics","arguments":{}}}' | kafkamcp`
Enter
Sleep 2s

Type "# Produce a message to a topic"
Enter
Type `echo '{"method":"tools/call","params":{"name":"kafka_produce","arguments":{"topic":"orders","value":"{\"order_id\":\"123\",\"amount\":99.95}"}}}' | kafkamcp`
Enter
Sleep 2s

Type "# Consume messages from a topic"
Enter
Type `echo '{"method":"tools/call","params":{"name":"kafka_consume","arguments":{"topic":"orders","count":5}}}' | kafkamcp`
Enter
Sleep 2s

Type "# Describe a consumer group"
Enter
Type `echo '{"method":"tools/call","params":{"name":"kafka_describe_group","arguments":{"group_id":"my-app"}}}' | kafkamcp`
Enter
Sleep 3s
