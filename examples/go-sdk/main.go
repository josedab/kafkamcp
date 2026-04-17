// Example Go SDK agent that interacts with KafkaMCP.
//
// This demonstrates how to use the KafkaMCP Go SDK for
// programmatic Kafka operations from Go applications.
package main

import (
	"fmt"
)

func main() {
	fmt.Println("KafkaMCP Go SDK Example")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  import \"github.com/kafkamcp/kafkamcp/sdk/go/kafkamcpsdk\"")
	fmt.Println()
	fmt.Println("  // Create client with transport")
	fmt.Println("  client := kafkamcpsdk.New(transport,")
	fmt.Println("    kafkamcpsdk.WithAgentID(\"my-agent\"),")
	fmt.Println("    kafkamcpsdk.WithTimeout(30 * time.Second),")
	fmt.Println("  )")
	fmt.Println()
	fmt.Println("  // List topics")
	fmt.Println("  topics, _ := client.ListTopics(ctx, \"orders.*\")")
	fmt.Println()
	fmt.Println("  // Consume messages")
	fmt.Println("  msgs, _ := client.Consume(ctx, \"orders\", 10, \"latest\")")
	fmt.Println()
	fmt.Println("  // Watch for events")
	fmt.Println("  subID, _ := client.Watch(ctx, \"*-dlq\", \"\", \"https://webhook.url\")")
}
