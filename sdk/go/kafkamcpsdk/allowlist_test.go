package kafkamcpsdk

import (
	"os"
	"regexp"
	"testing"
)

// supportedTools is the exact set of 29 tools KafkaMCP exposes across the
// core-read, ops-write and governance packs. The SDK must never call a tool
// outside this allow-list. Keep in sync with internal/server/tool_packs.go.
var supportedTools = map[string]bool{
	// core-read (stable)
	"kafka_list_topics": true, "kafka_describe_topic": true, "kafka_list_schemas": true,
	"kafka_get_schema": true, "kafka_check_schema_compatibility": true, "kafka_consume": true,
	"kafka_search": true, "kafka_list_consumer_groups": true, "kafka_describe_consumer_group": true,
	"kafka_diagnose_lag": true, "kafka_analyze_dlq": true, "kafka_health": true,
	"kafka_metrics": true, "kafka_discover_clusters": true, "kafka_discover_tools": true,
	// ops-write + governance (beta)
	"kafka_produce": true, "kafka_create_topic": true, "kafka_delete_topic": true,
	"kafka_alter_topic": true, "kafka_reset_offsets": true, "kafka_acl_manage": true,
	"kafka_create_connector": true, "kafka_update_connector": true, "kafka_delete_connector": true,
	"kafka_restart_connector": true, "kafka_approval": true, "kafka_governance_decision": true,
	"kafka_policy_evaluate": true, "kafka_policy_simulate": true,
}

// TestSDKOnlyCallsSupportedTools enumerates every tool name the SDK emits via
// CallTool and asserts it belongs to the supported 29-tool allow-list. This
// prevents the SDK from re-introducing phantom tool names.
func TestSDKOnlyCallsSupportedTools(t *testing.T) {
	src, err := os.ReadFile("client.go")
	if err != nil {
		t.Fatalf("reading client.go: %v", err)
	}
	re := regexp.MustCompile(`CallTool\(ctx, "([a-z_]+)"`)
	matches := re.FindAllStringSubmatch(string(src), -1)
	if len(matches) == 0 {
		t.Fatal("no CallTool invocations found in client.go")
	}
	for _, m := range matches {
		name := m[1]
		if !supportedTools[name] {
			t.Errorf("SDK calls unsupported tool %q (not in the 29-tool allow-list)", name)
		}
	}
}
