package kafkamcpsdk

import (
	"encoding/json"
	"os"
	"regexp"
	"testing"
)

type toolContract struct {
	Tools []struct {
		Name string `json:"name"`
	} `json:"tools"`
}

// TestSDKOnlyCallsSupportedTools enumerates every tool name the SDK emits via
// CallTool and asserts it belongs to the supported 29-tool allow-list. This
// prevents the SDK from re-introducing phantom tool names.
func TestSDKOnlyCallsSupportedTools(t *testing.T) {
	contractData, err := os.ReadFile("../../tool-contract.json")
	if err != nil {
		t.Fatalf("reading tool contract: %v", err)
	}
	var contract toolContract
	if err := json.Unmarshal(contractData, &contract); err != nil {
		t.Fatalf("parsing tool contract: %v", err)
	}
	supportedTools := make(map[string]bool, len(contract.Tools))
	for _, tool := range contract.Tools {
		supportedTools[tool.Name] = true
	}

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
