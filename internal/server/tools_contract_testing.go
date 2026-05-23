package server

import (
	"context"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/contracts"
)

func (s *Server) handleContractTest(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_contract_test", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")

		switch action {
		case "register_pair":
			topic := request.GetString("topic", "")
			producerID := request.GetString("producer_id", "")
			if topic == "" || producerID == "" {
				return mcp.NewToolResultError("topic and producer_id are required"), nil
			}
			consumerIDs := extractStrings(args, "consumer_ids")
			pair := s.contractTester.RegisterPair(topic, producerID, consumerIDs)
			s.logAudit(ctx, agentID, "kafka_contract_test", args, "", "success", 1, nil, start)
			return marshalResult(pair)

		case "register_schema":
			topic := request.GetString("topic", "")
			serviceID := request.GetString("service_id", "")
			role := request.GetString("role", "")
			fields := extractStrings(args, "fields")
			if topic == "" || serviceID == "" || role == "" {
				return mcp.NewToolResultError("topic, service_id, and role are required"), nil
			}
			if role == "producer" {
				s.contractTester.RegisterProducerSchema(topic, serviceID, fields)
			} else {
				s.contractTester.RegisterConsumerSchema(topic, serviceID, fields)
			}
			s.logAudit(ctx, agentID, "kafka_contract_test", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"registered": serviceID, "topic": topic, "role": role, "fields": len(fields)})

		case "test_compatibility":
			topic := request.GetString("topic", "")
			if topic == "" {
				return mcp.NewToolResultError("topic is required"), nil
			}
			result := s.contractTester.TestCompatibility(topic)
			s.logAudit(ctx, agentID, "kafka_contract_test", args, "", "success", 1, nil, start)
			return marshalResult(result)

		case "create_suite":
			suiteName := request.GetString("suite_name", "")
			topic := request.GetString("topic", "")
			if suiteName == "" || topic == "" {
				return mcp.NewToolResultError("suite_name and topic are required"), nil
			}
			rawCases, _ := args["test_cases"].([]any)
			var testCases []contracts.ContractTestCase
			for _, raw := range rawCases {
				m, ok := raw.(map[string]any)
				if !ok {
					continue
				}
				tc := contracts.ContractTestCase{
					Name:        stringFromMap(m, "name"),
					Description: stringFromMap(m, "description"),
					ExpectValid: boolFromMap(m, "expect_valid"),
				}
				if input, ok := m["input"].(map[string]any); ok {
					tc.Input = input
				}
				testCases = append(testCases, tc)
			}
			suite := s.contractTester.CreateTestSuite(suiteName, topic, testCases)
			s.logAudit(ctx, agentID, "kafka_contract_test", args, "", "success", 1, nil, start)
			return marshalResult(suite)

		case "run_suite":
			suiteID := request.GetString("suite_id", "")
			if suiteID == "" {
				return mcp.NewToolResultError("suite_id is required"), nil
			}
			result, err := s.contractTester.RunTestSuite(suiteID, s.contractEngine)
			if err != nil {
				return s.toolError("failed to run test suite", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_contract_test", args, "", "success", result.Total, nil, start)
			return marshalResult(result)

		case "detect_changes":
			oldFields := extractStrings(args, "old_fields")
			newFields := extractStrings(args, "new_fields")
			issues := contracts.DetectBreakingChanges(oldFields, newFields)
			s.logAudit(ctx, agentID, "kafka_contract_test", args, "", "success", len(issues), nil, start)
			return marshalResult(map[string]any{"issues": issues, "breaking": len(issues) > 0, "total": len(issues)})

		case "list_pairs":
			pairs := s.contractTester.ListPairs()
			s.logAudit(ctx, agentID, "kafka_contract_test", args, "", "success", len(pairs), nil, start)
			return marshalResult(map[string]any{"pairs": pairs, "total": len(pairs)})

		case "list_suites":
			suites := s.contractTester.ListTestSuites()
			s.logAudit(ctx, agentID, "kafka_contract_test", args, "", "success", len(suites), nil, start)
			return marshalResult(map[string]any{"suites": suites, "total": len(suites)})

		default:
			return mcp.NewToolResultError("action must be: register_pair, register_schema, test_compatibility, create_suite, run_suite, detect_changes, list_pairs, list_suites"), nil
		}
	})(ctx, request)
}

func boolFromMap(m map[string]any, key string) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}
	return false
}

func extractStrings(args map[string]any, key string) []string {
	raw, ok := args[key].([]any)
	if !ok {
		return nil
	}
	var result []string
	for _, v := range raw {
		if s, ok := v.(string); ok {
			result = append(result, s)
		}
	}
	return result
}
