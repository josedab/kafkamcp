package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleStateMachine(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_state_machine", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		if action == "" {
			return mcp.NewToolResultError("action is required"), nil
		}
		id := request.GetString("id", "")

		switch action {
		case "list_machines":
			machines := s.stateMachineEngine.ListMachines()
			return marshalResult(map[string]any{"machines": machines, "total": len(machines)})

		case "get_machine":
			if id == "" {
				return mcp.NewToolResultError("id is required"), nil
			}
			m, err := s.stateMachineEngine.GetMachine(id)
			if err != nil {
				return s.toolError("Machine not found", err), nil
			}
			return marshalResult(m)

		case "create_machine":
			def := request.GetString("definition", "")
			if def == "" {
				return mcp.NewToolResultError("definition (YAML) is required"), nil
			}
			m, err := s.stateMachineEngine.CreateMachine(def, agentID)
			if err != nil {
				return s.toolError("Failed to create machine", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_state_machine", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "created", "machine": m})

		case "create_instance":
			if id == "" {
				return mcp.NewToolResultError("id (machine ID) is required"), nil
			}
			data := extractAnyMapArg(request, "data")
			inst, err := s.stateMachineEngine.CreateInstance(id, data)
			if err != nil {
				return s.toolError("Failed to create instance", err), nil
			}
			return marshalResult(map[string]any{"status": "created", "instance": inst})

		case "get_instance":
			if id == "" {
				return mcp.NewToolResultError("id is required"), nil
			}
			inst, err := s.stateMachineEngine.GetInstance(id)
			if err != nil {
				return s.toolError("Instance not found", err), nil
			}
			return marshalResult(inst)

		case "list_instances":
			insts := s.stateMachineEngine.ListInstances(id)
			return marshalResult(map[string]any{"instances": insts, "total": len(insts)})

		case "transition":
			if id == "" {
				return mcp.NewToolResultError("id (instance ID) is required"), nil
			}
			event := request.GetString("event", "")
			if event == "" {
				return mcp.NewToolResultError("event is required"), nil
			}
			inst, err := s.stateMachineEngine.Transition(id, event, agentID)
			if err != nil {
				return s.toolError("Transition failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_state_machine", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "transitioned", "instance": inst})

		case "valid_transitions":
			if id == "" {
				return mcp.NewToolResultError("id (instance ID) is required"), nil
			}
			transitions, err := s.stateMachineEngine.ValidTransitions(id)
			if err != nil {
				return s.toolError("Failed to get transitions", err), nil
			}
			return marshalResult(map[string]any{"transitions": transitions, "total": len(transitions)})

		case "start_saga":
			if id == "" {
				return mcp.NewToolResultError("id (instance ID) is required"), nil
			}
			inst, err := s.stateMachineEngine.StartSaga(id)
			if err != nil {
				return s.toolError("Failed to start saga", err), nil
			}
			return marshalResult(map[string]any{"status": "saga_started", "instance": inst})

		case "complete_saga_step":
			if id == "" {
				return mcp.NewToolResultError("id (instance ID) is required"), nil
			}
			success := request.GetBool("success", true)
			errMsg := request.GetString("error_message", "")
			inst, err := s.stateMachineEngine.CompleteSagaStep(id, success, errMsg)
			if err != nil {
				return s.toolError("Failed to complete saga step", err), nil
			}
			return marshalResult(map[string]any{"status": inst.SagaState.Status, "instance": inst})

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action: %s", action)), nil
		}
	})(ctx, request)
}
