package server

import (
	"context"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerCollaborationTools() {
	if s.collaborationCoord == nil {
		return
	}

	s.addTool(mcp.NewTool("agent_lock_acquire",
		withCategory("ops"),
		mcp.WithDescription("Acquire a distributed lock on a shared resource (topic, consumer group, schema) to prevent concurrent modifications by multiple agents."),
		mcp.WithString("resource", mcp.Required(), mcp.Description("Resource to lock (e.g., 'topic:orders', 'group:my-group')")),
		mcp.WithNumber("ttl_seconds", mcp.Description("Lock TTL in seconds. Default: 30, Max: 3600")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "agent_lock_acquire",
		resourceType: "cluster",
		action:       "write",
	}, s.handleLockAcquire))

	s.addTool(mcp.NewTool("agent_lock_release",
		withCategory("ops"),
		mcp.WithDescription("Release a previously acquired distributed lock on a resource."),
		mcp.WithString("resource", mcp.Required(), mcp.Description("Resource to unlock")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "agent_lock_release",
		resourceType: "cluster",
		action:       "write",
	}, s.handleLockRelease))

	s.addTool(mcp.NewTool("agent_lock_list",
		withCategory("ops"),
		mcp.WithDescription("List all active distributed locks showing resource, owner, and expiration."),
	), s.withToolAuth(toolAuthConfig{
		tool:         "agent_lock_list",
		resourceType: "cluster",
		action:       "read",
	}, s.handleLockList))

	s.addTool(mcp.NewTool("agent_clock_get",
		withCategory("ops"),
		mcp.WithDescription("Get the vector clock for a resource to check causal ordering of operations across agents."),
		mcp.WithString("resource", mcp.Required(), mcp.Description("Resource to get clock for")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "agent_clock_get",
		resourceType: "cluster",
		action:       "read",
	}, s.handleClockGet))

	s.addTool(mcp.NewTool("agent_clock_increment",
		withCategory("ops"),
		mcp.WithDescription("Increment the vector clock for a resource, recording an operation by this agent."),
		mcp.WithString("resource", mcp.Required(), mcp.Description("Resource to increment clock for")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "agent_clock_increment",
		resourceType: "cluster",
		action:       "write",
	}, s.handleClockIncrement))

	s.addTool(mcp.NewTool("agent_message_send",
		withCategory("ops"),
		mcp.WithDescription("Send a message to another agent for coordination. Use '*' as recipient for broadcasts."),
		mcp.WithString("to", mcp.Required(), mcp.Description("Recipient agent ID or '*' for broadcast")),
		mcp.WithString("type", mcp.Description("Message type: request, response, broadcast, notification. Default: notification")),
		mcp.WithString("subject", mcp.Required(), mcp.Description("Message subject")),
		mcp.WithString("body", mcp.Description("Message body")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "agent_message_send",
		resourceType: "cluster",
		action:       "write",
	}, s.handleMessageSend))

	s.addTool(mcp.NewTool("agent_message_inbox",
		withCategory("ops"),
		mcp.WithDescription("Read messages sent to this agent. Supports filtering to unread-only."),
		mcp.WithBoolean("unread_only", mcp.Description("Only return unread messages. Default: false")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "agent_message_inbox",
		resourceType: "cluster",
		action:       "read",
	}, s.handleMessageInbox))

	s.addTool(mcp.NewTool("agent_coordinate_register",
		withCategory("ops"),
		mcp.WithDescription("Register this agent's capabilities and status for coordination with other agents."),
		mcp.WithString("status", mcp.Description("Agent status: active, idle, busy. Default: active")),
		mcp.WithObject("capabilities", mcp.Description("List of agent capabilities")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "agent_coordinate_register",
		resourceType: "cluster",
		action:       "write",
	}, s.handleAgentRegister))

	s.addTool(mcp.NewTool("agent_coordinate_list",
		withCategory("ops"),
		mcp.WithDescription("List all registered agents with their status, capabilities, and last seen time."),
	), s.withToolAuth(toolAuthConfig{
		tool:         "agent_coordinate_list",
		resourceType: "cluster",
		action:       "read",
	}, s.handleAgentList))

	// Lease-based topic ownership tools
	s.addTool(mcp.NewTool("agent_topic_lease_acquire",
		withCategory("ops"),
		mcp.WithDescription("Acquire a lease on a Kafka topic for exclusive or shared ownership. Prevents concurrent conflicting operations by multiple agents."),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name to lease")),
		mcp.WithString("lease_type", mcp.Description("Lease type: exclusive (default) or shared")),
		mcp.WithNumber("ttl_seconds", mcp.Description("Lease TTL in seconds. Default: 60, Max: 3600")),
		mcp.WithNumber("max_renewals", mcp.Description("Max lease renewals. Default: 10")),
		mcp.WithObject("permissions", mcp.Description("Permissions: read, write, admin")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "agent_topic_lease_acquire",
		resourceType: "topic",
		action:       "write",
		resourceName: func(r mcp.CallToolRequest) string {
			if t, ok := r.GetArguments()["topic"].(string); ok {
				return t
			}
			return "*"
		},
	}, s.handleTopicLeaseAcquire))

	s.addTool(mcp.NewTool("agent_topic_lease_release",
		withCategory("ops"),
		mcp.WithDescription("Release a previously acquired topic lease."),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name to release lease on")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "agent_topic_lease_release",
		resourceType: "topic",
		action:       "write",
	}, s.handleTopicLeaseRelease))

	s.addTool(mcp.NewTool("agent_topic_lease_renew",
		withCategory("ops"),
		mcp.WithDescription("Renew an existing topic lease to extend its TTL."),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name to renew lease for")),
		mcp.WithNumber("ttl_seconds", mcp.Description("New TTL in seconds. Default: 60")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "agent_topic_lease_renew",
		resourceType: "topic",
		action:       "write",
	}, s.handleTopicLeaseRenew))

	s.addTool(mcp.NewTool("agent_topic_lease_list",
		withCategory("ops"),
		mcp.WithDescription("List all active topic leases showing owner, type, permissions, and expiration."),
	), s.withToolAuth(toolAuthConfig{
		tool:         "agent_topic_lease_list",
		resourceType: "cluster",
		action:       "read",
	}, s.handleTopicLeaseList))
}

func (s *Server) handleLockAcquire(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	resource, _ := args["resource"].(string)
	if resource == "" {
		return mcp.NewToolResultError("resource is required"), nil
	}

	ttl := 30
	if t, ok := args["ttl_seconds"].(float64); ok && t > 0 {
		ttl = int(t)
	}

	result := s.collaborationCoord.AcquireLock(agentID, resource, ttl, nil)
	s.logAudit(ctx, agentID, "agent_lock_acquire", args, "", boolStatus(result.Acquired), 1, nil, start)
	return marshalResult(result)
}

func (s *Server) handleLockRelease(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	resource, _ := args["resource"].(string)
	if resource == "" {
		return mcp.NewToolResultError("resource is required"), nil
	}

	err := s.collaborationCoord.ReleaseLock(agentID, resource)
	if err != nil {
		s.logAudit(ctx, agentID, "agent_lock_release", args, "", "error", 0, err, start)
		return s.toolError("failed to release lock", err), nil
	}

	s.logAudit(ctx, agentID, "agent_lock_release", args, "", "success", 1, nil, start)
	return marshalResult(map[string]string{"status": "released", "resource": resource})
}

func (s *Server) handleLockList(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	locks := s.collaborationCoord.ListLocks()
	s.logAudit(ctx, agentID, "agent_lock_list", args, "", "success", len(locks), nil, start)
	return marshalResult(map[string]any{"locks": locks, "total": len(locks)})
}

func (s *Server) handleClockGet(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	resource, _ := args["resource"].(string)
	if resource == "" {
		return mcp.NewToolResultError("resource is required"), nil
	}

	clock := s.collaborationCoord.GetClock(resource)
	s.logAudit(ctx, agentID, "agent_clock_get", args, "", "success", 1, nil, start)
	return marshalResult(clock)
}

func (s *Server) handleClockIncrement(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	resource, _ := args["resource"].(string)
	if resource == "" {
		return mcp.NewToolResultError("resource is required"), nil
	}

	clock := s.collaborationCoord.IncrementClock(agentID, resource)
	s.logAudit(ctx, agentID, "agent_clock_increment", args, "", "success", 1, nil, start)
	return marshalResult(clock)
}

func (s *Server) handleMessageSend(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	to, _ := args["to"].(string)
	subject, _ := args["subject"].(string)
	body, _ := args["body"].(string)
	msgType, _ := args["type"].(string)

	if to == "" || subject == "" {
		return mcp.NewToolResultError("to and subject are required"), nil
	}
	if msgType == "" {
		msgType = "notification"
	}

	msg, err := s.collaborationCoord.SendMessage(agentID, to, msgType, subject, body)
	if err != nil {
		s.logAudit(ctx, agentID, "agent_message_send", args, "", "error", 0, err, start)
		return s.toolError("failed to send message", err), nil
	}

	s.logAudit(ctx, agentID, "agent_message_send", args, "", "success", 1, nil, start)
	return marshalResult(msg)
}

func (s *Server) handleMessageInbox(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	unreadOnly, _ := args["unread_only"].(bool)

	messages := s.collaborationCoord.GetMessages(agentID, unreadOnly)
	s.logAudit(ctx, agentID, "agent_message_inbox", args, "", "success", len(messages), nil, start)
	return marshalResult(map[string]any{"messages": messages, "total": len(messages)})
}

func (s *Server) handleAgentRegister(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	status, _ := args["status"].(string)
	if status == "" {
		status = "active"
	}

	var capabilities []string
	if caps, ok := args["capabilities"].([]any); ok {
		for _, c := range caps {
			if s, ok := c.(string); ok {
				capabilities = append(capabilities, s)
			}
		}
	}

	state := s.collaborationCoord.RegisterAgent(agentID, status, capabilities, nil)
	s.logAudit(ctx, agentID, "agent_coordinate_register", args, "", "success", 1, nil, start)
	return marshalResult(state)
}

func (s *Server) handleAgentList(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	agents := s.collaborationCoord.ListAgents()
	s.logAudit(ctx, agentID, "agent_coordinate_list", args, "", "success", len(agents), nil, start)
	return marshalResult(map[string]any{"agents": agents, "total": len(agents)})
}

func (s *Server) handleTopicLeaseAcquire(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	topic, _ := args["topic"].(string)
	if topic == "" {
		return mcp.NewToolResultError("topic is required"), nil
	}

	leaseType, _ := args["lease_type"].(string)
	ttl := 60
	if t, ok := args["ttl_seconds"].(float64); ok && t > 0 {
		ttl = int(t)
	}
	maxRenewals := 10
	if m, ok := args["max_renewals"].(float64); ok && m > 0 {
		maxRenewals = int(m)
	}

	var permissions []string
	if perms, ok := args["permissions"].([]any); ok {
		for _, p := range perms {
			if ps, ok := p.(string); ok {
				permissions = append(permissions, ps)
			}
		}
	}

	result := s.collaborationCoord.AcquireTopicLease(agentID, topic, leaseType, ttl, maxRenewals, permissions, nil)
	s.logAudit(ctx, agentID, "agent_topic_lease_acquire", args, "", boolStatus(result.Acquired), 1, nil, start)
	return marshalResult(result)
}

func (s *Server) handleTopicLeaseRelease(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	topic, _ := args["topic"].(string)
	if topic == "" {
		return mcp.NewToolResultError("topic is required"), nil
	}

	err := s.collaborationCoord.ReleaseTopicLease(agentID, topic)
	if err != nil {
		s.logAudit(ctx, agentID, "agent_topic_lease_release", args, "", "error", 0, err, start)
		return s.toolError("failed to release topic lease", err), nil
	}

	s.logAudit(ctx, agentID, "agent_topic_lease_release", args, "", "success", 1, nil, start)
	return marshalResult(map[string]string{"status": "released", "topic": topic})
}

func (s *Server) handleTopicLeaseRenew(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	topic, _ := args["topic"].(string)
	if topic == "" {
		return mcp.NewToolResultError("topic is required"), nil
	}

	ttl := 60
	if t, ok := args["ttl_seconds"].(float64); ok && t > 0 {
		ttl = int(t)
	}

	lease, err := s.collaborationCoord.RenewTopicLease(agentID, topic, ttl)
	if err != nil {
		s.logAudit(ctx, agentID, "agent_topic_lease_renew", args, "", "error", 0, err, start)
		return s.toolError("failed to renew topic lease", err), nil
	}

	s.logAudit(ctx, agentID, "agent_topic_lease_renew", args, "", "success", 1, nil, start)
	return marshalResult(lease)
}

func (s *Server) handleTopicLeaseList(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	leases := s.collaborationCoord.ListTopicLeases()
	s.logAudit(ctx, agentID, "agent_topic_lease_list", args, "", "success", len(leases), nil, start)
	return marshalResult(map[string]any{"leases": leases, "total": len(leases)})
}

func boolStatus(b bool) string {
	if b {
		return "success"
	}
	return "denied"
}
