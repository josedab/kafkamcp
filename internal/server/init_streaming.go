package server

import (
	"log/slog"

	"github.com/kafkamcp/kafkamcp/internal/dlq"
	"github.com/kafkamcp/kafkamcp/internal/flowdebug"
	"github.com/kafkamcp/kafkamcp/internal/lineage"
	"github.com/kafkamcp/kafkamcp/internal/replication"
	"github.com/kafkamcp/kafkamcp/internal/selfheal"
	"github.com/kafkamcp/kafkamcp/internal/watch"
)

// Streaming groups event-driven and stream-processing subsystems.
type Streaming struct {
	watchManager   *watch.Manager         // event-driven watch subscriptions
	dlqRemediator  *dlq.Remediator        // DLQ auto-remediation
	replicationMon *replication.Monitor    // cross-cluster replication monitoring
	selfHealMon    *selfheal.Monitor      // self-healing consumer groups
	lineageTracker *lineage.Tracker       // topic data lineage
	messageTracer  *lineage.MessageTracer // distributed message tracing
	flowDebugger   *flowdebug.Debugger    // live message flow debugger
}

// newStreaming initializes all streaming subsystems.
func newStreaming(logger *slog.Logger) Streaming {
	return Streaming{
		watchManager:   watch.NewManager(logger),
		dlqRemediator:  dlq.NewRemediator(logger),
		replicationMon: replication.NewMonitor(logger),
		selfHealMon:    selfheal.NewMonitor(),
		lineageTracker: lineage.NewTracker(),
		messageTracer:  lineage.NewMessageTracer(),
		flowDebugger:   flowdebug.NewDebugger(logger),
	}
}
