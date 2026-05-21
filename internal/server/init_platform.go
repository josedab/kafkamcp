package server

import (
	"context"
	"log/slog"

	"github.com/kafkamcp/kafkamcp/internal/aclmgmt"
	"github.com/kafkamcp/kafkamcp/internal/adaptiverate"
	"github.com/kafkamcp/kafkamcp/internal/agentmemory"
	"github.com/kafkamcp/kafkamcp/internal/autopilot"
	"github.com/kafkamcp/kafkamcp/internal/benchmark"
	"github.com/kafkamcp/kafkamcp/internal/capability"
	"github.com/kafkamcp/kafkamcp/internal/catalog"
	"github.com/kafkamcp/kafkamcp/internal/cdc"
	"github.com/kafkamcp/kafkamcp/internal/chaos"
	"github.com/kafkamcp/kafkamcp/internal/chat"
	"github.com/kafkamcp/kafkamcp/internal/clouddeploy"
	"github.com/kafkamcp/kafkamcp/internal/codec"
	codecavro "github.com/kafkamcp/kafkamcp/internal/codec/avro"
	"github.com/kafkamcp/kafkamcp/internal/collaboration"
	"github.com/kafkamcp/kafkamcp/internal/configvcs"
	"github.com/kafkamcp/kafkamcp/internal/controlplane"
	"github.com/kafkamcp/kafkamcp/internal/correlation"
	"github.com/kafkamcp/kafkamcp/internal/costattribution"
	"github.com/kafkamcp/kafkamcp/internal/crosslineage"
	"github.com/kafkamcp/kafkamcp/internal/dagworkflow"
	"github.com/kafkamcp/kafkamcp/internal/devmode"
	"github.com/kafkamcp/kafkamcp/internal/digitaltwin"
	"github.com/kafkamcp/kafkamcp/internal/encryption"
	enginebackend "github.com/kafkamcp/kafkamcp/internal/engine/backend"
	"github.com/kafkamcp/kafkamcp/internal/eventbus"
	"github.com/kafkamcp/kafkamcp/internal/eventsourcing"
	"github.com/kafkamcp/kafkamcp/internal/federation"
	"github.com/kafkamcp/kafkamcp/internal/finops"
	"github.com/kafkamcp/kafkamcp/internal/gateway"
	"github.com/kafkamcp/kafkamcp/internal/gitops"
	"github.com/kafkamcp/kafkamcp/internal/graphqlapi"
	"github.com/kafkamcp/kafkamcp/internal/healthscore"
	"github.com/kafkamcp/kafkamcp/internal/intentverify"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/kafkamcp/kafkamcp/internal/kafkaproxy"
	"github.com/kafkamcp/kafkamcp/internal/llmpipeline"
	"github.com/kafkamcp/kafkamcp/internal/migration"
	"github.com/kafkamcp/kafkamcp/internal/msgsampling"
	"github.com/kafkamcp/kafkamcp/internal/nlpipeline"
	"github.com/kafkamcp/kafkamcp/internal/observability"
	"github.com/kafkamcp/kafkamcp/internal/onboarding"
	"github.com/kafkamcp/kafkamcp/internal/opa"
	"github.com/kafkamcp/kafkamcp/internal/operator"
	"github.com/kafkamcp/kafkamcp/internal/orchestration"
	"github.com/kafkamcp/kafkamcp/internal/perfguard"
	"github.com/kafkamcp/kafkamcp/internal/pipeline"
	"github.com/kafkamcp/kafkamcp/internal/platform"
	"github.com/kafkamcp/kafkamcp/internal/playbook"
	"github.com/kafkamcp/kafkamcp/internal/plugin"
	"github.com/kafkamcp/kafkamcp/internal/portal"
	"github.com/kafkamcp/kafkamcp/internal/predictalert"
	"github.com/kafkamcp/kafkamcp/internal/profiler"
	"github.com/kafkamcp/kafkamcp/internal/replay"
	"github.com/kafkamcp/kafkamcp/internal/runbook"
	"github.com/kafkamcp/kafkamcp/internal/sandbox"
	"github.com/kafkamcp/kafkamcp/internal/schemaadvisor"
	"github.com/kafkamcp/kafkamcp/internal/schemaplayground"
	"github.com/kafkamcp/kafkamcp/internal/semantic"
	"github.com/kafkamcp/kafkamcp/internal/sessionreplay"
	"github.com/kafkamcp/kafkamcp/internal/smartroute"
	"github.com/kafkamcp/kafkamcp/internal/statemachine"
	"github.com/kafkamcp/kafkamcp/internal/streamsql"
	"github.com/kafkamcp/kafkamcp/internal/taskqueue"
	"github.com/kafkamcp/kafkamcp/internal/telemetry"
	"github.com/kafkamcp/kafkamcp/internal/timeseries"
	"github.com/kafkamcp/kafkamcp/internal/timetravel"
	"github.com/kafkamcp/kafkamcp/internal/toolcompose"
	"github.com/kafkamcp/kafkamcp/internal/topicportal"
	"github.com/kafkamcp/kafkamcp/internal/topictemplate"
	"github.com/kafkamcp/kafkamcp/internal/vectorsync"
	"github.com/kafkamcp/kafkamcp/internal/viewstore"
	"github.com/kafkamcp/kafkamcp/internal/wasmruntime"
	"github.com/kafkamcp/kafkamcp/internal/webhook"
	"github.com/kafkamcp/kafkamcp/internal/workflow"
)

// Platform groups infrastructure, orchestration, and extensibility subsystems.
type Platform struct {
	pluginRegistry       *plugin.Registry           // plugin/extension system
	agentRegistry        *kafka.AgentRegistry       // multi-agent collaboration
	otelProvider         *telemetry.Provider        // OpenTelemetry trace provider
	platformMgr          *platform.Manager          // multi-platform abstraction
	gitopsManager        *gitops.Manager            // GitOps configuration management
	migrationPlanner     *migration.Planner         // kafka migration assistant
	workflowEngine       *workflow.Engine           // agent workflow templates
	gatewayManager       *gateway.Manager           // multi-protocol streaming gateway
	eventTranslator      *gateway.EventTranslator   // cross-platform event translation
	collaborationCoord   *collaboration.Coordinator // agent collaboration
	semanticEngine       *semantic.Engine           // semantic search & RAG
	onboardingEngine     *onboarding.Engine         // zero-config onboarding
	opaPolicyEngine      *opa.PolicyEngine          // OPA policy-as-code engine
	operatorController   *operator.Controller       // Kubernetes operator
	pipelineEngine       *pipeline.Engine           // streaming function pipelines
	marketplace          *plugin.Marketplace        // plugin marketplace
	observabilityMgr     *observability.Manager     // observability dashboards & alerts
	chaosEngine          *chaos.Engine              // chaos engineering toolkit
	replayEngine         *replay.Engine             // interactive replay & time-travel
	federationCoord      *federation.Coordinator    // federated multi-instance coordination
	agentProfiler        *profiler.Profiler         // agent performance profiler
	dagWorkflowEngine    *dagworkflow.Engine        // DAG-based workflow orchestrator
	viewStoreEngine      *viewstore.Engine          // materialized view store
	agentMemoryEngine    *agentmemory.Engine        // per-agent persistent memory
	taskQueueEngine      *taskqueue.Engine          // multi-agent task queue
	streamSQLEngine      *streamsql.Engine          // streaming SQL engine
	catalogEngine        *catalog.Engine            // data catalog & discovery
	adaptiveRateEngine   *adaptiverate.Engine       // ML-driven rate limiting
	configVCSEngine      *configvcs.Engine          // config version control
	costAttrEngine       *costattribution.Engine    // real-time cost attribution
	sandboxEngine        *sandbox.Engine            // agent sandbox environments
	agentConfigStore     *plugin.AgentConfigStore   // agent config marketplace
	cdcEngine            *cdc.Engine                // CDC bridge engine
	orchestrationEngine  *orchestration.Engine      // multi-agent orchestration
	schemaAdvisor        *schemaadvisor.Advisor     // schema evolution advisor
	nlPipelineBuilder    *nlpipeline.Builder        // NL data pipeline builder
	playbookEngine       *playbook.Engine           // anomaly response playbooks
	cloudDeployGenerator *clouddeploy.Generator     // cloud deployment generator
	vectorSyncEngine     *vectorsync.Engine         // vector DB sync bridge
	finOpsDashboard      *finops.Dashboard          // FinOps cost intelligence
	codecEngine          *codec.Engine              // Avro/Protobuf wire codec
	eventSourcingEngine  *eventsourcing.Engine      // agent action event sourcing
	topicPortalEngine    *topicportal.Engine        // self-service topic portal
	gitopsReconciler     *gitops.Reconciler         // GitOps reconciliation loop
	llmPipelineEngine    *llmpipeline.Engine        // LLM training data pipeline
	chatEngine           *chat.Engine               // conversational REPL chat engine
	smartRouter          *smartroute.Router         // intelligent tool routing & composition
	timeseriesEngine     *timeseries.Engine         // time-series analytics engine
	timeTravelEngine     *timetravel.Engine         // point-in-time state reconstruction
	controlPlane         *controlplane.Manager      // multi-tenant SaaS control plane
	schemaPlayground     *schemaplayground.Engine   // interactive schema playground
	capabilityNegotiator *capability.Negotiator     // agent capability negotiation
	digitalTwin          *digitaltwin.Engine        // cluster digital twin simulation
	correlationEngine    *correlation.Engine        // observability correlation engine
	devModeEngine        *devmode.Engine            // offline-first local dev mode
	healthScoreEngine    *healthscore.Engine        // unified cluster health scoring
	topicTemplateEngine  *topictemplate.Engine      // declarative topic-as-code templates
	sessionReplayEngine  *sessionreplay.Engine      // agent session recording & replay
	predictAlertEngine   *predictalert.Engine       // predictive consumer group alerting
	msgSamplingEngine    *msgsampling.Engine        // message sampling & profiling
	runbookEngine        *runbook.Engine            // automated runbook executor
	crossLineageEngine   *crosslineage.Engine       // cross-topic data lineage
	aclMgmtEngine        *aclmgmt.Engine            // ACL management with diff/audit
	benchmarkEngine      *benchmark.Engine          // embedded benchmarking & load testing
	webhookManager       *webhook.Manager           // webhook event gateway
	autopilotEngine      *autopilot.Engine          // automated migration autopilot
	wasmEngine           *wasmruntime.Engine        // WASM plugin runtime
	graphqlEngine        *graphqlapi.Engine         // GraphQL federation layer
	reviewStore          *plugin.ReviewStore        // marketplace review store
	encryptionEngine     *encryption.Engine         // field-level encryption engine
	portalEngine         *portal.Engine             // governance portal data layer
	kafkaProxy           *kafkaproxy.Proxy          // Kafka wire protocol proxy
	eventBus             *eventbus.Bus              // real-time agent observability bus
	perfGuard            *perfguard.Guard           // performance regression guard
	toolComposeEngine    *toolcompose.Engine        // MCP tool composition engine
	stateMachineEngine   *statemachine.Engine       // topic-backed state machine engine
	intentVerifier       *intentverify.Verifier     // agent intent verification
}

// newPlatform initializes all platform subsystems. backend is the shared
// persistence backend used by engines that support durable state; passing
// nil keeps every engine in-process.
func newPlatform(logger *slog.Logger, be enginebackend.Backend) Platform {
	viewStoreEngine, err := viewstore.NewEngineWithBackend(context.Background(), logger, be)
	if err != nil {
		logger.Error("viewstore: failed to restore from persistence, continuing with empty state", "error", err)
		viewStoreEngine = viewstore.NewEngine(logger)
	}
	agentMemoryEngine, err := agentmemory.NewEngineWithBackend(context.Background(), logger, be)
	if err != nil {
		logger.Error("agentmemory: failed to restore from persistence, continuing with empty state", "error", err)
		agentMemoryEngine = agentmemory.NewEngine(logger)
	}
	p := Platform{
		pluginRegistry:       plugin.NewRegistry(logger),
		agentRegistry:        kafka.NewAgentRegistry(),
		otelProvider:         nil,
		platformMgr:          platform.NewManager(logger),
		gitopsManager:        gitops.NewManager(logger),
		migrationPlanner:     migration.NewPlanner(),
		workflowEngine:       workflow.NewEngine(logger),
		gatewayManager:       gateway.NewManager(logger),
		eventTranslator:      gateway.NewEventTranslator(gateway.NewManager(logger), logger),
		collaborationCoord:   collaboration.NewCoordinator(logger),
		semanticEngine:       semantic.NewEngine(logger),
		onboardingEngine:     onboarding.NewEngine(logger),
		opaPolicyEngine:      opa.NewPolicyEngine(logger),
		operatorController:   operator.NewController(logger),
		pipelineEngine:       pipeline.NewEngine(logger),
		observabilityMgr:     observability.NewManager(),
		chaosEngine:          chaos.NewEngine(logger),
		replayEngine:         replay.NewEngine(logger),
		federationCoord:      federation.NewCoordinator("local", logger),
		agentProfiler:        profiler.NewProfiler(),
		dagWorkflowEngine:    dagworkflow.NewEngine(logger),
		viewStoreEngine:      viewStoreEngine,
		agentMemoryEngine:    agentMemoryEngine,
		taskQueueEngine:      taskqueue.NewEngine(logger),
		streamSQLEngine:      streamsql.NewEngine(logger),
		catalogEngine:        catalog.NewEngine(logger),
		adaptiveRateEngine:   adaptiverate.NewEngine(logger),
		configVCSEngine:      configvcs.NewEngine(logger),
		costAttrEngine:       costattribution.NewEngine(logger),
		sandboxEngine:        sandbox.NewEngine(logger),
		agentConfigStore:     plugin.NewAgentConfigStore(),
		cdcEngine:            cdc.NewEngine(logger),
		orchestrationEngine:  orchestration.NewEngine(logger),
		schemaAdvisor:        schemaadvisor.NewAdvisor(logger),
		nlPipelineBuilder:    nlpipeline.NewBuilder(logger),
		playbookEngine:       playbook.NewEngine(logger),
		cloudDeployGenerator: clouddeploy.NewGenerator(logger),
		vectorSyncEngine:     vectorsync.NewEngine(logger),
		finOpsDashboard:      finops.NewDashboard(logger),
		codecEngine:          codec.NewEngineWithAvro(nil, codecavro.New()),
		eventSourcingEngine:  eventsourcing.NewEngine(logger),
		topicPortalEngine:    topicportal.NewEngine(topicportal.DefaultApprovalChainConfig(), logger),
		gitopsReconciler:     gitops.NewReconciler(gitops.DefaultReconcilerConfig(), gitops.NewManager(logger), logger),
		llmPipelineEngine:    llmpipeline.NewEngine(logger),
		chatEngine:           chat.NewEngine(logger),
		smartRouter:          smartroute.NewRouter(logger),
		timeseriesEngine:     timeseries.NewEngine(logger),
		timeTravelEngine:     timetravel.NewEngine(logger),
		controlPlane:         controlplane.NewManager(logger),
		schemaPlayground:     schemaplayground.NewEngine(logger),
		capabilityNegotiator: capability.NewNegotiator(logger),
		digitalTwin:          digitaltwin.NewEngine(logger),
		correlationEngine:    correlation.NewEngine(logger),
		devModeEngine:        devmode.NewEngine(logger),
		healthScoreEngine:    healthscore.NewEngine(logger),
		topicTemplateEngine:  topictemplate.NewEngine(logger),
		sessionReplayEngine:  sessionreplay.NewEngine(logger),
		predictAlertEngine:   predictalert.NewEngine(logger),
		msgSamplingEngine:    msgsampling.NewEngine(logger),
		runbookEngine:        runbook.NewEngine(logger),
		crossLineageEngine:   crosslineage.NewEngine(logger),
		aclMgmtEngine:        aclmgmt.NewEngine(logger),
		benchmarkEngine:      benchmark.NewEngine(logger),
		webhookManager:       webhook.NewManager(logger),
		autopilotEngine:      autopilot.NewEngine(migration.NewPlanner(), digitaltwin.NewEngine(logger), sandbox.NewEngine(logger), logger),
		wasmEngine:           wasmruntime.NewEngine(logger),
		graphqlEngine:        graphqlapi.NewEngine(logger),
		reviewStore:          plugin.NewReviewStore(),
		encryptionEngine:     encryption.NewEngine(logger),
		portalEngine:         portal.NewEngine(logger),
		eventBus:             eventbus.NewBus(eventbus.DefaultBusConfig(), logger),
		perfGuard:            perfguard.NewGuard(perfguard.DefaultGuardConfig(), logger),
		toolComposeEngine:    toolcompose.NewEngine(logger),
		stateMachineEngine:   statemachine.NewEngine(logger),
		intentVerifier:       intentverify.NewVerifier(intentverify.DefaultVerifierConfig(), logger),
	}
	p.marketplace = plugin.NewMarketplace(p.pluginRegistry)
	return p
}
