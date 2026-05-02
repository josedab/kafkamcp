# KafkaMCP Tool Reference

> Auto-generated on 2026-05-04 from `internal/server/register_*.go`.
> Do not edit manually. Run `go generate ./internal/server/` to regenerate.

## Summary

**251 tools** available.

| Tool | Description |
|------|-------------|
| [`agent_action_analytics`](#agent_action_analytics) | Get tool usage analytics including call counts, error rates, latency percenti... |
| [`agent_action_export`](#agent_action_export) | Export agent actions as JSONL for LLM fine-tuning on domain-specific Kafka op... |
| [`agent_action_replay`](#agent_action_replay) | Replay an agent session showing all tool calls, parameters, results, and timing. |
| [`agent_action_stats`](#agent_action_stats) | Get event sourcing engine statistics. |
| [`agent_clock_get`](#agent_clock_get) | Get the vector clock for a resource to check causal ordering of operations ac... |
| [`agent_clock_increment`](#agent_clock_increment) | Increment the vector clock for a resource, recording an operation by this agent. |
| [`agent_coordinate_list`](#agent_coordinate_list) | List all registered agents with their status, capabilities, and last seen time. |
| [`agent_coordinate_register`](#agent_coordinate_register) | Register this agent's capabilities and status for coordination with other age... |
| [`agent_lock_acquire`](#agent_lock_acquire) | Acquire a distributed lock on a shared resource (topic, consumer group, schem... |
| [`agent_lock_list`](#agent_lock_list) | List all active distributed locks showing resource, owner, and expiration. |
| [`agent_lock_release`](#agent_lock_release) | Release a previously acquired distributed lock on a resource. |
| [`agent_message_inbox`](#agent_message_inbox) | Read messages sent to this agent. Supports filtering to unread-only. |
| [`agent_message_send`](#agent_message_send) | Send a message to another agent for coordination. Use '*' as recipient for br... |
| [`agent_threat_alerts`](#agent_threat_alerts) | List recent behavioral threat alerts detected for agents (data exfiltration, ... |
| [`agent_threat_profile`](#agent_threat_profile) | Get the behavioral profile for an agent, including access patterns, topic usa... |
| [`agent_threat_stats`](#agent_threat_stats) | Get threat detection system statistics including profile count and alert totals. |
| [`codec_decode`](#codec_decode) | Decode a wire-format message (Confluent Schema Registry format with Avro/Prot... |
| [`codec_detect_format`](#codec_detect_format) | Detect the serialization format of a message (Confluent wire format, JSON, or... |
| [`codec_encode`](#codec_encode) | Encode a message using Confluent wire format (magic byte + schema ID + Avro/P... |
| [`gateway_consume`](#gateway_consume) | Consume messages from a named streaming backend (Pulsar, RabbitMQ, NATS). |
| [`gateway_create_topic`](#gateway_create_topic) | Create a topic/queue/subject on a named streaming backend. |
| [`gateway_list_backends`](#gateway_list_backends) | List all registered multi-protocol streaming backends (Pulsar, RabbitMQ, NATS... |
| [`gateway_list_topics`](#gateway_list_topics) | List topics/queues/subjects on a named streaming backend. |
| [`gateway_produce`](#gateway_produce) | Produce a message through a named streaming backend (Pulsar, RabbitMQ, NATS). |
| [`gitops_reconcile`](#gitops_reconcile) | Run a GitOps reconciliation against the live cluster state. Defaults to dry-r... |
| [`gitops_reconcile_history`](#gitops_reconcile_history) | Get recent GitOps reconciliation results. |
| [`gitops_set_desired_state`](#gitops_set_desired_state) | Set the desired Kafka state for GitOps reconciliation (topics, schemas, ACLs ... |
| [`gitops_webhook`](#gitops_webhook) | Process a Git webhook event (push or pull_request) for PR-based reconciliatio... |
| [`k8s_operator_manage`](#k8s_operator_manage) | Manage KafkaMCP Kubernetes operator instances. Actions: create, get, list, up... |
| [`kafka_acl_manage`](#kafka_acl_manage) | Manage Kafka ACLs — list, audit, diff, and dry-run access control entries f... |
| [`kafka_advisor_report`](#kafka_advisor_report) | Generate a detailed advisory report with health scores, category breakdowns, ... |
| [`kafka_advisor_scan`](#kafka_advisor_scan) | Scan topics for best-practice violations, anti-patterns, and improvement oppo... |
| [`kafka_agent_config_get`](#kafka_agent_config_get) | Get details of a specific agent configuration or tool bundle from the marketp... |
| [`kafka_agent_config_publish`](#kafka_agent_config_publish) | Publish an agent configuration or tool bundle to the marketplace. |
| [`kafka_agent_config_search`](#kafka_agent_config_search) | Search the agent configuration marketplace for pre-built agent configs, tool ... |
| [`kafka_agent_optimize`](#kafka_agent_optimize) | Get optimization suggestions for an agent based on its call patterns. Detects... |
| [`kafka_agent_profile`](#kafka_agent_profile) | Profile agent performance: latency distributions, tool call patterns, error r... |
| [`kafka_agent_registry`](#kafka_agent_registry) | Multi-agent collaboration: register agents, claim partitions, share cursors, ... |
| [`kafka_agent_report`](#kafka_agent_report) | Generate a comprehensive agent efficiency report with trends, comparisons, an... |
| [`kafka_aggregate`](#kafka_aggregate) | Compute count, sum, avg, min, or max over a time window on a numeric field in... |
| [`kafka_alert_create`](#kafka_alert_create) | Create an alerting rule for Kafka metrics. Supports consumer lag, throughput,... |
| [`kafka_alert_delete`](#kafka_alert_delete) | Delete an alerting rule by ID. |
| [`kafka_alert_list`](#kafka_alert_list) | List all configured alerting rules. |
| [`kafka_alter_topic`](#kafka_alter_topic) | Modify configuration of an existing Kafka topic. |
| [`kafka_analyze_dlq`](#kafka_analyze_dlq) | Analyze a dead-letter queue topic: consume recent messages, parse error heade... |
| [`kafka_anomalies`](#kafka_anomalies) | Get AI-detected anomalies in consumer lag, throughput, and error rates. Uses ... |
| [`kafka_anomaly_ingest`](#kafka_anomaly_ingest) | Manually ingest a data point for anomaly detection analysis. |
| [`kafka_benchmark`](#kafka_benchmark) | Embedded benchmarking and load testing. Measure producer/consumer throughput,... |
| [`kafka_capability_profile`](#kafka_capability_profile) | View the current agent's capability profile including allowed tools, role, an... |
| [`kafka_capacity_forecast`](#kafka_capacity_forecast) | Forecast capacity for a specific topic using exponential smoothing and linear... |
| [`kafka_capacity_plan`](#kafka_capacity_plan) | Generate a comprehensive capacity plan with ML-based forecasting, storage gro... |
| [`kafka_catalog_search`](#kafka_catalog_search) | Search the data catalog for topics, schemas, connectors by name, description,... |
| [`kafka_catalog_stats`](#kafka_catalog_stats) | Get catalog statistics: entry counts by type, freshness distribution, quality... |
| [`kafka_catalog_tag`](#kafka_catalog_tag) | Add tags to a catalog entry, index a new entry, or set lineage information. |
| [`kafka_cdc_create_bridge`](#kafka_cdc_create_bridge) | Create a CDC bridge that streams changes from a Kafka topic to a target data ... |
| [`kafka_cdc_list_bridges`](#kafka_cdc_list_bridges) | List all CDC bridges with their status and statistics. |
| [`kafka_cdc_manage_bridge`](#kafka_cdc_manage_bridge) | Manage a CDC bridge: get status, pause, resume, stop, or delete. |
| [`kafka_cdc_process`](#kafka_cdc_process) | Process a batch of messages through a CDC bridge, applying transformations an... |
| [`kafka_chaos_inject`](#kafka_chaos_inject) | Inject controlled chaos into a Kafka environment. Types: broker_outage, netwo... |
| [`kafka_chaos_status`](#kafka_chaos_status) | Get the status of chaos experiments. Provide experiment_id for a specific exp... |
| [`kafka_chaos_stop`](#kafka_chaos_stop) | Stop a running chaos experiment and trigger rollback. |
| [`kafka_chat_context`](#kafka_chat_context) | Get the accumulated context for a chat session, including tracked entities, t... |
| [`kafka_chat_message`](#kafka_chat_message) | Send a message in a chat session. The engine classifies intent, resolves refe... |
| [`kafka_chat_session`](#kafka_chat_session) | Manage conversational chat sessions for multi-turn Kafka exploration. Support... |
| [`kafka_chat_template`](#kafka_chat_template) | Get investigation templates for common scenarios like incident triage, capaci... |
| [`kafka_check_schema_compatibility`](#kafka_check_schema_compatibility) | Check if a proposed schema is compatible with the subject's compatibility pol... |
| [`kafka_cloud_deploy_generate`](#kafka_cloud_deploy_generate) | Generate zero-config deployment templates for AWS (MSK), GCP, Azure (Event Hu... |
| [`kafka_cloud_deploy_list`](#kafka_cloud_deploy_list) | List previously generated deployment templates. |
| [`kafka_compliance_report`](#kafka_compliance_report) | Generate a comprehensive compliance report with scores, violations, retention... |
| [`kafka_compliance_scan`](#kafka_compliance_scan) | Scan topics for compliance with GDPR, HIPAA, SOC2, or PCI-DSS. Detects PII ex... |
| [`kafka_compliance_violations`](#kafka_compliance_violations) | List detected compliance violations with optional filters. |
| [`kafka_config_compare`](#kafka_config_compare) | Compare two versions of a resource configuration showing a unified diff. |
| [`kafka_config_drift`](#kafka_config_drift) | Detect configuration drift across clusters. Compare topic configs between clu... |
| [`kafka_config_history`](#kafka_config_history) | Get configuration change history for a resource with full diff details. Suppo... |
| [`kafka_config_rollback`](#kafka_config_rollback) | Rollback a resource configuration to a previous version. |
| [`kafka_connect_bridge`](#kafka_connect_bridge) | Unified Kafka Connect MCP bridge. Manage connectors through a single tool: li... |
| [`kafka_connector_health`](#kafka_connector_health) | Get health score and diagnostics for a Kafka Connect connector including task... |
| [`kafka_consume`](#kafka_consume) | Read messages from a Kafka topic with flexible positioning. Supports earliest... |
| [`kafka_contract_violations`](#kafka_contract_violations) | View contract violations, enforce contracts on messages, set enforcement mode... |
| [`kafka_controlplane_instance`](#kafka_controlplane_instance) | Manage KafkaMCP instances across organizations. Provision, start, stop, termi... |
| [`kafka_controlplane_org`](#kafka_controlplane_org) | Manage organizations in the multi-tenant SaaS control plane. Create, list, or... |
| [`kafka_controlplane_usage`](#kafka_controlplane_usage) | View usage and billing metrics for an organization including tool calls, mess... |
| [`kafka_correlation_analyze`](#kafka_correlation_analyze) | Analyze correlations between Kafka metrics and external signals within a time... |
| [`kafka_correlation_endpoints`](#kafka_correlation_endpoints) | Register or list external observability endpoints for signal ingestion. |
| [`kafka_correlation_ingest`](#kafka_correlation_ingest) | Ingest external observability signals (from Prometheus, Datadog, PagerDuty, o... |
| [`kafka_cost_allocate`](#kafka_cost_allocate) | Create or view cost allocation groups that map resources to teams, services, ... |
| [`kafka_cost_analyze`](#kafka_cost_analyze) | Analyze storage and compute costs for a Kafka topic. Estimates monthly cost b... |
| [`kafka_cost_budget`](#kafka_cost_budget) | Create spending budgets with alert thresholds or view budget status. |
| [`kafka_cost_live`](#kafka_cost_live) | Get real-time cost metrics for a resource or all resources with per-hour/day/... |
| [`kafka_cost_waste`](#kafka_cost_waste) | Detect cost waste across all topics: idle topics, over-replication, compressi... |
| [`kafka_cost_whatif`](#kafka_cost_whatif) | Simulate configuration changes and see cost impact. What-if analysis for rete... |
| [`kafka_create_connector`](#kafka_create_connector) | Create a new Kafka Connect connector with the specified configuration. |
| [`kafka_create_topic`](#kafka_create_topic) | Create a new Kafka topic with specified partitions, replication factor, and c... |
| [`kafka_cross_lineage`](#kafka_cross_lineage) | Cross-topic data lineage tracking and visualization. Maps how data flows betw... |
| [`kafka_dashboard_create`](#kafka_dashboard_create) | Auto-provision Kafka monitoring dashboards for Grafana, Datadog, or custom pr... |
| [`kafka_delete_connector`](#kafka_delete_connector) | Delete a Kafka Connect connector. |
| [`kafka_delete_topic`](#kafka_delete_topic) | Delete a Kafka topic. This is a destructive operation. |
| [`kafka_describe_connector`](#kafka_describe_connector) | Get full details for a Kafka Connect connector including config, task status,... |
| [`kafka_describe_consumer_group`](#kafka_describe_consumer_group) | Get the state, members, partition assignments, and per-partition lag of a con... |
| [`kafka_describe_topic`](#kafka_describe_topic) | Get detailed metadata for a specific topic including partition details, repli... |
| [`kafka_dev_environment`](#kafka_dev_environment) | Get the local development environment status, sample topics, quick start guid... |
| [`kafka_dev_sample_data`](#kafka_dev_sample_data) | Generate sample data for local development. Available generators: ecommerce-o... |
| [`kafka_dev_tutorial`](#kafka_dev_tutorial) | Interactive guided tutorials for learning KafkaMCP. List tutorials, start one... |
| [`kafka_diagnose_lag`](#kafka_diagnose_lag) | Deep diagnostic analysis of consumer group lag. Traces root cause: slow consu... |
| [`kafka_discover_clusters`](#kafka_discover_clusters) | Auto-discover Kafka clusters by scanning common ports, environment variables,... |
| [`kafka_discover_tools`](#kafka_discover_tools) | Progressive disclosure meta-tool: discover additional tools in a specific cat... |
| [`kafka_failover_readiness`](#kafka_failover_readiness) | Evaluate failover readiness for a replication link. Provides readiness score ... |
| [`kafka_federation_join`](#kafka_federation_join) | Manage federation membership. Actions: join (register instance), leave (dereg... |
| [`kafka_federation_query`](#kafka_federation_query) | Execute a query across all federated instances. Routes tool calls to matching... |
| [`kafka_federation_status`](#kafka_federation_status) | Get federation status: instances, policies, health. Actions: status (overview... |
| [`kafka_finops_budget`](#kafka_finops_budget) | Set a budget alert for Kafka infrastructure spend. |
| [`kafka_finops_record_cost`](#kafka_finops_record_cost) | Record topic cost data for FinOps tracking and analysis. |
| [`kafka_finops_summary`](#kafka_finops_summary) | Get a cost intelligence summary including total spend, topic costs, optimizat... |
| [`kafka_flow_bottlenecks`](#kafka_flow_bottlenecks) | Detect bottlenecks in the message flow: high consumer lag, low throughput, hi... |
| [`kafka_flow_debug`](#kafka_flow_debug) | Live message flow debugging. Builds a real-time graph of producers, topics, a... |
| [`kafka_forecast`](#kafka_forecast) | ML-based capacity forecasting using linear regression and seasonal decomposit... |
| [`kafka_get_schema`](#kafka_get_schema) | Get the full schema definition for a subject. Returns parsed schema, field su... |
| [`kafka_gitops_diff`](#kafka_gitops_diff) | Compare desired Kafka configuration (declarative YAML) against live cluster s... |
| [`kafka_group_heal`](#kafka_group_heal) | Auto-heal consumer groups: diagnose root cause (stuck partition, slow consume... |
| [`kafka_group_health`](#kafka_group_health) | Evaluate consumer group health with scoring (0-100), detect zombies, rebalanc... |
| [`kafka_health_score`](#kafka_health_score) | Get a unified health score for a Kafka cluster by aggregating broker status, ... |
| [`kafka_incident_ingest`](#kafka_incident_ingest) | Ingest an anomaly event for cross-cluster correlation. |
| [`kafka_incident_list`](#kafka_incident_list) | List correlated incidents across Kafka clusters with root cause analysis. |
| [`kafka_incident_report`](#kafka_incident_report) | Generate a structured incident report with timeline, blast radius, root cause... |
| [`kafka_incident_stats`](#kafka_incident_stats) | Get incident correlation engine statistics. |
| [`kafka_join`](#kafka_join) | Perform a time-windowed join across two topics on a key field. Returns matche... |
| [`kafka_knowledge_graph`](#kafka_knowledge_graph) | Query the embedded knowledge graph of topic relationships, consumer group top... |
| [`kafka_list_acls`](#kafka_list_acls) | List all Kafka ACLs with principal, resource, and operation breakdown. Includ... |
| [`kafka_list_connectors`](#kafka_list_connectors) | List all Kafka Connect connectors with their status, type, and worker assignm... |
| [`kafka_list_consumer_groups`](#kafka_list_consumer_groups) | List all consumer groups with summary state, member count, and total lag. |
| [`kafka_list_schemas`](#kafka_list_schemas) | List all subjects in the Schema Registry with latest version, type, and compa... |
| [`kafka_list_topics`](#kafka_list_topics) | List all Kafka topics in the cluster with metadata including partition count,... |
| [`kafka_memory_recall`](#kafka_memory_recall) | Recall a specific memory entry by key, or list all entries. Actions: get, lis... |
| [`kafka_memory_search`](#kafka_memory_search) | Search agent memory using TF-IDF semantic search. Returns ranked results by r... |
| [`kafka_memory_store`](#kafka_memory_store) | Store a memory entry for the current agent. Supports categories (preference, ... |
| [`kafka_message_sample`](#kafka_message_sample) | Statistical sampling and profiling of messages on a topic. Provides schema di... |
| [`kafka_migrate_execute`](#kafka_migrate_execute) | Execute a migration plan. Supports dry-run mode, parallel execution, and prog... |
| [`kafka_migrate_rollback`](#kafka_migrate_rollback) | Rollback a completed or failed migration. |
| [`kafka_migrate_status`](#kafka_migrate_status) | Get the status and progress of a migration execution. |
| [`kafka_migration_plan`](#kafka_migration_plan) | Plan and validate Kafka migrations between clusters. Actions: plan (diff clus... |
| [`kafka_negotiate_capabilities`](#kafka_negotiate_capabilities) | Negotiate agent capabilities to receive a contextual subset of available tool... |
| [`kafka_nl_pipeline_build`](#kafka_nl_pipeline_build) | Build a data pipeline from a natural language description. Translates plain E... |
| [`kafka_nl_pipeline_manage`](#kafka_nl_pipeline_manage) | Manage NL-generated pipelines: list, get, validate, deploy, or delete. |
| [`kafka_nl_query`](#kafka_nl_query) | Query Kafka with plain English. Semantic parser handles complex boolean filte... |
| [`kafka_orchestration_agents`](#kafka_orchestration_agents) | Register an agent for orchestration or list available agents. |
| [`kafka_orchestration_complete_step`](#kafka_orchestration_complete_step) | Complete a step in an orchestration with results, enabling dependent steps to... |
| [`kafka_orchestration_create`](#kafka_orchestration_create) | Create a multi-agent orchestration that coordinates agents working together o... |
| [`kafka_orchestration_handoff`](#kafka_orchestration_handoff) | Create a task handoff between agents with context and message for collaborati... |
| [`kafka_orchestration_status`](#kafka_orchestration_status) | Get status of an orchestration, list all orchestrations, or get ready steps f... |
| [`kafka_persona`](#kafka_persona) | Manage declarative agent personas. Actions: list (all personas), get (persona... |
| [`kafka_pipeline_create`](#kafka_pipeline_create) | Create a streaming function pipeline with filter/map/enrich/transform steps. ... |
| [`kafka_pipeline_execute`](#kafka_pipeline_execute) | Execute a pipeline on messages from the source topic. Consumes, processes thr... |
| [`kafka_pipeline_manage`](#kafka_pipeline_manage) | Manage streaming function pipelines. Actions: list, get, delete, runs (execut... |
| [`kafka_platform_info`](#kafka_platform_info) | Get information about configured Kafka-compatible platforms (Apache Kafka, Re... |
| [`kafka_playbook_execute`](#kafka_playbook_execute) | Execute an anomaly response playbook. Triggers automated remediation steps fo... |
| [`kafka_playbook_list`](#kafka_playbook_list) | List anomaly response playbooks. Playbooks define automated remediation steps... |
| [`kafka_playbook_manage`](#kafka_playbook_manage) | Manage playbooks and executions: get status, complete steps, enable/disable, ... |
| [`kafka_playbook_match`](#kafka_playbook_match) | Find playbooks matching a specific anomaly trigger type and value. |
| [`kafka_playground`](#kafka_playground) | Sandboxed Kafka environment for safe experimentation. Create temporary topics... |
| [`kafka_plugin_install`](#kafka_plugin_install) | Install a plugin from the marketplace. Validates dependencies and security ca... |
| [`kafka_plugin_list`](#kafka_plugin_list) | List installed plugins with their status, version, provided tools, and resour... |
| [`kafka_plugin_search`](#kafka_plugin_search) | Search the plugin marketplace for extensions. Find monitoring, security, anal... |
| [`kafka_policy_evaluate`](#kafka_policy_evaluate) | Evaluate an OPA Rego policy for an authorization decision. Returns whether th... |
| [`kafka_policy_manage`](#kafka_policy_manage) | Manage OPA Rego policies. Actions: list (show policies), register (add/update... |
| [`kafka_policy_simulate`](#kafka_policy_simulate) | Simulate a policy evaluation showing how each policy rule matches, without re... |
| [`kafka_predict_alert`](#kafka_predict_alert) | Predictive consumer group alerting. Analyzes lag trends, consumption rates, a... |
| [`kafka_produce`](#kafka_produce) | Publish one or more messages to a Kafka topic. Returns delivery confirmation ... |
| [`kafka_quality_report`](#kafka_quality_report) | Generate a detailed data quality report for a topic including trends, SLA sta... |
| [`kafka_quality_score`](#kafka_quality_score) | Get the real-time data quality score for a Kafka topic across freshness, comp... |
| [`kafka_query`](#kafka_query) | Accept a natural language description of desired data and translate it to a t... |
| [`kafka_quickstart`](#kafka_quickstart) | Guided onboarding: auto-discovers Kafka clusters, suggests configuration, and... |
| [`kafka_quota_request`](#kafka_quota_request) | Request a quota adjustment: increase, decrease, reset, or grant burst credits. |
| [`kafka_quota_status`](#kafka_quota_status) | Get adaptive rate limit status for an agent or all agents. Shows current rate... |
| [`kafka_rag_context`](#kafka_rag_context) | Build RAG (Retrieval-Augmented Generation) context from indexed Kafka message... |
| [`kafka_recommend`](#kafka_recommend) | Get intelligent recommendations for topic optimization: detect under-replicat... |
| [`kafka_remediate_dlq`](#kafka_remediate_dlq) | Auto-remediate DLQ messages: retry with backoff, route to correction topic, o... |
| [`kafka_replay_create`](#kafka_replay_create) | Create a bookmark capturing message state at the current point in time. Store... |
| [`kafka_replay_diff`](#kafka_replay_diff) | Compare message payloads between two bookmarks. Detects schema changes, conte... |
| [`kafka_replay_simulate`](#kafka_replay_simulate) | Simulate replaying messages through a consumer group. Predicts processing imp... |
| [`kafka_replication_lag`](#kafka_replication_lag) | Get detailed partition-level replication lag for a specific replication link. |
| [`kafka_replication_status`](#kafka_replication_status) | Get real-time cross-cluster replication status for MirrorMaker2/Confluent Rep... |
| [`kafka_reset_offsets`](#kafka_reset_offsets) | Reset consumer group offsets. Supports earliest, latest, or timestamp strateg... |
| [`kafka_restart_connector`](#kafka_restart_connector) | Restart a Kafka Connect connector or specific tasks. Supports bulk restart of... |
| [`kafka_retention_approve`](#kafka_retention_approve) | Approve a pending retention change request. |
| [`kafka_retention_change_request`](#kafka_retention_change_request) | Request a retention change with approval workflow. Reducing retention require... |
| [`kafka_retention_policy`](#kafka_retention_policy) | Get or set the intelligent retention policy for a topic, including compliance... |
| [`kafka_retention_recommendations`](#kafka_retention_recommendations) | Get AI-powered retention recommendations based on usage patterns, compliance,... |
| [`kafka_runbook`](#kafka_runbook) | Automated runbook executor for multi-step operational procedures (partition r... |
| [`kafka_sandbox_create`](#kafka_sandbox_create) | Create an isolated sandbox environment for safe experimentation with Kafka op... |
| [`kafka_sandbox_destroy`](#kafka_sandbox_destroy) | Destroy a sandbox, list sandboxes, get details, or generate a promotion report. |
| [`kafka_sandbox_exec`](#kafka_sandbox_exec) | Execute a Kafka tool within a sandbox. All operations are tracked with resour... |
| [`kafka_schema_advisor_analyze`](#kafka_schema_advisor_analyze) | Analyze schema evolution history and provide health score, patterns, best pra... |
| [`kafka_schema_advisor_migrate`](#kafka_schema_advisor_migrate) | Generate a step-by-step migration plan for evolving a schema between versions. |
| [`kafka_schema_advisor_predict`](#kafka_schema_advisor_predict) | Predict compatibility issues for a proposed schema change. Analyzes breaking ... |
| [`kafka_schema_apply`](#kafka_schema_apply) | Apply, review, or rollback a schema proposal. Actions: review (approve/reject... |
| [`kafka_schema_diff`](#kafka_schema_diff) | Compute a field-by-field diff between two schema versions showing additions, ... |
| [`kafka_schema_playground`](#kafka_schema_playground) | Interactive schema playground for drafting, testing, and validating schemas. ... |
| [`kafka_schema_propose`](#kafka_schema_propose) | Propose a schema change with auto-generated migration plan, compatibility val... |
| [`kafka_search`](#kafka_search) | Search/filter messages in a topic by key, header, value content, or time rang... |
| [`kafka_semantic_index`](#kafka_semantic_index) | Index Kafka messages for semantic search. Consumes recent messages from a top... |
| [`kafka_semantic_search`](#kafka_semantic_search) | Search indexed Kafka messages by meaning using semantic similarity. Returns t... |
| [`kafka_semantic_stats`](#kafka_semantic_stats) | Get statistics about the semantic search index including document counts, top... |
| [`kafka_session_replay`](#kafka_session_replay) | Record and replay AI agent sessions for debugging. Captures tool call traces,... |
| [`kafka_sla_configure`](#kafka_sla_configure) | Configure a data quality SLA for a topic with threshold alerts. |
| [`kafka_sla_status`](#kafka_sla_status) | Get the SLA compliance status for a topic or list all configured SLAs. |
| [`kafka_smart_route`](#kafka_smart_route) | Analyze a high-level request and automatically compose a multi-tool execution... |
| [`kafka_smart_route_status`](#kafka_smart_route_status) | Get status of a smart route execution plan, list plans, get next executable s... |
| [`kafka_sql_execute`](#kafka_sql_execute) | Execute a streaming SQL query against Kafka topics. Supports SELECT, WHERE, G... |
| [`kafka_sql_explain`](#kafka_sql_explain) | Explain a streaming SQL query plan without executing it. Shows execution step... |
| [`kafka_sql_manage`](#kafka_sql_manage) | Manage SQL queries. Actions: list, get, delete. |
| [`kafka_stream`](#kafka_stream) | Manage streaming consumer sessions for real-time message delivery. Actions: s... |
| [`kafka_subscribe`](#kafka_subscribe) | Subscribe to a Kafka topic using consumer group semantics. Polls for new mess... |
| [`kafka_suggest_rbac`](#kafka_suggest_rbac) | Generate recommended RBAC configuration based on your Kafka topology. Outputs... |
| [`kafka_suggest_schema_evolution`](#kafka_suggest_schema_evolution) | Given a current schema subject and a desired change description, suggest comp... |
| [`kafka_task_claim`](#kafka_task_claim) | Claim the highest-priority pending task from a queue. |
| [`kafka_task_complete`](#kafka_task_complete) | Mark a claimed task as completed or failed. |
| [`kafka_task_publish`](#kafka_task_publish) | Publish a task to a priority queue for other agents to claim and execute. |
| [`kafka_task_status`](#kafka_task_status) | Get task status, queue statistics, or list tasks. Actions: get, stats, list, ... |
| [`kafka_time_travel`](#kafka_time_travel) | Point-in-time topic queries: snapshot state at a timestamp, reconstruct compa... |
| [`kafka_timeseries_analyze`](#kafka_timeseries_analyze) | Advanced time-series analysis: Mann-Kendall trend detection, FFT seasonality ... |
| [`kafka_timeseries_baseline`](#kafka_timeseries_baseline) | Compute a baseline from historical data and compare current metrics against i... |
| [`kafka_timeseries_compare`](#kafka_timeseries_compare) | Compare time-series metrics between two windows to identify changes, degradat... |
| [`kafka_timeseries_list`](#kafka_timeseries_list) | List all tracked time-series and available analysis windows. |
| [`kafka_timeseries_query`](#kafka_timeseries_query) | Query time-series metrics for a topic or consumer group within a configurable... |
| [`kafka_topic_approve`](#kafka_topic_approve) | Approve a pending topic provisioning request. |
| [`kafka_topic_reject`](#kafka_topic_reject) | Reject a pending topic provisioning request. |
| [`kafka_topic_request`](#kafka_topic_request) | Submit a self-service topic provisioning request with justification and appro... |
| [`kafka_topic_request_status`](#kafka_topic_request_status) | Check the status of topic provisioning requests. |
| [`kafka_topic_template`](#kafka_topic_template) | Manage declarative topic-as-code templates. Define topic settings (partitions... |
| [`kafka_trace_flow`](#kafka_trace_flow) | Distributed tracing for Kafka messages: inject W3C Trace Context headers, cor... |
| [`kafka_trace_lineage`](#kafka_trace_lineage) | Track data lineage through Kafka topics. Shows producer→topic→consumer re... |
| [`kafka_trace_message`](#kafka_trace_message) | Trace a message across topics using correlation IDs. Auto-detects correlation... |
| [`kafka_twin_simulate`](#kafka_twin_simulate) | Simulate operations against a cluster digital twin without affecting producti... |
| [`kafka_twin_snapshot`](#kafka_twin_snapshot) | Capture a snapshot of the current cluster state (topics, consumer groups, bro... |
| [`kafka_update_connector`](#kafka_update_connector) | Update an existing Kafka Connect connector configuration. |
| [`kafka_usage_report`](#kafka_usage_report) | Generate usage analytics and chargeback reports. Actions: rollup (usage summa... |
| [`kafka_validate_contract`](#kafka_validate_contract) | Validate messages against data contracts, register contracts, or get complian... |
| [`kafka_vector_sync_create`](#kafka_vector_sync_create) | Create a sync bridge that streams Kafka topic data into a vector database (Pi... |
| [`kafka_vector_sync_manage`](#kafka_vector_sync_manage) | Manage vector sync bridges: list, get, pause, resume, delete. |
| [`kafka_vector_sync_search`](#kafka_vector_sync_search) | Perform semantic similarity search against synced vector data from Kafka topics. |
| [`kafka_view_create`](#kafka_view_create) | Create a materialized view over a Kafka topic with projections, aggregations ... |
| [`kafka_view_query`](#kafka_view_query) | Query a materialized view with point lookups, range scans, or full scans. Ret... |
| [`kafka_view_status`](#kafka_view_status) | Manage materialized views. Actions: list, get, delete, stats. |
| [`kafka_watch`](#kafka_watch) | Register or manage event-driven watch subscriptions on topic patterns. When m... |
| [`kafka_workflow_create`](#kafka_workflow_create) | Create a reusable DAG-based workflow template for multi-step Kafka operations... |
| [`kafka_workflow_run`](#kafka_workflow_run) | Start a workflow execution from a template. Returns the run ID and initial re... |
| [`kafka_workflow_status`](#kafka_workflow_status) | Get workflow status, manage workflows, or advance a running workflow. Actions... |
| [`kafka_workflows`](#kafka_workflows) | Manage pre-built workflow templates for common Kafka operations. Actions: lis... |
| [`llm_pipeline_create`](#llm_pipeline_create) | Create an LLM training data export pipeline from Kafka topics with sampling, ... |
| [`llm_pipeline_list`](#llm_pipeline_list) | List all configured LLM training data export pipelines. |
| [`llm_pipeline_status`](#llm_pipeline_status) | Get the status and results of an LLM training data pipeline. |

---

## Tools

### `agent_action_analytics`

Get tool usage analytics including call counts, error rates, latency percentiles, and unique agent counts.

**Source:** `register_eventsourcing.go`

### `agent_action_export`

Export agent actions as JSONL for LLM fine-tuning on domain-specific Kafka operations.

**Source:** `register_eventsourcing.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `agent_id` | string |  | Filter by agent ID |
| `tool` | string |  | Filter by tool name |
| `status` | string |  | Filter by status: success or error |
| `limit` | number |  | Max events. Default: 10000 |

### `agent_action_replay`

Replay an agent session showing all tool calls, parameters, results, and timing.

**Source:** `register_eventsourcing.go`

### `agent_action_stats`

Get event sourcing engine statistics.

**Source:** `register_eventsourcing.go`

### `agent_clock_get`

Get the vector clock for a resource to check causal ordering of operations across agents.

**Source:** `register_collaboration.go`

### `agent_clock_increment`

Increment the vector clock for a resource, recording an operation by this agent.

**Source:** `register_collaboration.go`

### `agent_coordinate_list`

List all registered agents with their status, capabilities, and last seen time.

**Source:** `register_collaboration.go`

### `agent_coordinate_register`

Register this agent's capabilities and status for coordination with other agents.

**Source:** `register_collaboration.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `status` | string |  | Agent status: active, idle, busy. Default: active |
| `capabilities` | object |  | List of agent capabilities |

### `agent_lock_acquire`

Acquire a distributed lock on a shared resource (topic, consumer group, schema) to prevent concurrent modifications by multiple agents.

**Source:** `register_collaboration.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `ttl_seconds` | number |  | Lock TTL in seconds. Default: 30, Max: 3600 |

### `agent_lock_list`

List all active distributed locks showing resource, owner, and expiration.

**Source:** `register_collaboration.go`

### `agent_lock_release`

Release a previously acquired distributed lock on a resource.

**Source:** `register_collaboration.go`

### `agent_message_inbox`

Read messages sent to this agent. Supports filtering to unread-only.

**Source:** `register_collaboration.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `unread_only` | boolean |  | Only return unread messages. Default: false |

### `agent_message_send`

Send a message to another agent for coordination. Use '*' as recipient for broadcasts.

**Source:** `register_collaboration.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `type` | string |  | Message type: request, response, broadcast, notification. Default: notification |
| `body` | string |  | Message body |

### `agent_threat_alerts`

List recent behavioral threat alerts detected for agents (data exfiltration, privilege escalation, bulk reads).

**Source:** `register_threat_detection.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `agent_id` | string |  | Filter by agent ID |
| `limit` | number |  | Max alerts to return. Default: 50 |

### `agent_threat_profile`

Get the behavioral profile for an agent, including access patterns, topic usage, and baseline status.

**Source:** `register_threat_detection.go`

### `agent_threat_stats`

Get threat detection system statistics including profile count and alert totals.

**Source:** `register_threat_detection.go`

### `codec_decode`

Decode a wire-format message (Confluent Schema Registry format with Avro/Protobuf support, auto-detect with JSON fallback).

**Source:** `register_codec.go`

### `codec_detect_format`

Detect the serialization format of a message (Confluent wire format, JSON, or raw bytes).

**Source:** `register_codec.go`

### `codec_encode`

Encode a message using Confluent wire format (magic byte + schema ID + Avro/Protobuf/JSON payload).

**Source:** `register_codec.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `format` | string |  | Encoding format: avro, protobuf, json. Default: json |

### `gateway_consume`

Consume messages from a named streaming backend (Pulsar, RabbitMQ, NATS).

**Source:** `register_gateway.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `group` | string |  | Consumer group/subscription name |
| `limit` | number |  | Max messages to consume. Default: 10 |

### `gateway_create_topic`

Create a topic/queue/subject on a named streaming backend.

**Source:** `register_gateway.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `partitions` | number |  | Number of partitions. Default: 1 |

### `gateway_list_backends`

List all registered multi-protocol streaming backends (Pulsar, RabbitMQ, NATS) and their health status.

**Source:** `register_gateway.go`

### `gateway_list_topics`

List topics/queues/subjects on a named streaming backend.

**Source:** `register_gateway.go`

### `gateway_produce`

Produce a message through a named streaming backend (Pulsar, RabbitMQ, NATS).

**Source:** `register_gateway.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `key` | string |  | Message key |

### `gitops_reconcile`

Run a GitOps reconciliation against the live cluster state. Defaults to dry-run mode.

**Source:** `register_gitops_reconcile.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `dry_run` | boolean |  | Run in dry-run mode (no changes applied). Default: true |

### `gitops_reconcile_history`

Get recent GitOps reconciliation results.

**Source:** `register_gitops_reconcile.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `limit` | number |  | Max results. Default: 10 |

### `gitops_set_desired_state`

Set the desired Kafka state for GitOps reconciliation (topics, schemas, ACLs in YAML format).

**Source:** `register_gitops_reconcile.go`

### `gitops_webhook`

Process a Git webhook event (push or pull_request) for PR-based reconciliation workflows.

**Source:** `register_gitops_reconcile.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `ref` | string |  | Git ref (e.g., refs/heads/main) |
| `action` | string |  | PR action: opened, synchronize, closed, merged |

### `k8s_operator_manage`

Manage KafkaMCP Kubernetes operator instances. Actions: create, get, list, update, delete, scale, manifest, crd.

**Source:** `register_operator.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string |  | Instance name |
| `namespace` | string |  | Kubernetes namespace. Default: default |
| `replicas` | number |  | Number of replicas |
| `image` | string |  | Container image |
| `version` | string |  | Image version tag |
| `transport` | string |  | MCP transport: stdio, sse, streamable-http |
| `port` | number |  | Service port |

### `kafka_acl_manage`

Manage Kafka ACLs — list, audit, diff, and dry-run access control entries for topics, consumer groups, and cluster resources.

**Source:** `register_aclmgmt.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `principal` | string |  | Principal filter or target (e.g., User:app1) |
| `resource_type` | string |  | Resource type filter: topic, group, cluster |
| `cluster` | string |  | Cluster name for multi-cluster setups. |
| `rules` | object |  | ACL rules array (for set_desired). Each rule: {resource_type, resource_name, operation, permission} |

### `kafka_advisor_report`

Generate a detailed advisory report with health scores, category breakdowns, and prioritized recommendations.

**Source:** `register_advisor.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cluster` | string |  | Cluster name |

### `kafka_advisor_scan`

Scan topics for best-practice violations, anti-patterns, and improvement opportunities. Checks replication factors, partition counts, retention policies, naming conventions, orphaned topics, and topic sprawl.

**Source:** `register_advisor.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cluster` | string |  | Cluster name |
| `severity` | string |  | Filter by severity: info, warning, critical |
| `category` | string |  | Filter by category: replication, partitioning, retention, naming, hygiene |

### `kafka_agent_config_get`

Get details of a specific agent configuration or tool bundle from the marketplace.

**Source:** `register_agentstore.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `type` | string |  | Type: config or bundle. Default: config |

### `kafka_agent_config_publish`

Publish an agent configuration or tool bundle to the marketplace.

**Source:** `register_agentstore.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `category` | string |  | Category: monitoring, operations, analytics, pipeline, security |
| `type` | string |  | Type: config or bundle. Default: config |

### `kafka_agent_config_search`

Search the agent configuration marketplace for pre-built agent configs, tool bundles, and integration setups.

**Source:** `register_agentstore.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `query` | string |  | Search query (matches name, description, tags) |
| `category` | string |  | Filter by category: monitoring, operations, analytics, pipeline, security |
| `type` | string |  | Type to search: configs, bundles, or all. Default: all |
| `verified_only` | boolean |  | Only show verified entries. Default: false |

### `kafka_agent_optimize`

Get optimization suggestions for an agent based on its call patterns. Detects redundant calls, suboptimal tool sequences, and suggests improvements.

**Source:** `register_profiler.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `since_hours` | number |  | Look back period in hours. Default: 24 |

### `kafka_agent_profile`

Profile agent performance: latency distributions, tool call patterns, error rates, and efficiency scoring. Actions: profile (agent metrics), slow_queries (find slow calls), list_agents (all agents).

**Source:** `register_profiler.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `agent_id` | string |  | Agent ID to profile |
| `since_hours` | number |  | Look back period in hours. Default: 24 |
| `threshold_ms` | number |  | Slow query threshold in ms. Default: 1000 |

### `kafka_agent_registry`

Multi-agent collaboration: register agents, claim partitions, share cursors, and handoff messages. Actions: register, heartbeat, claim, release, share_cursor, handoff, list_agents.

**Source:** `register_streaming.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `agent_name` | string |  | Agent name for registration |
| `topic` | string |  | Topic for partition operations |
| `partition` | number |  | Partition number to claim/release |
| `offset` | number |  | Offset for cursor sharing |
| `target_agent` | string |  | Target agent for handoff |
| `message_data` | string |  | Message data for handoff (JSON string) |
| `cluster` | string |  | Cluster name. |

### `kafka_agent_report`

Generate a comprehensive agent efficiency report with trends, comparisons, and scoring.

**Source:** `register_profiler.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `agent_id` | string |  | Agent ID (empty = all agents) |
| `since_hours` | number |  | Look back period in hours. Default: 24 |

### `kafka_aggregate`

Compute count, sum, avg, min, or max over a time window on a numeric field in topic messages. Lightweight stream processing.

**Source:** `register_p2.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `field` | string | ✓ | Dot-notation path to the numeric field (e.g., 'amount', 'metrics.latency_ms'). Empty for the entire value. |
| `window_ms` | number |  | Time window in milliseconds. Default: 300000 (5 min) |
| `cluster` | string |  | Cluster name. |

### `kafka_alert_create`

Create an alerting rule for Kafka metrics. Supports consumer lag, throughput, error rate, and partition metrics with configurable thresholds and notification channels.

**Source:** `register_observability.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `condition` | string |  | Condition: gt, lt, eq, ne. Default: gt |
| `severity` | string |  | Severity: info, warning, critical. Default: warning |
| `duration` | string |  | Duration before firing: 5m, 15m, 1h. Default: 5m |
| `topic` | string |  | Topic to monitor |
| `group_id` | string |  | Consumer group to monitor |

### `kafka_alert_delete`

Delete an alerting rule by ID.

**Source:** `register_observability.go`

### `kafka_alert_list`

List all configured alerting rules.

**Source:** `register_observability.go`

### `kafka_alter_topic`

Modify configuration of an existing Kafka topic.

**Source:** `register_p1.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cluster` | string |  | Cluster name. |

### `kafka_analyze_dlq`

Analyze a dead-letter queue topic: consume recent messages, parse error headers, categorize failure types, and return a statistical summary with samples.

**Source:** `register_p1.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `limit` | number |  | Max messages to analyze. Default: 100 |
| `cluster` | string |  | Cluster name. |

### `kafka_anomalies`

Get AI-detected anomalies in consumer lag, throughput, and error rates. Uses z-score and IQR statistical models with configurable sensitivity.

**Source:** `register_features.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `severity` | string |  | Filter by severity: warning, critical |
| `metric` | string |  | Filter by metric type: consumer_lag, throughput, error_rate |
| `limit` | number |  | Max alerts to return. Default: 50 |
| `since_hours` | number |  | Look back period in hours. Default: 24 |

### `kafka_anomaly_ingest`

Manually ingest a data point for anomaly detection analysis.

**Source:** `register_features.go`

### `kafka_benchmark`

Embedded benchmarking and load testing. Measure producer/consumer throughput, latency percentiles, and topic performance under configurable synthetic workloads.

**Source:** `register_benchmark.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `topic` | string |  | Topic to benchmark (required for run) |
| `type` | string |  | Benchmark type: producer, consumer, both (default: producer) |
| `message_count` | number |  | Number of messages to produce (default: 1000, max: 100000) |
| `message_size` | number |  | Message size in bytes (default: 100, max: 1MB) |
| `concurrency` | number |  | Number of concurrent workers (default: 1, max: 20) |
| `bench_id` | string |  | Benchmark ID (for status action) |
| `cluster` | string |  | Cluster name for multi-cluster setups. |

### `kafka_capability_profile`

View the current agent's capability profile including allowed tools, role, and context.

**Source:** `register_capability.go`

### `kafka_capacity_forecast`

Forecast capacity for a specific topic using exponential smoothing and linear regression. Predicts throughput, storage, and lag trends.

**Source:** `register_capacity.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `metric` | string | ✓ | Metric to forecast: throughput, storage, lag. Default: throughput |
| `horizon_days` | number |  | Forecast horizon in days. Default: 7 |
| `cluster` | string |  | Cluster name. |

### `kafka_capacity_plan`

Generate a comprehensive capacity plan with ML-based forecasting, storage growth analysis, and scaling recommendations for all tracked topics.

**Source:** `register_capacity.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `horizon_days` | number |  | Forecast horizon in days. Default: 30 |
| `max_storage_gb` | number |  | Maximum cluster storage in GB. Default: 1000 |
| `cluster` | string |  | Cluster name. |

### `kafka_catalog_search`

Search the data catalog for topics, schemas, connectors by name, description, tag, owner, freshness, or quality score.

**Source:** `register_catalog.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `query` | string |  | Full-text search query |
| `type` | string |  | Filter by type: topic, schema, connector, consumer_group |
| `tags` | object |  | Filter by tags |
| `owner` | string |  | Filter by owner |
| `min_quality` | number |  | Minimum quality score (0-100) |
| `freshness` | string |  | Filter by freshness: live, stale, inactive |
| `limit` | number |  | Max results (default: 20) |

### `kafka_catalog_stats`

Get catalog statistics: entry counts by type, freshness distribution, quality scores, tag usage.

**Source:** `register_catalog.go`

### `kafka_catalog_tag`

Add tags to a catalog entry, index a new entry, or set lineage information.

**Source:** `register_catalog.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `entry_id` | string |  | Catalog entry ID (for tag/lineage/delete) |
| `tags` | object |  | Tags to add |
| `name` | string |  | Resource name (for index) |
| `type` | string |  | Resource type: topic, schema, connector (for index) |
| `description` | string |  | Description (for index) |
| `owner` | string |  | Owner (for index) |
| `lineage` | object |  | Lineage info: {upstream, downstream, producers, consumers} |

### `kafka_cdc_create_bridge`

Create a CDC bridge that streams changes from a Kafka topic to a target data store (postgres, mysql, mongodb, elasticsearch, s3, bigquery, snowflake, redis, http).

**Source:** `register_cdc.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `table` | string |  | Target table/collection/index name |
| `key_field` | string |  | Primary key field for upserts |
| `cluster` | string |  | Source Kafka cluster name |

### `kafka_cdc_list_bridges`

List all CDC bridges with their status and statistics.

**Source:** `register_cdc.go`

### `kafka_cdc_manage_bridge`

Manage a CDC bridge: get status, pause, resume, stop, or delete.

**Source:** `register_cdc.go`

### `kafka_cdc_process`

Process a batch of messages through a CDC bridge, applying transformations and tracking change operations.

**Source:** `register_cdc.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cluster` | string |  | Kafka cluster name |

### `kafka_chaos_inject`

Inject controlled chaos into a Kafka environment. Types: broker_outage, network_partition, slow_consumer, schema_corruption, rebalance_storm, producer_flood. Actions: inject (start experiment), scenario (run pre-built scenario), list_scenarios.

**Source:** `register_chaos.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `type` | string |  | Experiment type (for inject action) |
| `scenario` | string |  | Pre-built scenario name (for scenario action) |
| `topics` | object |  | Target topics array |
| `groups` | object |  | Target consumer groups array |
| `parameters` | object |  | Experiment parameters |
| `max_duration_minutes` | number |  | Max duration in minutes. Default: 10 |
| `category` | string |  | Scenario category filter: network, infrastructure, data, consumer, producer |

### `kafka_chaos_status`

Get the status of chaos experiments. Provide experiment_id for a specific experiment, or omit for all experiments.

**Source:** `register_chaos.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `experiment_id` | string |  | Specific experiment ID |
| `status_filter` | string |  | Filter by status: running, completed, rolled_back, stopped |

### `kafka_chaos_stop`

Stop a running chaos experiment and trigger rollback.

**Source:** `register_chaos.go`

### `kafka_chat_context`

Get the accumulated context for a chat session, including tracked entities, turn count, and session age.

**Source:** `register_chat.go`

### `kafka_chat_message`

Send a message in a chat session. The engine classifies intent, resolves references to previous context (e.g., 'that topic', 'same group'), and tracks entities across turns.

**Source:** `register_chat.go`

### `kafka_chat_session`

Manage conversational chat sessions for multi-turn Kafka exploration. Supports creating, listing, and closing sessions that maintain context across turns.

**Source:** `register_chat.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `session_id` | string |  | Session ID (required for get/close) |

### `kafka_chat_template`

Get investigation templates for common scenarios like incident triage, capacity review, and schema audit. Templates provide step-by-step guided workflows.

**Source:** `register_chat.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `template_id` | string |  | Template ID (required for get) |
| `category` | string |  | Filter by category: incident, capacity, schema, governance |

### `kafka_check_schema_compatibility`

Check if a proposed schema is compatible with the subject's compatibility policy.

**Source:** `register_p0.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `schema_type` | string |  | Schema type: AVRO, PROTOBUF, JSON. Default: AVRO |
| `version` | string |  | Version to check against. Default: 'latest' |

### `kafka_cloud_deploy_generate`

Generate zero-config deployment templates for AWS (MSK), GCP, Azure (Event Hubs), or Confluent Cloud. Produces Docker Compose, Kubernetes manifests, cloud CLI commands, and config files.

**Source:** `register_clouddeploy.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `service` | string |  | Kafka service: aws-msk, confluent-cloud, azure-event-hubs, gcp-managed-kafka |
| `bootstrap_servers` | string |  | Kafka bootstrap servers |
| `region` | string |  | Cloud region (defaults to provider's primary region) |
| `cluster_name` | string |  | Cluster name. Default: kafkamcp |

### `kafka_cloud_deploy_list`

List previously generated deployment templates.

**Source:** `register_clouddeploy.go`

### `kafka_compliance_report`

Generate a comprehensive compliance report with scores, violations, retention checks, and recommendations.

**Source:** `register_compliance.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cluster` | string |  | Cluster name |

### `kafka_compliance_scan`

Scan topics for compliance with GDPR, HIPAA, SOC2, or PCI-DSS. Detects PII exposure, retention policy violations, and missing data masking.

**Source:** `register_compliance.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `regulation` | string |  | Regulation to check: GDPR, HIPAA, SOC2, PCI-DSS. Empty = all |
| `cluster` | string |  | Cluster name |
| `topic` | string |  | Specific topic to scan (empty = all) |

### `kafka_compliance_violations`

List detected compliance violations with optional filters.

**Source:** `register_compliance.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `regulation` | string |  | Filter by regulation |
| `severity` | string |  | Filter by severity: critical, high, medium, low |

### `kafka_config_compare`

Compare two versions of a resource configuration showing a unified diff.

**Source:** `register_configvcs.go`

### `kafka_config_drift`

Detect configuration drift across clusters. Compare topic configs between clusters, check against golden templates, and generate remediation commands.

**Source:** `register_advanced.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `action` | string |  | Action: compare, audit, template. Default: compare |
| `topic` | string |  | Specific topic to compare |
| `source_cluster` | string |  | Source cluster for comparison |
| `target_cluster` | string |  | Target cluster for comparison |
| `template` | object |  | Golden template configs for audit |
| `cluster` | string |  | Cluster name. |

### `kafka_config_history`

Get configuration change history for a resource with full diff details. Supports SOC2-ready audit reports.

**Source:** `register_configvcs.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `limit` | number |  | Max history entries (default: 50) |
| `audit_report` | boolean |  | Generate a full audit report |

### `kafka_config_rollback`

Rollback a resource configuration to a previous version.

**Source:** `register_configvcs.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `reason` | string |  | Reason for rollback |

### `kafka_connect_bridge`

Unified Kafka Connect MCP bridge. Manage connectors through a single tool: list, describe, create, update, pause, resume, restart, delete, and health check.

**Source:** `register_connect_bridge.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string |  | Connector name (required for all actions except list) |
| `config` | object |  | Connector configuration map (for create/update) |
| `task_id` | number |  | Specific task ID to restart (-1 for all, default: -1) |
| `failed_only` | boolean |  | Only restart failed tasks (for restart action) |
| `cluster` | string |  | Cluster name for multi-cluster setups. |

### `kafka_connector_health`

Get health score and diagnostics for a Kafka Connect connector including task states, error rates, and recommendations.

**Source:** `register_advanced.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cluster` | string |  | Cluster name. |

### `kafka_consume`

Read messages from a Kafka topic with flexible positioning. Supports earliest, latest, numeric offset, or ISO 8601 timestamp.

**Source:** `register_p0.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `partition` | number | ✓ | Specific partition (-1 for all). Default: all |
| `offset` | string |  | Start position: 'earliest', 'latest', numeric offset, or ISO 8601 timestamp. Default: 'latest' |
| `limit` | number |  | Maximum messages to return (1-1000). Default: 10 |
| `deserializer` | string |  | Deserialization format: auto, avro, protobuf, json, string, bytes. Default: auto |
| `timeout_ms` | number |  | Maximum wait time in ms. Default: 5000 |
| `cluster` | string |  | Cluster name. |

### `kafka_contract_violations`

View contract violations, enforce contracts on messages, set enforcement mode, or infer contracts from samples. Actions: violations (get violation summary), enforce (validate with enforcement), set_mode (set enforcement mode: warn/reject/quarantine), infer (generate contract from samples).

**Source:** `register_contracts.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `message` | object | ✓ | Message to enforce (for enforce action) |
| `mode` | string |  | Enforcement mode: warn, reject, quarantine (for set_mode action) |
| `samples` | object |  | Array of message samples for contract inference (for infer action) |

### `kafka_controlplane_instance`

Manage KafkaMCP instances across organizations. Provision, start, stop, terminate instances with lifecycle management.

**Source:** `register_controlplane.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `instance_id` | string |  | Instance ID (for get/start/stop/terminate) |
| `org_id` | string |  | Organization ID (for provision/list) |
| `name` | string |  | Instance name (for provision) |
| `tier` | string |  | Instance tier: standard, premium, enterprise |
| `region` | string |  | Deployment region |
| `bootstrap_servers` | string |  | Kafka bootstrap servers (for provision) |

### `kafka_controlplane_org`

Manage organizations in the multi-tenant SaaS control plane. Create, list, or get organization details including API keys and members.

**Source:** `register_controlplane.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `org_id` | string |  | Organization ID (for get/add_member/create_api_key) |
| `name` | string |  | Organization name (for create) or API key name |
| `plan` | string |  | Plan: starter, pro, enterprise (for create) |
| `email` | string |  | Member email (for add_member) |
| `role` | string |  | Member role: admin, editor, viewer (for add_member) |

### `kafka_controlplane_usage`

View usage and billing metrics for an organization including tool calls, messages, instance hours, and estimated costs.

**Source:** `register_controlplane.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `period` | string |  | Billing period: current, last_month. Default: current |

### `kafka_correlation_analyze`

Analyze correlations between Kafka metrics and external signals within a time window. Produces temporal correlations, root cause ranking, and unified timeline.

**Source:** `register_correlation.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `window` | string |  | Time window: 1h, 6h, 24h. Default: 1h |
| `metric` | string |  | Filter by specific metric name |

### `kafka_correlation_endpoints`

Register or list external observability endpoints for signal ingestion.

**Source:** `register_correlation.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `source` | string |  | Source type: prometheus, datadog, pagerduty |
| `url` | string |  | Endpoint URL (for register) |

### `kafka_correlation_ingest`

Ingest external observability signals (from Prometheus, Datadog, PagerDuty, or custom sources) for correlation with Kafka metrics.

**Source:** `register_correlation.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `severity` | string |  | Signal severity: info, warning, critical |
| `message` | string |  | Optional message describing the signal |

### `kafka_cost_allocate`

Create or view cost allocation groups that map resources to teams, services, or projects.

**Source:** `register_costattribution.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string |  | Allocation group name (for create) |
| `owner` | string |  | Team/project owner (for create) |
| `resources` | object |  | Resource names to include (for create) |
| `tags` | object |  | Tags for the allocation group |

### `kafka_cost_analyze`

Analyze storage and compute costs for a Kafka topic. Estimates monthly cost based on retention, partitions, throughput, and replication.

**Source:** `register_features.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `provider` | string | ✓ | Cost model: aws-msk, confluent-cloud, self-hosted. Default: aws-msk |
| `cluster` | string |  | Cluster name. |

### `kafka_cost_budget`

Create spending budgets with alert thresholds or view budget status.

**Source:** `register_costattribution.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string |  | Budget name (for create) |
| `monthly_limit` | number |  | Monthly spending limit in USD (for create) |
| `resources` | object |  | Resources to track (for create) |
| `alert_thresholds` | object |  | Alert thresholds as fractions, e.g. [0.5, 0.8, 0.9, 1.0] |

### `kafka_cost_live`

Get real-time cost metrics for a resource or all resources with per-hour/day/month breakdowns.

**Source:** `register_costattribution.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `resource_type` | string |  | Resource type: topic, consumer_group, connector |
| `resource_name` | string |  | Resource name (omit for all) |

### `kafka_cost_waste`

Detect cost waste across all topics: idle topics, over-replication, compression opportunities, excessive retention.

**Source:** `register_features.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `provider` | string |  | Cost model: aws-msk, confluent-cloud, self-hosted. Default: aws-msk |
| `cluster` | string |  | Cluster name. |

### `kafka_cost_whatif`

Simulate configuration changes and see cost impact. What-if analysis for retention, compression, partitions, and replication changes.

**Source:** `register_features.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `new_retention_ms` | number | ✓ | New retention period in ms |
| `new_compression` | string |  | New compression type: gzip, snappy, lz4, zstd, none |
| `new_partitions` | number |  | New partition count |
| `new_replication_factor` | number |  | New replication factor |
| `provider` | string |  | Cost model. Default: aws-msk |
| `cluster` | string |  | Cluster name. |

### `kafka_create_connector`

Create a new Kafka Connect connector with the specified configuration.

**Source:** `register_advanced.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cluster` | string |  | Cluster name. |

### `kafka_create_topic`

Create a new Kafka topic with specified partitions, replication factor, and configuration.

**Source:** `register_p1.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `partitions` | number | ✓ | Number of partitions. Default: 1 |
| `replication_factor` | number |  | Replication factor. Default: 1 |
| `cluster` | string |  | Cluster name. |

### `kafka_cross_lineage`

Cross-topic data lineage tracking and visualization. Maps how data flows between topics through streams applications, connectors, and consumers.

**Source:** `register_crosslineage.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string |  | Node name (for register_node, trace) |
| `node_type` | string |  | Node type: topic, connector, stream_app, consumer_group, external (for register_node) |
| `source_id` | string |  | Source node ID (for add_edge) |
| `target_id` | string |  | Target node ID (for add_edge) |
| `flow_type` | string |  | Flow type: produces, consumes, transforms, replicates (for add_edge) |
| `label` | string |  | Edge label (for add_edge) |
| `id` | string |  | Node or edge ID (for remove_node, remove_edge) |
| `max_depth` | number |  | Max lineage trace depth (for trace, default: 5) |

### `kafka_dashboard_create`

Auto-provision Kafka monitoring dashboards for Grafana, Datadog, or custom providers. Creates pre-configured panels for cluster, topic, or consumer group metrics.

**Source:** `register_observability.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `provider` | string |  | Dashboard provider: grafana, datadog, custom. Default: grafana |
| `type` | string |  | Dashboard type: cluster, topic, consumer_group. Default: cluster |
| `target` | string |  | Target resource (topic name or group ID) |

### `kafka_delete_connector`

Delete a Kafka Connect connector.

**Source:** `register_advanced.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cluster` | string |  | Cluster name. |

### `kafka_delete_topic`

Delete a Kafka topic. This is a destructive operation.

**Source:** `register_p1.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cluster` | string |  | Cluster name. |

### `kafka_describe_connector`

Get full details for a Kafka Connect connector including config, task status, and errors.

**Source:** `register_p2.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cluster` | string |  | Cluster name. |

### `kafka_describe_consumer_group`

Get the state, members, partition assignments, and per-partition lag of a consumer group.

**Source:** `register_p0.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cluster` | string |  | Cluster name. |

### `kafka_describe_topic`

Get detailed metadata for a specific topic including partition details, replicas, ISR, offsets, and configuration.

**Source:** `register_p0.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cluster` | string | ✓ | Cluster name for multi-cluster setups. |

### `kafka_dev_environment`

Get the local development environment status, sample topics, quick start guide, and setup instructions for offline-first development.

**Source:** `register_devmode.go`

### `kafka_dev_sample_data`

Generate sample data for local development. Available generators: ecommerce-orders, user-events, iot-sensors, click-stream, payment-events.

**Source:** `register_devmode.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `count` | string |  | Number of messages to generate (1-100). Default: 10 |

### `kafka_dev_tutorial`

Interactive guided tutorials for learning KafkaMCP. List tutorials, start one, or get a specific tutorial's steps.

**Source:** `register_devmode.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `tutorial_id` | string |  | Tutorial ID (for get): getting-started, anomaly-detection, schema-management, smart-investigation, digital-twin |
| `difficulty` | string |  | Filter by difficulty: beginner, intermediate, advanced (for list) |

### `kafka_diagnose_lag`

Deep diagnostic analysis of consumer group lag. Traces root cause: slow consumers, producer spikes, partition imbalance, or GC pressure.

**Source:** `register_features.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cluster` | string |  | Cluster name. |

### `kafka_discover_clusters`

Auto-discover Kafka clusters by scanning common ports, environment variables, and DNS records.

**Source:** `register_onboarding.go`

### `kafka_discover_tools`

Progressive disclosure meta-tool: discover additional tools in a specific category. Categories: discovery, monitoring, admin, streaming, schema, analytics, governance, orchestration, platform.

**Source:** `register_capability.go`

### `kafka_failover_readiness`

Evaluate failover readiness for a replication link. Provides readiness score (0-100), RPO/RTO estimates, and actionable recommendations.

**Source:** `register_replication.go`

### `kafka_federation_join`

Manage federation membership. Actions: join (register instance), leave (deregister), heartbeat (update status).

**Source:** `register_federation.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `instance_id` | string |  | Instance ID |
| `name` | string |  | Instance name (for join) |
| `endpoint` | string |  | Instance endpoint URL (for join) |
| `capabilities` | object |  | Instance capabilities list |
| `clusters` | object |  | Cluster names managed by this instance |

### `kafka_federation_query`

Execute a query across all federated instances. Routes tool calls to matching instances and aggregates results.

**Source:** `register_federation.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `args` | object |  | Tool arguments |
| `targets` | object |  | Target instance IDs (empty = all) |

### `kafka_federation_status`

Get federation status: instances, policies, health. Actions: status (overview), instances (list), policies (list), audit (audit log).

**Source:** `register_federation.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `action` | string |  | Action: status, instances, policies, audit. Default: status |
| `limit` | number |  | Limit for audit log. Default: 50 |

### `kafka_finops_budget`

Set a budget alert for Kafka infrastructure spend.

**Source:** `register_finops.go`

### `kafka_finops_record_cost`

Record topic cost data for FinOps tracking and analysis.

**Source:** `register_finops.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `storage_gb` | number |  | Storage in GB |
| `partitions` | number |  | Number of partitions |
| `msgs_per_sec` | number |  | Messages per second |

### `kafka_finops_summary`

Get a cost intelligence summary including total spend, topic costs, optimization opportunities, and budget alerts.

**Source:** `register_finops.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `action` | string |  | Action: summary, snapshot, recommendations, forecast, budgets. Default: summary |
| `months` | number |  | Forecast months (for forecast action). Default: 6 |

### `kafka_flow_bottlenecks`

Detect bottlenecks in the message flow: high consumer lag, low throughput, high error rates, and high latency. Returns chart-ready JSON with severity levels.

**Source:** `register_flowdebug.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cluster` | string |  | Cluster name |
| `severity` | string |  | Filter by severity: warning, critical |
| `type` | string |  | Filter by type: high_lag, low_throughput, high_error_rate, high_latency |

### `kafka_flow_debug`

Live message flow debugging. Builds a real-time graph of producers, topics, and consumer groups with throughput/latency metrics. Actions: graph (get current flow graph), snapshots (historical snapshots), config (get/set bottleneck thresholds), collect (trigger metric collection from cluster).

**Source:** `register_flowdebug.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cluster` | string |  | Cluster name |
| `since_minutes` | number |  | Look back period for snapshots. Default: 60 |
| `limit` | number |  | Max snapshots to return. Default: 30 |
| `thresholds` | object |  | Bottleneck thresholds for config action (lag_warning, lag_critical, error_rate_warning, etc.) |

### `kafka_forecast`

ML-based capacity forecasting using linear regression and seasonal decomposition. Predicts throughput growth and storage needs.

**Source:** `register_features.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `metric` | string | ✓ | Metric to forecast: throughput, storage, lag. Default: throughput |
| `horizon_days` | number |  | Forecast horizon in days. Default: 7 |
| `cluster` | string |  | Cluster name. |

### `kafka_get_schema`

Get the full schema definition for a subject. Returns parsed schema, field summary, compatibility, and references.

**Source:** `register_p0.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `version` | string |  | Version: 'latest' or number. Default: 'latest' |

### `kafka_gitops_diff`

Compare desired Kafka configuration (declarative YAML) against live cluster state. Generates a diff and migration plan with dry-run support.

**Source:** `register_features.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `topics` | object |  | Array of desired topic specs: [{name, partitions, replication_factor, configs}] |
| `cluster` | string |  | Cluster name. |

### `kafka_group_heal`

Auto-heal consumer groups: diagnose root cause (stuck partition, slow consumer, GC pressure, rebalance storm), generate remediation playbook, and optionally apply fixes. Combines health scoring with rule-based diagnosis.

**Source:** `register_autoheal.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `auto_execute` | boolean |  | Auto-execute safe remediation steps. Default: false |
| `cluster` | string |  | Cluster name. |

### `kafka_group_health`

Evaluate consumer group health with scoring (0-100), detect zombies, rebalance storms, stuck partitions, and suggest auto-remediation.

**Source:** `register_features.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cluster` | string |  | Cluster name. |

### `kafka_health_score`

Get a unified health score for a Kafka cluster by aggregating broker status, under-replicated partitions, ISR shrink rate, and consumer lag into a single score (0-100).

**Source:** `register_healthscore.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cluster` | string |  | Cluster name for multi-cluster setups. |
| `action` | string |  | Action: evaluate (default), history |
| `history_limit` | number |  | Number of historical scores to return (default: 20) |

### `kafka_incident_ingest`

Ingest an anomaly event for cross-cluster correlation.

**Source:** `register_incident.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `topic` | string |  | Affected topic |
| `severity` | string |  | Severity: info, warning, critical. Default: warning |
| `value` | number |  | Metric value |

### `kafka_incident_list`

List correlated incidents across Kafka clusters with root cause analysis.

**Source:** `register_incident.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `limit` | number |  | Max incidents. Default: 10 |

### `kafka_incident_report`

Generate a structured incident report with timeline, blast radius, root cause, and remediation steps.

**Source:** `register_incident.go`

### `kafka_incident_stats`

Get incident correlation engine statistics.

**Source:** `register_incident.go`

### `kafka_join`

Perform a time-windowed join across two topics on a key field. Returns matched messages from both topics.

**Source:** `register_p2.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `key_field` | string |  | Dot-notation field to join on. Empty uses the message key. |
| `window_ms` | number |  | Time window in ms. Default: 60000 (1 min) |
| `limit` | number |  | Max joined results. Default: 100 |
| `cluster` | string |  | Cluster name. |

### `kafka_knowledge_graph`

Query the embedded knowledge graph of topic relationships, consumer group topologies, and schema dependencies. Supports: 'depends_on <node>', 'impacts <node>', 'path <from> to <to>', 'neighbors <node>', 'type <node_type>', or free-text search.

**Source:** `register_features.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cluster` | string |  | Cluster name. |

### `kafka_list_acls`

List all Kafka ACLs with principal, resource, and operation breakdown. Includes security analysis for over-permissioned principals and wildcard exposure.

**Source:** `register_advanced.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `principal` | string |  | Filter by principal (e.g., 'User:alice') |
| `resource_type` | string |  | Filter by resource type: topic, group, cluster, transactional_id |
| `include_analysis` | boolean |  | Include security analysis. Default: true |
| `cluster` | string |  | Cluster name. |

### `kafka_list_connectors`

List all Kafka Connect connectors with their status, type, and worker assignment.

**Source:** `register_p2.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cluster` | string |  | Cluster name. |

### `kafka_list_consumer_groups`

List all consumer groups with summary state, member count, and total lag.

**Source:** `register_p0.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `state_filter` | string |  | Filter by state (Stable, Empty, Dead) |
| `cluster` | string |  | Cluster name. |

### `kafka_list_schemas`

List all subjects in the Schema Registry with latest version, type, and compatibility level.

**Source:** `register_p0.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `filter` | string |  | Glob pattern to filter subject names |

### `kafka_list_topics`

List all Kafka topics in the cluster with metadata including partition count, replication factor, and estimated message count.

**Source:** `register_p0.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `filter` | string |  | Glob pattern to filter topic names (e.g., 'orders.*', '*-dlq') |
| `include_internal` | boolean |  | Include internal topics (e.g., __consumer_offsets). Default: false |
| `cluster` | string |  | Cluster name for multi-cluster setups. |

### `kafka_memory_recall`

Recall a specific memory entry by key, or list all entries. Actions: get, list, delete.

**Source:** `register_agentmemory.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `key` | string |  | Memory key (for get/delete) |
| `category` | string |  | Filter by category (for list) |

### `kafka_memory_search`

Search agent memory using TF-IDF semantic search. Returns ranked results by relevance.

**Source:** `register_agentmemory.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `category` | string |  | Filter by category |
| `limit` | number |  | Max results (default: 10) |

### `kafka_memory_store`

Store a memory entry for the current agent. Supports categories (preference, schema, query_history, context, note), tags, TTL expiration, and arbitrary key-value data.

**Source:** `register_agentmemory.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `category` | string |  | Category: preference, schema, query_history, context, note |
| `tags` | object |  | Tags for categorization |
| `ttl_sec` | number |  | Time-to-live in seconds (0 = no expiry) |
| `metadata` | object |  | Additional metadata key-value pairs |

### `kafka_message_sample`

Statistical sampling and profiling of messages on a topic. Provides schema distribution, message size histograms, key cardinality analysis, and throughput profiling without consuming entire topics.

**Source:** `register_msgsampling.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `topic` | string |  | Topic name to sample (required for sample action) |
| `sample_size` | number |  | Number of messages to sample (default: 100, max: 1000) |
| `cluster` | string |  | Cluster name for multi-cluster setups. |

### `kafka_migrate_execute`

Execute a migration plan. Supports dry-run mode, parallel execution, and progress tracking.

**Source:** `register_features.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `dry_run` | boolean |  | If true, validate without making changes. Default: false |

### `kafka_migrate_rollback`

Rollback a completed or failed migration.

**Source:** `register_features.go`

### `kafka_migrate_status`

Get the status and progress of a migration execution.

**Source:** `register_features.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `view` | string |  | View type: progress, report. Default: progress |

### `kafka_migration_plan`

Plan and validate Kafka migrations between clusters. Actions: plan (diff clusters), validate (check plan), list (show plans).

**Source:** `register_features.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `source_cluster` | string |  | Source cluster name |
| `target_cluster` | string |  | Target cluster name |

### `kafka_negotiate_capabilities`

Negotiate agent capabilities to receive a contextual subset of available tools based on role, permissions, and conversation context. Reduces the full tool surface to 10-20 relevant tools.

**Source:** `register_capability.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `role` | string |  | Agent role: viewer, operator, admin, analyst, sre, security. Default: viewer |
| `context` | string |  | Comma-separated context hints (e.g., 'consumer lag,incident triage') |

### `kafka_nl_pipeline_build`

Build a data pipeline from a natural language description. Translates plain English into Kafka Streams/Connect configurations with filter, transform, aggregate, and routing steps.

**Source:** `register_nlpipeline.go`

### `kafka_nl_pipeline_manage`

Manage NL-generated pipelines: list, get, validate, deploy, or delete.

**Source:** `register_nlpipeline.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `pipeline_id` | string |  | Pipeline ID (required for get/validate/deploy/delete) |
| `description` | string |  | NL description (for parse action) |

### `kafka_nl_query`

Query Kafka with plain English. Semantic parser handles complex boolean filters, aggregations, time ranges, multi-topic joins. Actions: execute (default), history, suggest. Examples: 'show me all failed orders in the last hour', 'count errors in logs group by service', 'average latency for payments'.

**Source:** `register_features.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `query` | string |  | Natural language query (required for execute/suggest) |
| `action` | string |  | Action: execute, history, suggest. Default: execute |
| `cluster` | string |  | Cluster name. |
| `limit` | number |  | Max results. Default: 100 |

### `kafka_orchestration_agents`

Register an agent for orchestration or list available agents.

**Source:** `register_orchestration.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `agent_id` | string |  | Agent ID (required for register/heartbeat) |
| `name` | string |  | Agent name (for register) |
| `role` | string |  | Agent role: monitor, operator, analyst, security, pipeline, coordinator |

### `kafka_orchestration_complete_step`

Complete a step in an orchestration with results, enabling dependent steps to proceed.

**Source:** `register_orchestration.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `error` | string |  | Error message if failed |

### `kafka_orchestration_create`

Create a multi-agent orchestration that coordinates agents working together on Kafka operations with step dependencies, role-based assignment, and shared context.

**Source:** `register_orchestration.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `description` | string |  | Description of the orchestration |

### `kafka_orchestration_handoff`

Create a task handoff between agents with context and message for collaborative operations.

**Source:** `register_orchestration.go`

### `kafka_orchestration_status`

Get status of an orchestration, list all orchestrations, or get ready steps for execution.

**Source:** `register_orchestration.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `orchestration_id` | string |  | Orchestration ID (omit to list all) |
| `action` | string |  | Action: status, ready_steps, context. Default: status |

### `kafka_persona`

Manage declarative agent personas. Actions: list (all personas), get (persona details), activate (set active persona).

**Source:** `register_features.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `persona` | string |  | Persona name: sre, data-engineer, compliance, developer, admin |

### `kafka_pipeline_create`

Create a streaming function pipeline with filter/map/enrich/transform steps. Pipelines process Kafka messages through a chain of micro-functions.

**Source:** `register_pipeline.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `sink_topic` | string |  | Output topic for processed messages |

### `kafka_pipeline_execute`

Execute a pipeline on messages from the source topic. Consumes, processes through the pipeline steps, and returns the results.

**Source:** `register_pipeline.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `limit` | number |  | Max messages to process. Default: 50 |
| `cluster` | string |  | Cluster name |

### `kafka_pipeline_manage`

Manage streaming function pipelines. Actions: list, get, delete, runs (execution history).

**Source:** `register_pipeline.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `pipeline_id` | string |  | Pipeline ID |

### `kafka_platform_info`

Get information about configured Kafka-compatible platforms (Apache Kafka, Redpanda, Confluent Cloud, MSK Serverless) and their capabilities.

**Source:** `register_features.go`

### `kafka_playbook_execute`

Execute an anomaly response playbook. Triggers automated remediation steps for a detected anomaly.

**Source:** `register_playbook.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `trigger_info` | string |  | JSON string with trigger context information |

### `kafka_playbook_list`

List anomaly response playbooks. Playbooks define automated remediation steps triggered by specific anomaly patterns like high consumer lag, broker failures, or throughput anomalies.

**Source:** `register_playbook.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `trigger_type` | string |  | Filter by trigger type: high_consumer_lag, broker_down, partition_skew, throughput_drop, throughput_spike, under_replicated, disk_usage_high, schema_error |

### `kafka_playbook_manage`

Manage playbooks and executions: get status, complete steps, enable/disable, list executions.

**Source:** `register_playbook.go`

### `kafka_playbook_match`

Find playbooks matching a specific anomaly trigger type and value.

**Source:** `register_playbook.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `value` | number |  | Anomaly value to match against thresholds |

### `kafka_playground`

Sandboxed Kafka environment for safe experimentation. Create temporary topics, generate sample data, and auto-cleanup. Actions: create, produce_sample, list, cleanup.

**Source:** `register_advanced.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string |  | Playground topic name suffix (auto-prefixed with _playground_) |
| `partitions` | number |  | Partitions for playground topic. Default: 3 |
| `data_pattern` | string |  | Sample data pattern: ecommerce, iot, user_activity, logs. Default: ecommerce |
| `count` | number |  | Number of sample messages to produce. Default: 10 |
| `cluster` | string |  | Cluster name. |

### `kafka_plugin_install`

Install a plugin from the marketplace. Validates dependencies and security capabilities before installation.

**Source:** `register_marketplace.go`

### `kafka_plugin_list`

List installed plugins with their status, version, provided tools, and resources.

**Source:** `register_marketplace.go`

### `kafka_plugin_search`

Search the plugin marketplace for extensions. Find monitoring, security, analytics, integration, and utility plugins with ratings and verification status.

**Source:** `register_marketplace.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `query` | string |  | Search query (matches name, description, tags) |
| `category` | string |  | Filter by category: monitoring, security, analytics, integration, utility |
| `verified_only` | boolean |  | Only show verified plugins. Default: false |

### `kafka_policy_evaluate`

Evaluate an OPA Rego policy for an authorization decision. Returns whether the action is allowed with reasoning.

**Source:** `register_opa.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `resource_name` | string |  | Specific resource name. Default: * |

### `kafka_policy_manage`

Manage OPA Rego policies. Actions: list (show policies), register (add/update), delete (remove), export (JSON dump), decisions (recent log).

**Source:** `register_opa.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `policy_id` | string |  | Policy ID (for register/delete) |
| `name` | string |  | Policy name (for register) |
| `rego` | string |  | Rego policy rules (for register) |
| `limit` | number |  | Max decisions to return. Default: 50 |

### `kafka_policy_simulate`

Simulate a policy evaluation showing how each policy rule matches, without recording the decision.

**Source:** `register_opa.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `resource_name` | string |  | Specific resource name |

### `kafka_predict_alert`

Predictive consumer group alerting. Analyzes lag trends, consumption rates, and partition assignment patterns to forecast potential issues before they occur.

**Source:** `register_predictalert.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `group_id` | string |  | Consumer group ID (required for analyze/record) |
| `total_lag` | number |  | Current total lag (for record action) |
| `member_count` | number |  | Current member count (for record action) |
| `alert_id` | string |  | Alert ID (for acknowledge action) |
| `limit` | number |  | Max alerts to return (for alerts action, default: 50) |

### `kafka_produce`

Publish one or more messages to a Kafka topic. Returns delivery confirmation with partition, offset, and timestamp.

**Source:** `register_p0.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `serializer` | string |  | Serialization format: auto, avro, protobuf, json, string. Default: auto |
| `schema_id` | number |  | Explicit Schema Registry schema ID for serialization |
| `acks` | string |  | Acknowledgment level: 0, 1, all. Default: all |
| `cluster` | string |  | Cluster name. |

### `kafka_quality_report`

Generate a detailed data quality report for a topic including trends, SLA status, and recommendations.

**Source:** `register_dataquality.go`

### `kafka_quality_score`

Get the real-time data quality score for a Kafka topic across freshness, completeness, conformance, anomaly rate, and volume consistency.

**Source:** `register_dataquality.go`

### `kafka_query`

Accept a natural language description of desired data and translate it to a topic lookup, filter, and consume call. Example: 'last 10 failed payments'

**Source:** `register_p2.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cluster` | string |  | Cluster name. |

### `kafka_quickstart`

Guided onboarding: auto-discovers Kafka clusters, suggests configuration, and generates a step-by-step getting started guide.

**Source:** `register_onboarding.go`

### `kafka_quota_request`

Request a quota adjustment: increase, decrease, reset, or grant burst credits.

**Source:** `register_adaptiverate.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `agent_id` | string |  | Target agent ID (default: self) |
| `amount` | number |  | Adjustment amount |
| `reason` | string |  | Reason for the adjustment |

### `kafka_quota_status`

Get adaptive rate limit status for an agent or all agents. Shows current rate, dynamic quota, burst credits, utilization, and recommendations.

**Source:** `register_adaptiverate.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `agent_id` | string |  | Agent ID (omit for all agents) |

### `kafka_rag_context`

Build RAG (Retrieval-Augmented Generation) context from indexed Kafka messages. Returns relevant message fragments optimized for LLM context windows.

**Source:** `register_semantic.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `topic` | string |  | Filter to specific topic |
| `max_tokens` | number |  | Max context tokens. Default: 4000 |

### `kafka_recommend`

Get intelligent recommendations for topic optimization: detect under-replicated partitions, idle topics, consumer lag trends, partition hotspots, retention tuning, and compression analysis.

**Source:** `register_advanced.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `topic` | string |  | Specific topic to analyze (optional, analyzes all if empty) |
| `cluster` | string |  | Cluster name. |

### `kafka_remediate_dlq`

Auto-remediate DLQ messages: retry with backoff, route to correction topic, or generate fix suggestions using schema context.

**Source:** `register_p3.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `action` | string |  | Remediation action: analyze, retry, route, suggest. Default: analyze |
| `target_topic` | string |  | Target topic for routing remediation |
| `limit` | number |  | Max messages to process. Default: 100 |
| `cluster` | string |  | Cluster name. |

### `kafka_replay_create`

Create a bookmark capturing message state at the current point in time. Stores offsets, sample messages, and metadata for later comparison or replay.

**Source:** `register_replay.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string |  | Bookmark name |
| `cluster` | string |  | Cluster name |
| `tags` | object |  | Tags for categorization |
| `action` | string |  | Action: create, list, get, delete. Default: create |
| `bookmark_id` | string |  | Bookmark ID (for get/delete) |

### `kafka_replay_diff`

Compare message payloads between two bookmarks. Detects schema changes, content drift, and message volume differences.

**Source:** `register_replay.go`

### `kafka_replay_simulate`

Simulate replaying messages through a consumer group. Predicts processing impact and estimates reprocessing time.

**Source:** `register_replay.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `topic` | string |  | Topic for simulation |
| `group_id` | string |  | Consumer group ID |
| `from_bookmark` | string |  | Start from a bookmark |
| `simulation_id` | string |  | Simulation ID (for run/get) |
| `name` | string |  | Simulation name |

### `kafka_replication_lag`

Get detailed partition-level replication lag for a specific replication link.

**Source:** `register_replication.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `topic` | string |  | Filter by specific topic |

### `kafka_replication_status`

Get real-time cross-cluster replication status for MirrorMaker2/Confluent Replicator links. Shows active links, topic-level sync state, and recent alerts.

**Source:** `register_replication.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `source_cluster` | string |  | Filter by source cluster |
| `target_cluster` | string |  | Filter by target cluster |

### `kafka_reset_offsets`

Reset consumer group offsets. Supports earliest, latest, or timestamp strategies with dry-run mode.

**Source:** `register_p1.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `value` | string |  | Timestamp value for 'timestamp' strategy (ISO 8601) |
| `dry_run` | boolean |  | Preview without applying. Default: false |
| `cluster` | string |  | Cluster name. |

### `kafka_restart_connector`

Restart a Kafka Connect connector or specific tasks. Supports bulk restart of failed tasks.

**Source:** `register_advanced.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `task_id` | number |  | Specific task ID to restart (-1 for all). Default: -1 |
| `failed_only` | boolean |  | Only restart failed tasks. Default: false |
| `cluster` | string |  | Cluster name. |

### `kafka_retention_approve`

Approve a pending retention change request.

**Source:** `register_retention.go`

### `kafka_retention_change_request`

Request a retention change with approval workflow. Reducing retention requires approval and grace period.

**Source:** `register_retention.go`

### `kafka_retention_policy`

Get or set the intelligent retention policy for a topic, including compliance constraints (GDPR, HIPAA, SOX).

**Source:** `register_retention.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `retention_ms` | number | ✓ | Current retention in milliseconds (for setting policy) |
| `compliance` | string |  | Compliance framework: GDPR, HIPAA, SOX, PCI_DSS, none |
| `criticality` | string |  | Topic criticality: low, medium, high, critical |

### `kafka_retention_recommendations`

Get AI-powered retention recommendations based on usage patterns, compliance, and cost analysis.

**Source:** `register_retention.go`

### `kafka_runbook`

Automated runbook executor for multi-step operational procedures (partition reassignment, rolling restart verification, topic migration). Defines and runs auditable workflows.

**Source:** `register_runbook.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `id` | string |  | Runbook ID or execution ID |
| `category` | string |  | Filter runbooks by category (for list) |
| `definition` | string |  | Runbook definition as JSON (for create) |
| `params` | object |  | Parameters for runbook execution (for execute) |
| `step_index` | number |  | Step index to complete (for complete_step) |
| `step_status` | string |  | Step completion status: completed, failed, skipped (for complete_step) |
| `step_output` | string |  | Step output as JSON string (for complete_step) |
| `step_error` | string |  | Step error message (for complete_step) |

### `kafka_sandbox_create`

Create an isolated sandbox environment for safe experimentation with Kafka operations. Sandboxes have resource quotas and auto-expire.

**Source:** `register_sandbox.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `description` | string |  | Sandbox description |
| `duration_sec` | number |  | Sandbox duration in seconds (default: 3600, max: 86400) |
| `quotas` | object |  | Resource quotas: {max_topics, max_partitions, max_messages, max_storage_bytes} |

### `kafka_sandbox_destroy`

Destroy a sandbox, list sandboxes, get details, or generate a promotion report.

**Source:** `register_sandbox.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `sandbox_id` | string |  | Sandbox ID |

### `kafka_sandbox_exec`

Execute a Kafka tool within a sandbox. All operations are tracked with resource accounting.

**Source:** `register_sandbox.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `args` | object |  | Tool arguments |

### `kafka_schema_advisor_analyze`

Analyze schema evolution history and provide health score, patterns, best practices, and antipattern detection.

**Source:** `register_schema_advisor.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `subject` | string |  | Schema subject (omit to list all subjects) |

### `kafka_schema_advisor_migrate`

Generate a step-by-step migration plan for evolving a schema between versions.

**Source:** `register_schema_advisor.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `from_version` | number |  | Source version number |
| `to_version` | number |  | Target version number |

### `kafka_schema_advisor_predict`

Predict compatibility issues for a proposed schema change. Analyzes breaking changes, risk level, and provides mitigation suggestions.

**Source:** `register_schema_advisor.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `schema_type` | string |  | Schema type: AVRO, PROTOBUF, JSON. Default: AVRO |
| `compatibility` | string |  | Compatibility level: BACKWARD, FORWARD, FULL, NONE. Default: BACKWARD |

### `kafka_schema_apply`

Apply, review, or rollback a schema proposal. Actions: review (approve/reject), apply (register schema), rollback (revert), list (show proposals), get (proposal details), timeline (version history).

**Source:** `register_schema_workflow.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `proposal_id` | string |  | Proposal ID (required for review/apply/rollback/get) |
| `review_action` | string |  | Review action: approve, reject (required for review) |
| `note` | string |  | Review note or comment |
| `status_filter` | string |  | Filter proposals by status: pending, approved, rejected, applied |
| `subject` | string |  | Subject for timeline action |
| `cluster` | string |  | Cluster name. |

### `kafka_schema_diff`

Compute a field-by-field diff between two schema versions showing additions, removals, and changes.

**Source:** `register_p3.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `version_to` | string |  | Target version number. Default: 'latest' |

### `kafka_schema_playground`

Interactive schema playground for drafting, testing, and validating schemas. Supports draft lifecycle: create → test → validate → promote.

**Source:** `register_schema_playground.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `draft_id` | string |  | Draft ID (for update/test/validate/promote/get/evolve) |
| `subject` | string |  | Schema subject (for create) |
| `schema_type` | string |  | Schema type: AVRO, PROTOBUF, JSON (default: AVRO) |
| `schema` | string |  | Schema definition (for create/update) |
| `message` | string |  | Sample message JSON to test against schema (for test) |
| `against_schema` | string |  | Schema to validate compatibility against (for validate/evolve) |
| `compatibility` | string |  | Compatibility level: BACKWARD, FORWARD, FULL (for validate) |
| `status_filter` | string |  | Filter drafts by status (for list) |

### `kafka_schema_propose`

Propose a schema change with auto-generated migration plan, compatibility validation, and consumer impact analysis. Creates an approval workflow.

**Source:** `register_schema_workflow.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `schema_type` | string |  | Schema type: AVRO, JSON, PROTOBUF. Default: AVRO |
| `cluster` | string |  | Cluster name. |

### `kafka_search`

Search/filter messages in a topic by key, header, value content, or time range. Bounded scan.

**Source:** `register_p1.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `key_filter` | string | ✓ | Filter messages where key contains this string |
| `header_key` | string |  | Filter messages that have this header key |
| `header_value` | string |  | Filter messages where the header matches this value |
| `value_contains` | string |  | Filter messages where value contains this string |
| `start_time` | string |  | Start time (ISO 8601) for time range filter |
| `end_time` | string |  | End time (ISO 8601) for time range filter |
| `max_scan` | number |  | Max messages to scan. Default: 10000 |
| `limit` | number |  | Max matching messages to return. Default: 10 |
| `cluster` | string |  | Cluster name. |

### `kafka_semantic_index`

Index Kafka messages for semantic search. Consumes recent messages from a topic and adds them to the vector index for meaning-based retrieval.

**Source:** `register_semantic.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `limit` | number |  | Max messages to index. Default: 100 |
| `cluster` | string |  | Cluster name |

### `kafka_semantic_search`

Search indexed Kafka messages by meaning using semantic similarity. Returns the most relevant messages matching the query intent.

**Source:** `register_semantic.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `topic` | string |  | Filter results to a specific topic |
| `limit` | number |  | Max results. Default: 10 |

### `kafka_semantic_stats`

Get statistics about the semantic search index including document counts, topic distribution, and vocabulary size.

**Source:** `register_semantic.go`

### `kafka_session_replay`

Record and replay AI agent sessions for debugging. Captures tool call traces, timing, and response payloads.

**Source:** `register_sessionreplay.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `session_id` | string |  | Session ID (for record/end/get/replay/delete) |
| `agent_id` | string |  | Agent ID filter (for list) or session owner (for start) |
| `tool` | string |  | Tool name (for record action) |
| `call_args` | object |  | Tool call arguments (for record action) |
| `call_response` | object |  | Tool call response (for record action) |
| `is_error` | boolean |  | Whether the call was an error (for record action) |
| `error_message` | string |  | Error message if call failed (for record action) |
| `duration_ms` | number |  | Call duration in milliseconds (for record action) |
| `limit` | number |  | Max results to return (for list, default: 50) |

### `kafka_sla_configure`

Configure a data quality SLA for a topic with threshold alerts.

**Source:** `register_dataquality.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `min_freshness` | number | ✓ | Minimum freshness score (0-1). Default: 0.8 |
| `min_completeness` | number |  | Minimum completeness score (0-1). Default: 0.9 |
| `min_overall` | number |  | Minimum overall score (0-1). Default: 0.7 |

### `kafka_sla_status`

Get the SLA compliance status for a topic or list all configured SLAs.

**Source:** `register_dataquality.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `topic` | string |  | Topic name (omit to list all SLAs) |

### `kafka_smart_route`

Analyze a high-level request and automatically compose a multi-tool execution plan. E.g., 'investigate why orders are delayed' triggers lag diagnosis → anomaly detection → lineage trace.

**Source:** `register_smartroute.go`

### `kafka_smart_route_status`

Get status of a smart route execution plan, list plans, get next executable steps, or record step results.

**Source:** `register_smartroute.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `plan_id` | string |  | Plan ID (required for status/next_steps/complete_step) |
| `step_id` | string |  | Step ID (required for complete_step) |
| `step_status` | string |  | Step result status: completed, failed, skipped (for complete_step) |
| `error` | string |  | Error message if step failed |

### `kafka_sql_execute`

Execute a streaming SQL query against Kafka topics. Supports SELECT, WHERE, GROUP BY, aggregations (COUNT/SUM/AVG/MIN/MAX), and WINDOW (TUMBLING/SLIDING/SESSION) clauses.

**Source:** `register_streamsql.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `limit` | number |  | Max messages to process (default: 100) |
| `cluster` | string |  | Cluster name |

### `kafka_sql_explain`

Explain a streaming SQL query plan without executing it. Shows execution steps and estimated cost.

**Source:** `register_streamsql.go`

### `kafka_sql_manage`

Manage SQL queries. Actions: list, get, delete.

**Source:** `register_streamsql.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `query_id` | string |  | Query ID |

### `kafka_stream`

Manage streaming consumer sessions for real-time message delivery. Actions: start (create session), poll (get messages), pause/resume/stop (control), list (show sessions).

**Source:** `register_streaming.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `action` | string |  | Action: start, poll, pause, resume, stop, list. Default: start |
| `topic` | string |  | Topic name (required for start) |
| `group_id` | string |  | Consumer group ID. Auto-generated if omitted. |
| `session_id` | string |  | Session ID (required for poll/pause/resume/stop) |
| `batch_size` | number |  | Messages per batch (1-100). Default: 10 |
| `max_msg_per_sec` | number |  | Throttle: max messages/sec. Default: 1000 |
| `max_messages` | number |  | Max total messages before auto-stop. Default: 1000 |
| `timeout_ms` | number |  | Session timeout in ms (max 60000). Default: 30000 |
| `poll_timeout_ms` | number |  | Poll wait time in ms. Default: 5000 |
| `deserializer` | string |  | Deserialization: auto, json, string, bytes. Default: auto |
| `auto_commit` | boolean |  | Auto-commit offsets. Default: true |
| `cluster` | string |  | Cluster name. |

### `kafka_subscribe`

Subscribe to a Kafka topic using consumer group semantics. Polls for new messages for the specified duration and returns them as a batch. Supports auto-commit for offset management.

**Source:** `register_p1.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `group_id` | string |  | Consumer group ID. If omitted, an ephemeral group is created. |
| `auto_commit` | boolean |  | Automatically commit offsets after consuming. Default: true |
| `max_messages` | number |  | Maximum messages to return per poll (1-1000). Default: 100 |
| `timeout_ms` | number |  | How long to poll for messages in ms (max 30000). Default: 10000 |
| `deserializer` | string |  | Deserialization format: auto, json, string, bytes. Default: auto |
| `cluster` | string |  | Cluster name. |

### `kafka_suggest_rbac`

Generate recommended RBAC configuration based on your Kafka topology. Outputs ready-to-use YAML config.

**Source:** `register_onboarding.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cluster` | string |  | Cluster to analyze |

### `kafka_suggest_schema_evolution`

Given a current schema subject and a desired change description, suggest compatible evolution strategies with step-by-step instructions.

**Source:** `register_p2.go`

### `kafka_task_claim`

Claim the highest-priority pending task from a queue.

**Source:** `register_taskqueue.go`

### `kafka_task_complete`

Mark a claimed task as completed or failed.

**Source:** `register_taskqueue.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `result` | object |  | Task result data |
| `error` | string |  | Error message (for failed status) |

### `kafka_task_publish`

Publish a task to a priority queue for other agents to claim and execute.

**Source:** `register_taskqueue.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `priority` | number | ✓ | Priority: 0=critical, 1=high, 2=medium, 3=low (default: 2) |
| `payload` | object |  | Task payload data |
| `max_retries` | number |  | Max retry attempts (default: 3) |
| `deadline_sec` | number |  | Deadline in seconds from now |

### `kafka_task_status`

Get task status, queue statistics, or list tasks. Actions: get, stats, list, heartbeat.

**Source:** `register_taskqueue.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `task_id` | string |  | Task ID (for get/heartbeat) |
| `queue` | string |  | Queue name (for stats/list) |
| `filter_status` | string |  | Filter by status (for list) |

### `kafka_time_travel`

Point-in-time topic queries: snapshot state at a timestamp, reconstruct compacted topic state, or diff state between two timestamps.

**Source:** `register_advanced.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `timestamp` | string | ✓ | Point-in-time timestamp (ISO 8601). Required for snapshot. |
| `timestamp_from` | string |  | Start timestamp for diff (ISO 8601) |
| `timestamp_to` | string |  | End timestamp for diff (ISO 8601) |
| `max_keys` | number |  | Max unique keys to track. Default: 10000 |
| `cluster` | string |  | Cluster name. |

### `kafka_timeseries_analyze`

Advanced time-series analysis: Mann-Kendall trend detection, FFT seasonality analysis, outlier identification, and cross-metric correlation. Returns chart-ready JSON.

**Source:** `register_timeseries.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `action` | string |  | Action: analyze, correlate, ingest. Default: analyze |
| `metric2` | string |  | Second metric for correlation |
| `source2` | string |  | Second source for correlation |
| `since_hours` | number |  | Analysis window in hours. Default: 24 |
| `value` | number |  | Metric value (for ingest action) |

### `kafka_timeseries_baseline`

Compute a baseline from historical data and compare current metrics against it. Detects deviations and scores overall health.

**Source:** `register_timeseries_analytics.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `baseline_window` | string |  | Window for baseline computation. Default: 7d |
| `current_window` | string |  | Window for current comparison. Default: 1h |

### `kafka_timeseries_compare`

Compare time-series metrics between two windows to identify changes, degradation, or improvements over time.

**Source:** `register_timeseries_analytics.go`

### `kafka_timeseries_list`

List all tracked time-series and available analysis windows.

**Source:** `register_timeseries_analytics.go`

### `kafka_timeseries_query`

Query time-series metrics for a topic or consumer group within a configurable window (1h, 24h, 7d, 30d). Returns statistical summary, trend analysis, and percentile distribution.

**Source:** `register_timeseries_analytics.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `window` | string |  | Time window: 1h, 24h, 7d, 30d. Default: 1h |

### `kafka_topic_approve`

Approve a pending topic provisioning request.

**Source:** `register_topic_portal.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `comment` | string |  | Approval comment |

### `kafka_topic_reject`

Reject a pending topic provisioning request.

**Source:** `register_topic_portal.go`

### `kafka_topic_request`

Submit a self-service topic provisioning request with justification and approval chain.

**Source:** `register_topic_portal.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `owner` | string |  | Team or individual owning this topic |
| `partitions` | number |  | Number of partitions. Default: 1 |
| `replication_factor` | number |  | Replication factor. Default: 3 |
| `retention_ms` | number |  | Retention in milliseconds. Default: 7 days |

### `kafka_topic_request_status`

Check the status of topic provisioning requests.

**Source:** `register_topic_portal.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `request_id` | string |  | Specific request ID (omit to list all) |
| `status` | string |  | Filter by status: pending, approved, rejected, created |
| `limit` | number |  | Max results. Default: 50 |

### `kafka_topic_template`

Manage declarative topic-as-code templates. Define topic settings (partitions, replication, retention, cleanup policy) as YAML/JSON templates with apply/diff/plan commands.

**Source:** `register_topictemplate.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `template` | string |  | Template content as YAML or JSON string (for parse/save) |
| `format` | string |  | Template format: yaml, json (default: json) |
| `name` | string |  | Template name (for get/delete/diff/plan/export) |
| `cluster` | string |  | Cluster name for diff/plan operations. |

### `kafka_trace_flow`

Distributed tracing for Kafka messages: inject W3C Trace Context headers, correlate across topic chains, and visualize message flow graphs. Actions: trace (get trace by ID), record (record a span), create_context (generate trace context), list (recent traces).

**Source:** `register_tracing_flow.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `trace_id` | string |  | Trace ID (required for trace action) |
| `operation` | string |  | Operation: produce, consume, transform (for record action) |
| `topic` | string |  | Topic name (for record action) |
| `producer` | string |  | Producer ID (for record action) |
| `consumer` | string |  | Consumer ID (for record action) |
| `parent_span_id` | string |  | Parent span ID for child context |
| `partition` | number |  | Partition number |
| `offset` | number |  | Message offset |
| `duration_ms` | number |  | Operation duration in ms |
| `limit` | number |  | Max traces to list. Default: 50 |

### `kafka_trace_lineage`

Track data lineage through Kafka topics. Shows producer→topic→consumer relationships, PII annotations, and GDPR/CCPA compliance reports.

**Source:** `register_features.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `topic` | string |  | Topic name |
| `framework` | string |  | Compliance framework: GDPR, CCPA |
| `cluster` | string |  | Cluster name. |

### `kafka_trace_message`

Trace a message across topics using correlation IDs. Auto-detects correlation-id, trace-id, x-request-id headers and builds a message flow graph.

**Source:** `register_advanced.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `header_name` | string |  | Header name containing correlation ID. Default: auto-detect |
| `topics` | object |  | List of topic names to search. Default: all topics. |
| `max_scan` | number |  | Max messages to scan per topic. Default: 10000 |
| `cluster` | string |  | Cluster name. |

### `kafka_twin_simulate`

Simulate operations against a cluster digital twin without affecting production. Supports: create_topic, delete_topic, alter_partitions, alter_config, rebalance.

**Source:** `register_digitaltwin.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `snapshot_id` | string |  | Snapshot ID (for create_twin) |
| `twin_id` | string |  | Twin ID (for simulate/diff/migrate_plan/get/history) |
| `op_type` | string |  | Operation type (for simulate): create_topic, delete_topic, alter_partitions, alter_config, rebalance |
| `topic` | string |  | Topic name (for simulate) |
| `partitions` | number |  | Partition count (for simulate) |
| `replication_factor` | number |  | Replication factor (for simulate) |

### `kafka_twin_snapshot`

Capture a snapshot of the current cluster state (topics, consumer groups, brokers) for digital twin simulation. Creates an immutable baseline.

**Source:** `register_digitaltwin.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cluster` | string |  | Cluster name. Default: default cluster |

### `kafka_update_connector`

Update an existing Kafka Connect connector configuration.

**Source:** `register_advanced.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cluster` | string |  | Cluster name. |

### `kafka_usage_report`

Generate usage analytics and chargeback reports. Actions: rollup (usage summary), chargeback (cost attribution), export_csv (CSV export), export_json (JSON export).

**Source:** `register_usage.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `period` | string | ✓ | Period: hourly, daily, monthly. Default: daily |
| `days_back` | number |  | Look-back period in days. Default: 30 |

### `kafka_validate_contract`

Validate messages against data contracts, register contracts, or get compliance reports. Actions: validate, register, get, list, compliance.

**Source:** `register_features.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `topic` | string |  | Topic name |
| `contract` | object |  | Contract definition for register action |
| `message` | object |  | Message value to validate |
| `cluster` | string |  | Cluster name. |

### `kafka_vector_sync_create`

Create a sync bridge that streams Kafka topic data into a vector database (Pinecone, Weaviate, Qdrant, Chroma, Milvus) with automatic embedding generation.

**Source:** `register_vectorsync.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `embedding_model` | string |  | Embedding model: simple-hash, tfidf, openai, cohere. Default: simple-hash |
| `dimension` | number |  | Embedding dimension. Default: 384 |

### `kafka_vector_sync_manage`

Manage vector sync bridges: list, get, pause, resume, delete.

**Source:** `register_vectorsync.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `bridge_id` | string |  | Bridge ID (required for get/pause/resume/stop/delete) |

### `kafka_vector_sync_search`

Perform semantic similarity search against synced vector data from Kafka topics.

**Source:** `register_vectorsync.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `top_k` | number |  | Number of results to return. Default: 10 |

### `kafka_view_create`

Create a materialized view over a Kafka topic with projections, aggregations (count/sum/avg/min/max), windowed computations, and GROUP BY support for instant analytical queries.

**Source:** `register_viewstore.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `key_field` | string |  | Field to use as view key (default: _key) |
| `aggregation_type` | string |  | Aggregation: count, sum, avg, min, max, group_by |
| `window_type` | string |  | Window type: tumbling, sliding, session, none |
| `window_size_ms` | number |  | Window size in milliseconds |
| `group_by_fields` | object |  | Fields for GROUP BY |
| `value_fields` | object |  | Fields to project into the view |
| `filter_expr` | string |  | Filter expression |
| `description` | string |  | View description |
| `retention_ms` | number |  | Retention period in milliseconds |

### `kafka_view_query`

Query a materialized view with point lookups, range scans, or full scans. Returns instant results without re-scanning the source topic.

**Source:** `register_viewstore.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `key_exact` | string |  | Exact key lookup |
| `key_prefix` | string |  | Key prefix for range scan |
| `limit` | number |  | Max results (default: 100) |
| `offset` | number |  | Pagination offset |
| `descending` | boolean |  | Reverse sort order |

### `kafka_view_status`

Manage materialized views. Actions: list, get, delete, stats.

**Source:** `register_viewstore.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `view_id` | string |  | View ID |

### `kafka_watch`

Register or manage event-driven watch subscriptions on topic patterns. When matching messages arrive, notifications are pushed via webhooks or MCP sampling.

**Source:** `register_p3.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `action` | string |  | Action: subscribe, unsubscribe, list. Default: subscribe |
| `topic_pattern` | string |  | Glob pattern to match topics (e.g., 'orders.*', '*-dlq'). Required for subscribe. |
| `key_filter` | string |  | Filter messages where key contains this string |
| `value_filter` | string |  | Filter messages where value contains this string |
| `webhook_url` | string |  | URL to POST notifications to |
| `subscription_id` | string |  | Subscription ID (required for unsubscribe) |

### `kafka_workflow_create`

Create a reusable DAG-based workflow template for multi-step Kafka operations with conditional branching, parallel fan-out, and error recovery.

**Source:** `register_dagworkflow.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `description` | string |  | Workflow description |
| `parameters` | object |  | Array of input parameters: [{name, description, type, required, default}] |
| `tags` | object |  | Tags for categorization |

### `kafka_workflow_run`

Start a workflow execution from a template. Returns the run ID and initial ready steps for the agent to execute.

**Source:** `register_dagworkflow.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `parameters` | object |  | Input parameters for the workflow |

### `kafka_workflow_status`

Get workflow status, manage workflows, or advance a running workflow. Actions: status, list, versions, cancel, complete_step, ready_steps.

**Source:** `register_dagworkflow.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `run_id` | string |  | Run ID (for status, cancel, complete_step, ready_steps) |
| `template_id` | string |  | Template ID (for versions) |
| `step_id` | string |  | Step ID (for complete_step) |
| `step_status` | string |  | Step status: completed, failed, skipped (for complete_step) |
| `step_output` | object |  | Step output data (for complete_step) |
| `step_error` | string |  | Error message (for complete_step with failed status) |

### `kafka_workflows`

Manage pre-built workflow templates for common Kafka operations. Actions: list (browse templates), describe (get template details), execute (start a workflow).

**Source:** `register_features.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `template` | string |  | Template name (required for describe/execute) |
| `category` | string |  | Filter by category: operations, planning, development, monitoring, discovery, quality, debugging |
| `params` | object |  | Parameters for template execution |
| `topic` | string |  | Topic parameter for workflow |
| `group_id` | string |  | Consumer group parameter for workflow |
| `subject` | string |  | Schema subject parameter for workflow |

### `llm_pipeline_create`

Create an LLM training data export pipeline from Kafka topics with sampling, PII redaction, and deduplication.

**Source:** `register_llm_pipeline.go`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `format` | string |  | Output format: jsonl, csv, parquet. Default: jsonl |
| `sampling_strategy` | string |  | Sampling: random, time_based, key_based, stratified. Default: random |
| `sampling_rate` | number |  | Sampling rate 0.0-1.0. Default: 1.0 |
| `pii_redaction` | boolean |  | Enable automatic PII redaction. Default: true |
| `deduplicate` | boolean |  | Enable deduplication. Default: true |
| `max_messages` | number |  | Max messages to export. Default: 100000 |
| `destination` | string |  | Destination: local, s3, gcs. Default: local |
| `destination_path` | string |  | Output path |

### `llm_pipeline_list`

List all configured LLM training data export pipelines.

**Source:** `register_llm_pipeline.go`

### `llm_pipeline_status`

Get the status and results of an LLM training data pipeline.

**Source:** `register_llm_pipeline.go`

