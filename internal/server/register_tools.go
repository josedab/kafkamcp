package server

// registerTools registers exactly the supported production tool surface: the
// union of the core-read, ops-write and governance packs (29 tools). Every
// register function below contributes only supported tools; the tool-pack
// filter then narrows the surface to the operator's configured packs.
func (s *Server) registerTools() {
	// Read-only core (core-read pack).
	s.registerP0Tools()                // list/describe topics, consume, produce, schemas, consumer groups
	s.registerP1Tools()                // create/alter/delete topic, search, reset offsets, analyze DLQ
	s.registerCoreObservabilityTools() // health, metrics
	s.registerFeatureTools()           // diagnose lag
	s.registerOnboardingTools()        // discover clusters
	s.registerCapabilityTools()        // discover tools

	// Mutating operations (ops-write pack), each approval-gated.
	s.registerAdvancedTools() // connector CRUD

	// Governance (governance + ops-write packs).
	s.registerApprovalTools() // approval lifecycle + governance decision
	s.registerOPATools()      // policy evaluate/simulate
	s.registerACLMgmtTools()  // ACL inspect/reconcile
}
