package incident

import (
	"testing"
	"time"
)

func TestNewCorrelator(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	if c == nil {
		t.Fatal("NewCorrelator returned nil")
	}
	stats := c.Stats()
	if stats["total_events"].(int) != 0 {
		t.Error("expected 0 events")
	}
}

func TestIngestSingleEvent(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	result := c.IngestEvent(AnomalyEvent{
		Cluster:   "prod-1",
		Topic:     "orders",
		Type:      "consumer_lag",
		Severity:  SeverityWarning,
		Value:     10000,
		Timestamp: time.Now(),
	})
	// Single event shouldn't create incident
	if result != nil {
		t.Error("single event should not create incident")
	}
}

func TestIngestCorrelatedEvents(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	now := time.Now()

	c.IngestEvent(AnomalyEvent{
		ID: "e1", Cluster: "prod-1", Topic: "orders",
		Type: "consumer_lag", Severity: SeverityWarning,
		Timestamp: now,
	})
	result := c.IngestEvent(AnomalyEvent{
		ID: "e2", Cluster: "prod-1", Topic: "payments",
		Type: "throughput_drop", Severity: SeverityCritical,
		Timestamp: now.Add(5 * time.Second),
	})

	if result == nil {
		t.Fatal("expected incident from correlated events")
	}
	if result.Status != "active" {
		t.Errorf("Status = %v, want active", result.Status)
	}
	if len(result.Clusters) == 0 {
		t.Error("expected affected clusters")
	}
	if result.RootCause == nil {
		t.Error("expected root cause")
	}
}

func TestCrossClusterCorrelation(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	c.SetDependencies([]TopicDependency{
		{Source: "orders", Target: "orders-replica", Type: "mirrors"},
	})

	now := time.Now()
	c.IngestEvent(AnomalyEvent{
		ID: "e1", Cluster: "prod-1", Topic: "orders",
		Type: "broker_down", Severity: SeverityCritical,
		Timestamp: now,
	})
	result := c.IngestEvent(AnomalyEvent{
		ID: "e2", Cluster: "prod-2", Topic: "orders-replica",
		Type: "throughput_drop", Severity: SeverityWarning,
		Timestamp: now.Add(10 * time.Second),
	})

	if result == nil {
		t.Fatal("expected cross-cluster incident")
	}
	if len(result.Clusters) < 2 {
		t.Errorf("expected 2 clusters, got %d", len(result.Clusters))
	}
}

func TestGenerateReport(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	now := time.Now()

	c.IngestEvent(AnomalyEvent{
		ID: "e1", Cluster: "prod-1", Topic: "orders",
		Type: "consumer_lag", Severity: SeverityWarning,
		Timestamp: now,
	})
	inc := c.IngestEvent(AnomalyEvent{
		ID: "e2", Cluster: "prod-1", Topic: "payments",
		Type: "error_spike", Severity: SeverityCritical,
		Timestamp: now.Add(5 * time.Second),
	})

	if inc == nil {
		t.Fatal("expected incident")
	}

	report, err := c.GenerateReport(inc.ID)
	if err != nil {
		t.Fatalf("GenerateReport error: %v", err)
	}
	if len(report.Timeline) == 0 {
		t.Error("expected timeline entries")
	}
	if len(report.Remediation) == 0 {
		t.Error("expected remediation steps")
	}
}

func TestGenerateReportNotFound(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	_, err := c.GenerateReport("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent incident")
	}
}

func TestGetIncidents(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	incidents := c.GetIncidents(10)
	if len(incidents) != 0 {
		t.Errorf("expected 0 incidents, got %d", len(incidents))
	}
}

func TestOverlaps_SharedTopics(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	incident := &IncidentCluster{
		Events: []AnomalyEvent{
			{Cluster: "c1", Topic: "orders"},
		},
	}
	events := []AnomalyEvent{
		{Cluster: "c2", Topic: "orders"},
	}
	if !c.overlaps(incident, events) {
		t.Error("expected overlap on shared topic")
	}
}

func TestOverlaps_SharedClusters(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	incident := &IncidentCluster{
		Events: []AnomalyEvent{
			{Cluster: "c1", Topic: "orders"},
		},
	}
	events := []AnomalyEvent{
		{Cluster: "c1", Topic: "payments"},
	}
	if !c.overlaps(incident, events) {
		t.Error("expected overlap on shared cluster")
	}
}

func TestOverlaps_DisjointEvents(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	incident := &IncidentCluster{
		Events: []AnomalyEvent{
			{Cluster: "c1", Topic: "orders"},
		},
	}
	events := []AnomalyEvent{
		{Cluster: "c2", Topic: "payments"},
	}
	if c.overlaps(incident, events) {
		t.Error("expected no overlap for disjoint events")
	}
}

func TestOverlaps_EmptyTopics(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	incident := &IncidentCluster{
		Events: []AnomalyEvent{
			{Cluster: "c1", Topic: ""},
		},
	}
	events := []AnomalyEvent{
		{Cluster: "c2", Topic: ""},
	}
	// Empty topics will match via the empty string key in the map, which is expected behavior
	if !c.overlaps(incident, events) {
		t.Error("expected overlap on empty topic strings (both map to same key)")
	}
}

func TestMergeIntoIncident_AppendEvents(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	now := time.Now()
	incident := &IncidentCluster{
		Events: []AnomalyEvent{
			{ID: "e1", Cluster: "c1", Topic: "orders", Severity: SeverityInfo, Timestamp: now},
		},
	}

	events := []AnomalyEvent{
		{ID: "e2", Cluster: "c1", Topic: "payments", Severity: SeverityWarning, Timestamp: now.Add(time.Second)},
	}
	c.mergeIntoIncident(incident, events)

	if len(incident.Events) != 2 {
		t.Errorf("expected 2 events, got %d", len(incident.Events))
	}
}

func TestMergeIntoIncident_DuplicatePrevention(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	now := time.Now()
	incident := &IncidentCluster{
		Events: []AnomalyEvent{
			{ID: "e1", Cluster: "c1", Topic: "orders", Severity: SeverityInfo, Timestamp: now},
		},
	}

	events := []AnomalyEvent{
		{ID: "e1", Cluster: "c1", Topic: "orders", Severity: SeverityInfo, Timestamp: now},
	}
	c.mergeIntoIncident(incident, events)

	if len(incident.Events) != 1 {
		t.Errorf("expected 1 event (no duplicates), got %d", len(incident.Events))
	}
}

func TestMergeIntoIncident_SeverityUpgrade(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	now := time.Now()
	incident := &IncidentCluster{
		Events: []AnomalyEvent{
			{ID: "e1", Cluster: "c1", Topic: "orders", Severity: SeverityInfo, Timestamp: now},
		},
		Severity: SeverityInfo,
	}

	events := []AnomalyEvent{
		{ID: "e2", Cluster: "c1", Topic: "payments", Severity: SeverityCritical, Timestamp: now},
	}
	c.mergeIntoIncident(incident, events)

	if incident.Severity != SeverityCritical {
		t.Errorf("expected severity %s, got %s", SeverityCritical, incident.Severity)
	}
}

func TestCalculateBlastRadius_WithDependencies(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	c.SetDependencies([]TopicDependency{
		{Source: "orders", Target: "order-analytics", Type: "produces_to"},
		{Source: "orders", Target: "order-notifications", Type: "produces_to"},
	})

	incident := &IncidentCluster{
		Clusters: []string{"c1"},
		Topics:   []string{"orders"},
		Groups:   []string{"g1"},
	}

	br := c.calculateBlastRadius(incident)
	if br.ClustersAffected != 1 {
		t.Errorf("expected 1 cluster affected, got %d", br.ClustersAffected)
	}
	if len(br.DownstreamTopics) != 2 {
		t.Errorf("expected 2 downstream topics, got %d", len(br.DownstreamTopics))
	}
}

func TestCalculateBlastRadius_EmptyDependencies(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)

	incident := &IncidentCluster{
		Clusters: []string{"c1"},
		Topics:   []string{"orders"},
		Groups:   []string{},
	}

	br := c.calculateBlastRadius(incident)
	if len(br.DownstreamTopics) != 0 {
		t.Errorf("expected 0 downstream topics, got %d", len(br.DownstreamTopics))
	}
	if br.EstimatedImpact != "low" {
		t.Errorf("expected low impact, got %s", br.EstimatedImpact)
	}
}

func TestCalculateBlastRadius_HighImpact(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)

	incident := &IncidentCluster{
		Clusters: []string{"c1", "c2"},
		Topics:   []string{"t1", "t2", "t3", "t4", "t5", "t6"},
		Groups:   []string{},
	}

	br := c.calculateBlastRadius(incident)
	if br.EstimatedImpact != "high" {
		t.Errorf("expected high impact, got %s", br.EstimatedImpact)
	}
}

func TestCalculateBlastRadius_CriticalImpact(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)

	incident := &IncidentCluster{
		Clusters: []string{"c1", "c2", "c3"},
		Topics:   []string{"t1"},
		Groups:   []string{},
	}

	br := c.calculateBlastRadius(incident)
	if br.EstimatedImpact != "critical" {
		t.Errorf("expected critical impact, got %s", br.EstimatedImpact)
	}
}

func TestGenerateRemediation_BrokerComponent(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	incident := &IncidentCluster{
		RootCause: &RootCause{Component: "broker"},
	}
	steps := c.generateRemediation(incident)
	if len(steps) == 0 {
		t.Fatal("expected remediation steps for broker")
	}
}

func TestGenerateRemediation_ConsumerComponent(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	incident := &IncidentCluster{
		RootCause: &RootCause{Component: "consumer"},
	}
	steps := c.generateRemediation(incident)
	if len(steps) == 0 {
		t.Fatal("expected remediation steps for consumer")
	}
}

func TestGenerateRemediation_ProducerComponent(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	incident := &IncidentCluster{
		RootCause: &RootCause{Component: "producer"},
	}
	steps := c.generateRemediation(incident)
	if len(steps) == 0 {
		t.Fatal("expected remediation steps for producer")
	}
}

func TestGenerateRemediation_SchemaComponent(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	incident := &IncidentCluster{
		RootCause: &RootCause{Component: "schema"},
	}
	steps := c.generateRemediation(incident)
	if len(steps) == 0 {
		t.Fatal("expected remediation steps for schema")
	}
}

func TestGenerateRemediation_UnknownComponent(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	incident := &IncidentCluster{
		RootCause: &RootCause{Component: "xyz"},
	}
	steps := c.generateRemediation(incident)
	if len(steps) == 0 {
		t.Fatal("expected fallback remediation step")
	}
}

func TestGenerateRemediation_NilRootCause(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	incident := &IncidentCluster{RootCause: nil}
	steps := c.generateRemediation(incident)
	if len(steps) != 1 {
		t.Fatalf("expected 1 fallback step, got %d", len(steps))
	}
}

func TestIdentifyRootCause_SingleBrokerDown(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	now := time.Now()
	incident := &IncidentCluster{
		Events: []AnomalyEvent{
			{ID: "e1", Cluster: "c1", Type: "broker_down", Timestamp: now},
		},
		Clusters: []string{"c1"},
	}
	rc := c.identifyRootCause(incident)
	if rc == nil {
		t.Fatal("expected root cause")
	}
	if rc.Component != "broker" {
		t.Errorf("expected component broker, got %s", rc.Component)
	}
	if rc.Confidence != 0.85 {
		t.Errorf("expected confidence 0.85, got %f", rc.Confidence)
	}
}

func TestIdentifyRootCause_MultipleCandidates(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	now := time.Now()
	incident := &IncidentCluster{
		Events: []AnomalyEvent{
			{ID: "e1", Cluster: "c1", Type: "consumer_lag", Group: "g1", Timestamp: now},
			{ID: "e2", Cluster: "c1", Type: "throughput_drop", Timestamp: now.Add(time.Second)},
		},
		Clusters: []string{"c1"},
	}
	rc := c.identifyRootCause(incident)
	if rc == nil {
		t.Fatal("expected root cause")
	}
	// Earliest event is consumer_lag
	if rc.Component != "consumer" {
		t.Errorf("expected component consumer (earliest event), got %s", rc.Component)
	}
}

func TestIdentifyRootCause_NoEvents(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	incident := &IncidentCluster{Events: []AnomalyEvent{}}
	rc := c.identifyRootCause(incident)
	if rc != nil {
		t.Error("expected nil root cause for empty events")
	}
}

func TestIdentifyRootCause_UnknownType(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	now := time.Now()
	incident := &IncidentCluster{
		Events: []AnomalyEvent{
			{ID: "e1", Cluster: "c1", Type: "custom_event", Timestamp: now},
		},
		Clusters: []string{"c1"},
	}
	rc := c.identifyRootCause(incident)
	if rc == nil {
		t.Fatal("expected root cause")
	}
	if rc.Component != "unknown" {
		t.Errorf("expected component unknown, got %s", rc.Component)
	}
	if rc.Confidence != 0.5 {
		t.Errorf("expected confidence 0.5, got %f", rc.Confidence)
	}
}

func TestIdentifyRootCause_CrossClusterEvidence(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	now := time.Now()
	incident := &IncidentCluster{
		Events: []AnomalyEvent{
			{ID: "e1", Cluster: "c1", Type: "broker_down", Timestamp: now},
		},
		Clusters: []string{"c1", "c2"},
	}
	rc := c.identifyRootCause(incident)
	if rc == nil {
		t.Fatal("expected root cause")
	}
	if len(rc.Evidence) < 2 {
		t.Error("expected cross-cluster evidence entry")
	}
}
