package topictemplate

import (
	"log/slog"
	"strings"
	"testing"
)

func TestParseTemplate_JSON(t *testing.T) {
	e := NewEngine(slog.Default())
	tmpl, err := e.ParseTemplate(`{
		"spec": {"topic_name": "orders", "partitions": 6, "replication_factor": 3,
			"configs": {"retention.ms": "604800000"}}
	}`, "json")
	if err != nil {
		t.Fatal(err)
	}
	if tmpl.Spec.TopicName != "orders" {
		t.Errorf("expected topic_name orders, got %s", tmpl.Spec.TopicName)
	}
	if tmpl.Spec.Partitions != 6 {
		t.Errorf("expected 6 partitions, got %d", tmpl.Spec.Partitions)
	}
}

func TestParseTemplate_YAML(t *testing.T) {
	e := NewEngine(slog.Default())
	tmpl, err := e.ParseTemplate(`
spec:
  topic_name: events
  partitions: 12
  replication_factor: 3
  configs:
    cleanup.policy: compact
`, "yaml")
	if err != nil {
		t.Fatal(err)
	}
	if tmpl.Spec.TopicName != "events" {
		t.Errorf("expected topic_name events, got %s", tmpl.Spec.TopicName)
	}
}

func TestParseTemplate_Validation(t *testing.T) {
	e := NewEngine(slog.Default())
	_, err := e.ParseTemplate(`{"spec": {"topic_name": "", "partitions": 0}}`, "json")
	if err == nil {
		t.Error("expected validation error for empty topic_name")
	}
}

func TestComputeDiff_NewTopic(t *testing.T) {
	e := NewEngine(slog.Default())
	tmpl := &TopicTemplate{
		Metadata: TemplateMetadata{Name: "test"},
		Spec:     TopicSpec{TopicName: "new-topic", Partitions: 6, ReplicationFactor: 3},
	}
	diff := e.ComputeDiff(tmpl, false, 0, 0, nil)
	if diff.InSync {
		t.Error("expected not in sync for new topic")
	}
	if diff.Changes[0].Action != "create" {
		t.Errorf("expected create action, got %s", diff.Changes[0].Action)
	}
}

func TestComputeDiff_InSync(t *testing.T) {
	e := NewEngine(slog.Default())
	tmpl := &TopicTemplate{
		Metadata: TemplateMetadata{Name: "test"},
		Spec:     TopicSpec{TopicName: "existing", Partitions: 6, ReplicationFactor: 3, Configs: map[string]string{"retention.ms": "604800000"}},
	}
	diff := e.ComputeDiff(tmpl, true, 6, 3, map[string]string{"retention.ms": "604800000"})
	if !diff.InSync {
		t.Error("expected in sync")
	}
}

func TestComputeDiff_ConfigDrift(t *testing.T) {
	e := NewEngine(slog.Default())
	tmpl := &TopicTemplate{
		Metadata: TemplateMetadata{Name: "test"},
		Spec:     TopicSpec{TopicName: "existing", Partitions: 6, ReplicationFactor: 3, Configs: map[string]string{"retention.ms": "604800000"}},
	}
	diff := e.ComputeDiff(tmpl, true, 6, 3, map[string]string{"retention.ms": "86400000"})
	if diff.InSync {
		t.Error("expected not in sync due to config drift")
	}
}

func TestSaveAndListTemplates(t *testing.T) {
	e := NewEngine(slog.Default())
	tmpl := &TopicTemplate{
		Metadata: TemplateMetadata{Name: "test-tmpl"},
		Spec:     TopicSpec{TopicName: "my-topic", Partitions: 3, ReplicationFactor: 1},
	}
	if err := e.SaveTemplate(tmpl, "test-agent"); err != nil {
		t.Fatal(err)
	}
	templates := e.ListTemplates()
	if len(templates) != 1 {
		t.Errorf("expected 1 template, got %d", len(templates))
	}
}

func TestGetTemplate_Exists(t *testing.T) {
	e := NewEngine(slog.Default())
	tmpl := &TopicTemplate{
		Metadata: TemplateMetadata{Name: "my-tmpl"},
		Spec:     TopicSpec{TopicName: "topic1", Partitions: 3, ReplicationFactor: 1},
	}
	e.SaveTemplate(tmpl, "agent")

	got, err := e.GetTemplate("my-tmpl")
	if err != nil {
		t.Fatalf("GetTemplate: %v", err)
	}
	if got.Spec.TopicName != "topic1" {
		t.Errorf("expected topic1, got %s", got.Spec.TopicName)
	}
}

func TestGetTemplate_NotFound(t *testing.T) {
	e := NewEngine(slog.Default())
	_, err := e.GetTemplate("unknown")
	if err == nil {
		t.Error("expected error for unknown template")
	}
}

func TestDeleteTemplate_Success(t *testing.T) {
	e := NewEngine(slog.Default())
	tmpl := &TopicTemplate{
		Metadata: TemplateMetadata{Name: "del-tmpl"},
		Spec:     TopicSpec{TopicName: "topic1", Partitions: 3, ReplicationFactor: 1},
	}
	e.SaveTemplate(tmpl, "agent")

	err := e.DeleteTemplate("del-tmpl")
	if err != nil {
		t.Fatalf("DeleteTemplate: %v", err)
	}
	_, err = e.GetTemplate("del-tmpl")
	if err == nil {
		t.Error("expected error after deletion")
	}
}

func TestDeleteTemplate_NotFound(t *testing.T) {
	e := NewEngine(slog.Default())
	err := e.DeleteTemplate("unknown")
	if err == nil {
		t.Error("expected error for unknown template")
	}
}

func TestDeleteTemplate_Isolation(t *testing.T) {
	e := NewEngine(slog.Default())
	t1 := &TopicTemplate{Metadata: TemplateMetadata{Name: "tmpl-a"}, Spec: TopicSpec{TopicName: "a", Partitions: 1, ReplicationFactor: 1}}
	t2 := &TopicTemplate{Metadata: TemplateMetadata{Name: "tmpl-b"}, Spec: TopicSpec{TopicName: "b", Partitions: 1, ReplicationFactor: 1}}
	e.SaveTemplate(t1, "agent")
	e.SaveTemplate(t2, "agent")

	e.DeleteTemplate("tmpl-a")

	_, err := e.GetTemplate("tmpl-b")
	if err != nil {
		t.Error("deleting tmpl-a should not affect tmpl-b")
	}
}

func TestComputePlan_CreateAction(t *testing.T) {
	e := NewEngine(slog.Default())
	diff := &DiffResult{
		TemplateName: "test",
		TopicName:    "new-topic",
		TopicExists:  false,
		InSync:       false,
		Changes: []DiffChange{
			{Field: "topic", Action: "create"},
		},
	}
	plan := e.ComputePlan(diff)
	if plan.Action != "create" {
		t.Errorf("expected create action, got %s", plan.Action)
	}
	if !plan.DryRun {
		t.Error("ComputePlan should default to DryRun=true")
	}
}

func TestComputePlan_UpdateAction(t *testing.T) {
	e := NewEngine(slog.Default())
	diff := &DiffResult{
		TemplateName: "test",
		TopicName:    "existing",
		TopicExists:  true,
		InSync:       false,
		Changes: []DiffChange{
			{Field: "partitions", Action: "update", Expected: "6", Actual: "3"},
		},
	}
	plan := e.ComputePlan(diff)
	if plan.Action != "update" {
		t.Errorf("expected update action, got %s", plan.Action)
	}
}

func TestComputePlan_NoChange(t *testing.T) {
	e := NewEngine(slog.Default())
	diff := &DiffResult{
		TemplateName: "test",
		TopicName:    "existing",
		TopicExists:  true,
		InSync:       true,
		Changes:      []DiffChange{{Field: "*", Action: "no_change"}},
	}
	plan := e.ComputePlan(diff)
	if plan.Action != "no_change" {
		t.Errorf("expected no_change action, got %s", plan.Action)
	}
}

func TestExportTemplate_JSON(t *testing.T) {
	e := NewEngine(slog.Default())
	tmpl := &TopicTemplate{
		APIVersion: "v1",
		Kind:       "TopicTemplate",
		Metadata:   TemplateMetadata{Name: "test"},
		Spec:       TopicSpec{TopicName: "orders", Partitions: 3, ReplicationFactor: 1},
	}

	out, err := e.ExportTemplate(tmpl, "json")
	if err != nil {
		t.Fatalf("ExportTemplate JSON: %v", err)
	}
	if !strings.Contains(out, `"topic_name"`) {
		t.Error("expected JSON to contain topic_name")
	}
}

func TestExportTemplate_YAML(t *testing.T) {
	e := NewEngine(slog.Default())
	tmpl := &TopicTemplate{
		APIVersion: "v1",
		Kind:       "TopicTemplate",
		Metadata:   TemplateMetadata{Name: "test"},
		Spec:       TopicSpec{TopicName: "orders", Partitions: 3, ReplicationFactor: 1},
	}

	out, err := e.ExportTemplate(tmpl, "yaml")
	if err != nil {
		t.Fatalf("ExportTemplate YAML: %v", err)
	}
	if !strings.Contains(out, "topic_name:") {
		t.Error("expected YAML to contain topic_name:")
	}
}

func TestExportTemplate_UnknownFormat(t *testing.T) {
	e := NewEngine(slog.Default())
	tmpl := &TopicTemplate{Spec: TopicSpec{TopicName: "test"}}

	_, err := e.ExportTemplate(tmpl, "xml")
	if err == nil {
		t.Error("expected error for unknown format")
	}
}

func TestExportTemplate_EmptyConfigs(t *testing.T) {
	e := NewEngine(slog.Default())
	tmpl := &TopicTemplate{
		Metadata: TemplateMetadata{Name: "test"},
		Spec:     TopicSpec{TopicName: "orders", Partitions: 3, ReplicationFactor: 1, Configs: map[string]string{}},
	}

	_, err := e.ExportTemplate(tmpl, "json")
	if err != nil {
		t.Fatalf("ExportTemplate with empty configs: %v", err)
	}
}

func TestParseTemplate_MalformedJSON(t *testing.T) {
	e := NewEngine(slog.Default())
	_, err := e.ParseTemplate(`{invalid`, "json")
	if err == nil {
		t.Error("expected error for malformed JSON")
	}
}

func TestParseTemplate_MalformedYAML(t *testing.T) {
	e := NewEngine(slog.Default())
	_, err := e.ParseTemplate(`{invalid: [`, "yaml")
	if err == nil {
		t.Error("expected error for malformed YAML")
	}
}

func TestParseTemplate_MissingRequiredFields(t *testing.T) {
	e := NewEngine(slog.Default())
	// Missing topic_name
	_, err := e.ParseTemplate(`{"spec": {"partitions": 3, "replication_factor": 1}}`, "json")
	if err == nil {
		t.Error("expected error for missing topic_name")
	}

	// Missing partitions (0 value)
	_, err = e.ParseTemplate(`{"spec": {"topic_name": "t", "replication_factor": 1}}`, "json")
	if err == nil {
		t.Error("expected error for zero partitions")
	}
}

func TestParseTemplate_UnsupportedFormat(t *testing.T) {
	e := NewEngine(slog.Default())
	_, err := e.ParseTemplate(`data`, "xml")
	if err == nil {
		t.Error("expected error for unsupported format")
	}
}
