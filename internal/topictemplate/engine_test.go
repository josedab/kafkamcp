package topictemplate

import (
	"log/slog"
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
