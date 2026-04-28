package nlpipeline

import (
	"log/slog"
	"testing"
)

func newTestBuilder() *Builder {
	return NewBuilder(slog.Default())
}

func TestParseFilterIntent(t *testing.T) {
	b := newTestBuilder()

	r := b.Parse("Filter orders from orders-topic where amount > 100")
	if r.Intent != "filter" {
		t.Errorf("expected filter intent, got %s", r.Intent)
	}
	if r.SourceTopic != "orders-topic" {
		t.Errorf("expected orders-topic, got %s", r.SourceTopic)
	}
	if len(r.Filters) == 0 {
		t.Error("expected filter conditions")
	}
}

func TestParseTransformIntent(t *testing.T) {
	b := newTestBuilder()

	r := b.Parse("Transform messages from events to processed-events converting to uppercase")
	if r.Intent != "transform" {
		t.Errorf("expected transform intent, got %s", r.Intent)
	}
	if r.SourceTopic != "events" {
		t.Errorf("expected events, got %s", r.SourceTopic)
	}
	if r.SinkTopic != "processed-events" {
		t.Errorf("expected processed-events, got %s", r.SinkTopic)
	}
}

func TestParseAggregateIntent(t *testing.T) {
	b := newTestBuilder()

	r := b.Parse("Count messages from clicks group by user_id in 5 minute window")
	if r.Intent != "aggregate" {
		t.Errorf("expected aggregate intent, got %s", r.Intent)
	}
	if len(r.Aggregations) == 0 {
		t.Error("expected aggregation functions")
	}
}

func TestParseSyncIntent(t *testing.T) {
	b := newTestBuilder()

	r := b.Parse("Sync data from source-topic to destination-topic")
	if r.Intent != "sync" {
		t.Errorf("expected sync intent, got %s", r.Intent)
	}
}

func TestGenerate(t *testing.T) {
	b := newTestBuilder()

	spec, err := b.Generate("Filter orders from orders-topic where amount > 100", "agent-1")
	if err != nil {
		t.Fatal(err)
	}
	if spec.ID == "" {
		t.Error("expected non-empty ID")
	}
	if spec.Status != "draft" {
		t.Errorf("expected draft status, got %s", spec.Status)
	}
	if len(spec.Steps) == 0 {
		t.Error("expected pipeline steps")
	}
	if spec.SourceTopic != "orders-topic" {
		t.Errorf("expected orders-topic, got %s", spec.SourceTopic)
	}
}

func TestGenerateWithSyncConnectConfig(t *testing.T) {
	b := newTestBuilder()

	spec, err := b.Generate("Sync data from source-topic to target-topic", "agent")
	if err != nil {
		t.Fatal(err)
	}
	if spec.ConnectConfig == nil {
		t.Error("expected Connect config for sync pipeline")
	}
}

func TestGenerateValidation(t *testing.T) {
	b := newTestBuilder()

	if _, err := b.Generate("", "agent"); err == nil {
		t.Error("expected error for empty description")
	}
}

func TestValidateAndDeploy(t *testing.T) {
	b := newTestBuilder()

	spec, _ := b.Generate("Filter from orders where type = 'premium'", "agent")

	issues, err := b.Validate(spec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) > 0 {
		t.Logf("validation issues: %v", issues)
	}

	if err := b.Deploy(spec.ID); err != nil {
		t.Fatal(err)
	}

	got, _ := b.GetPipeline(spec.ID)
	if got.Status != "deployed" {
		t.Errorf("expected deployed, got %s", got.Status)
	}
}

func TestListAndDelete(t *testing.T) {
	b := newTestBuilder()

	b.Generate("Filter from t1", "a")
	b.Generate("Transform from t2", "a")

	all := b.ListPipelines()
	if len(all) != 2 {
		t.Errorf("expected 2 pipelines, got %d", len(all))
	}

	b.DeletePipeline(all[0].ID)
	all = b.ListPipelines()
	if len(all) != 1 {
		t.Errorf("expected 1 pipeline, got %d", len(all))
	}
}
