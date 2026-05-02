package schemaadvisor

import (
	"log/slog"
	"testing"
)

func newTestAdvisor() *Advisor {
	return NewAdvisor(slog.Default())
}

func TestRecordAndGetHistory(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()
	a.RecordVersion("user-value", 1, `{"name":"user","type":"record","fields":[]}`, "AVRO")
	a.RecordVersion("user-value", 2, `{"name":"user","type":"record","fields":[]}`, "AVRO")

	// Duplicate should be ignored
	a.RecordVersion("user-value", 1, `{"name":"user","type":"record","fields":[]}`, "AVRO")

	h, err := a.GetHistory("user-value")
	if err != nil {
		t.Fatal(err)
	}
	if len(h) != 2 {
		t.Errorf("expected 2 versions, got %d", len(h))
	}
}

func TestPredictCompatibilityNoHistory(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()
	pred := a.PredictCompatibility("new-subject", `{}`, "AVRO", CompatBackward)
	if !pred.IsCompatible {
		t.Error("first version should be compatible")
	}
	if pred.RiskLevel != "low" {
		t.Errorf("expected low risk, got %s", pred.RiskLevel)
	}
}

func TestPredictCompatibilityBreaking(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()

	v1 := `{"name":"user","type":"record","fields":[
		{"name":"id","type":"string"},
		{"name":"email","type":"string"}
	]}`
	v2 := `{"name":"user","type":"record","fields":[
		{"name":"id","type":"int"},
		{"name":"age","type":"int"}
	]}`

	a.RecordVersion("user-value", 1, v1, "AVRO")

	pred := a.PredictCompatibility("user-value", v2, "AVRO", CompatBackward)
	if pred.IsCompatible {
		t.Error("should detect breaking changes")
	}
	if len(pred.BreakingChanges) == 0 {
		t.Error("expected breaking changes")
	}
	if pred.RiskLevel == "low" {
		t.Error("expected higher risk level")
	}
}

func TestPredictCompatibilityAddOptionalField(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()

	v1 := `{"name":"user","type":"record","fields":[
		{"name":"id","type":"string"}
	]}`
	v2 := `{"name":"user","type":"record","fields":[
		{"name":"id","type":"string"},
		{"name":"name","type":"string"}
	]}`

	a.RecordVersion("user-value", 1, v1, "AVRO")
	pred := a.PredictCompatibility("user-value", v2, "AVRO", CompatBackward)
	if !pred.IsCompatible {
		t.Error("adding optional field should be compatible")
	}
}

func TestGenerateMigrationPlan(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()
	a.RecordVersion("user-value", 1, `{}`, "AVRO")
	a.RecordVersion("user-value", 2, `{}`, "AVRO")

	plan, err := a.GenerateMigrationPlan("user-value", 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) == 0 {
		t.Error("expected migration steps")
	}
	if plan.Subject != "user-value" {
		t.Errorf("expected user-value, got %s", plan.Subject)
	}

	// Invalid range
	_, err = a.GenerateMigrationPlan("user-value", 2, 1)
	if err == nil {
		t.Error("expected error for invalid version range")
	}

	// Unknown subject
	_, err = a.GenerateMigrationPlan("unknown", 1, 2)
	if err == nil {
		t.Error("expected error for unknown subject")
	}
}

func TestAnalyzeEvolution(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()

	v1 := `{"name":"user","type":"record","fields":[
		{"name":"id","type":"string"},
		{"name":"email","type":"string"}
	]}`
	v2 := `{"name":"user","type":"record","fields":[
		{"name":"id","type":"string"},
		{"name":"email","type":"string"},
		{"name":"name","type":"string"}
	]}`
	v3 := `{"name":"user","type":"record","fields":[
		{"name":"id","type":"int"},
		{"name":"name","type":"string"}
	]}`

	a.RecordVersion("user-value", 1, v1, "AVRO")
	a.RecordVersion("user-value", 2, v2, "AVRO")
	a.RecordVersion("user-value", 3, v3, "AVRO")

	advice, err := a.AnalyzeEvolution("user-value")
	if err != nil {
		t.Fatal(err)
	}
	if advice.VersionCount != 3 {
		t.Errorf("expected 3 versions, got %d", advice.VersionCount)
	}
	if len(advice.Patterns) == 0 {
		t.Error("expected evolution patterns")
	}
	if len(advice.BestPractices) == 0 {
		t.Error("expected best practices")
	}
	if advice.HealthScore >= 100 {
		t.Error("expected reduced health score due to removals/type changes")
	}
}

func TestListSubjects(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()
	a.RecordVersion("subject-a", 1, `{}`, "AVRO")
	a.RecordVersion("subject-b", 1, `{}`, "AVRO")

	subjects := a.ListSubjects()
	if len(subjects) != 2 {
		t.Errorf("expected 2 subjects, got %d", len(subjects))
	}
}
