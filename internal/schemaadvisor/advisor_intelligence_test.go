package schemaadvisor

import (
	"strings"
	"testing"
	"time"
)

// --- AnalyzeIntelligent ---

func TestAnalyzeIntelligent_UnknownSubject(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()
	_, err := a.AnalyzeIntelligent("nonexistent")
	if err == nil {
		t.Fatal("expected error for unknown subject")
	}
}

func TestAnalyzeIntelligent_SingleVersion(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()
	schema := `{"name":"rec",
"type":"record","fields":[
{"name":"user_id","type":"string"},
{"name":"created_at","type":"long"}
]}`
	a.RecordVersion("single", 1, schema, "AVRO")

	intel, err := a.AnalyzeIntelligent("single")
	if err != nil {
		t.Fatal(err)
	}
	if intel.Subject != "single" {
		t.Errorf("expected subject 'single', got %q", intel.Subject)
	}
	if intel.SchemaType != "AVRO" {
		t.Errorf("expected schema type AVRO, got %q", intel.SchemaType)
	}
	if len(intel.UsagePatterns) < 2 {
		t.Errorf("expected at least 2 usage patterns, got %d", len(intel.UsagePatterns))
	}
	if !strings.Contains(intel.Prediction, "Insufficient history") {
		t.Errorf("single version should predict insufficient history, got %q", intel.Prediction)
	}
}

func TestAnalyzeIntelligent_MultiVersionFieldAdditions(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()

	v1 := `{"name":"order",
"type":"record","fields":[
{"name":"order_id","type":"string"},
{"name":"amount","type":"long"}
]}`
	v2 := `{"name":"order",
"type":"record","fields":[
{"name":"order_id","type":"string"},
{"name":"amount","type":"long"},
{"name":"created_at","type":"long"}
]}`
	v3 := `{"name":"order",
"type":"record","fields":[
{"name":"order_id","type":"string"},
{"name":"amount","type":"long"},
{"name":"created_at","type":"long"},
{"name":"updated_at","type":"long"}
]}`

	a.RecordVersion("orders", 1, v1, "AVRO")
	a.RecordVersion("orders", 2, v2, "AVRO")
	a.RecordVersion("orders", 3, v3, "AVRO")

	intel, err := a.AnalyzeIntelligent("orders")
	if err != nil {
		t.Fatal(err)
	}

	if len(intel.UsagePatterns) < 4 {
		t.Errorf("expected at least 4 usage patterns, got %d", len(intel.UsagePatterns))
	}

	patternMap := make(map[string]UsagePattern)
	for _, p := range intel.UsagePatterns {
		patternMap[p.FieldName] = p
	}

	if p, ok := patternMap["order_id"]; ok {
		if p.UsageType != "key" {
			t.Errorf("expected order_id usage 'key', got %q", p.UsageType)
		}
		if p.Stability != 1.0 {
			t.Errorf("expected order_id stability 1.0, got %f", p.Stability)
		}
	} else {
		t.Error("missing usage pattern for order_id")
	}

	if p, ok := patternMap["updated_at"]; ok {
		if p.UsageType != "timestamp" {
			t.Errorf("expected updated_at usage 'timestamp', got %q", p.UsageType)
		}
		if p.AddedVersion != 3 {
			t.Errorf("expected updated_at added in version 3, got %d", p.AddedVersion)
		}
	} else {
		t.Error("missing usage pattern for updated_at")
	}
}

func TestAnalyzeIntelligent_ComplexTypes(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()

	schema := `{"name":"evt",
"type":"record","fields":[
{"name":"data","type":"array"},
{"name":"props","type":"map"},
{"name":"nested","type":"record"},
{"name":"opt","type":"union"}
]}`
	a.RecordVersion("complex", 1, schema, "AVRO")

	intel, err := a.AnalyzeIntelligent("complex")
	if err != nil {
		t.Fatal(err)
	}

	// 4 fields*5=20 + array(10) + map(10) + record(10) + union(5) = 55
	if intel.ComplexityScore < 50 {
		t.Errorf("expected high complexity score for complex types, got %f", intel.ComplexityScore)
	}
}

// --- computeEvolutionScore (tested indirectly through AnalyzeIntelligent) ---

func TestEvolutionScore_RapidVersionGrowth(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()

	schema := `{"name":"rec",
"type":"record","fields":[
{"name":"a","type":"string"}
]}`
	for i := 1; i <= 40; i++ {
		a.RecordVersion("rapid", i, schema, "AVRO")
	}

	intel, err := a.AnalyzeIntelligent("rapid")
	if err != nil {
		t.Fatal(err)
	}

	// penalty=(40-20)*2=40, additive bonus=20 → score ~80
	if intel.EvolutionScore >= 100 {
		t.Errorf("rapid version growth should lower evolution score below 100, got %f", intel.EvolutionScore)
	}
}

func TestEvolutionScore_StableAdditiveEvolution(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()

	v1 := `{"name":"rec",
"type":"record","fields":[
{"name":"a","type":"string"}
]}`
	v2 := `{"name":"rec",
"type":"record","fields":[
{"name":"a","type":"string"},
{"name":"b","type":"string"}
]}`
	v3 := `{"name":"rec",
"type":"record","fields":[
{"name":"a","type":"string"},
{"name":"b","type":"string"},
{"name":"c","type":"string"}
]}`

	a.RecordVersion("stable-evo", 1, v1, "AVRO")
	a.RecordVersion("stable-evo", 2, v2, "AVRO")
	a.RecordVersion("stable-evo", 3, v3, "AVRO")

	intel, err := a.AnalyzeIntelligent("stable-evo")
	if err != nil {
		t.Fatal(err)
	}

	if intel.EvolutionScore < 95 {
		t.Errorf("stable additive evolution should give high score, got %f", intel.EvolutionScore)
	}
}

func TestEvolutionScore_TypeChanges(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()

	v1 := `{"name":"rec",
"type":"record","fields":[
{"name":"a","type":"string"},
{"name":"b","type":"string"}
]}`
	v2 := `{"name":"rec",
"type":"record","fields":[
{"name":"a","type":"int"},
{"name":"b","type":"int"}
]}`
	v3 := `{"name":"rec",
"type":"record","fields":[
{"name":"a","type":"long"},
{"name":"b","type":"long"}
]}`
	v4 := `{"name":"rec",
"type":"record","fields":[
{"name":"a","type":"float"},
{"name":"b","type":"float"}
]}`

	a.RecordVersion("types", 1, v1, "AVRO")
	a.RecordVersion("types", 2, v2, "AVRO")
	a.RecordVersion("types", 3, v3, "AVRO")
	a.RecordVersion("types", 4, v4, "AVRO")

	intel, err := a.AnalyzeIntelligent("types")
	if err != nil {
		t.Fatal(err)
	}

	// a: 4 types → (4-1)*10=30, b: same → 30, total penalty=60, bonus≤20 → score≤60
	if intel.EvolutionScore >= 80 {
		t.Errorf("type changes should lower evolution score, got %f", intel.EvolutionScore)
	}
}

// --- computeComplexityScore ---

func TestComputeComplexityScore_Empty(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()
	score := a.computeComplexityScore(nil)
	if score != 0 {
		t.Errorf("expected 0 for empty history, got %f", score)
	}
}

func TestComplexityScore_SimpleSchema(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()

	schema := `{"name":"rec",
"type":"record","fields":[
{"name":"a","type":"string"},
{"name":"b","type":"int"}
]}`
	a.RecordVersion("simple-cx", 1, schema, "AVRO")

	intel, err := a.AnalyzeIntelligent("simple-cx")
	if err != nil {
		t.Fatal(err)
	}

	// 2 flat fields * 5 = 10, no complex types
	if intel.ComplexityScore > 20 {
		t.Errorf("expected low complexity for simple schema, got %f", intel.ComplexityScore)
	}
}

func TestComplexityScore_ComplexSchema(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()

	schema := `{"name":"evt",
"type":"record","fields":[
{"name":"data","type":"array"},
{"name":"props","type":"map"},
{"name":"nested","type":"record"},
{"name":"opt","type":"union"}
]}`
	a.RecordVersion("complex-cx", 1, schema, "AVRO")

	intel, err := a.AnalyzeIntelligent("complex-cx")
	if err != nil {
		t.Fatal(err)
	}

	if intel.ComplexityScore < 50 {
		t.Errorf("expected high complexity for complex schema, got %f", intel.ComplexityScore)
	}
}

// --- generateRecommendations (tested indirectly through AnalyzeIntelligent) ---

func TestRecommendations_UnstableFields(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()

	// Field "data" has 3 distinct types → len(types)=3 > 2 → restructure
	v1 := `{"name":"rec",
"type":"record","fields":[
{"name":"data","type":"string"}
]}`
	v2 := `{"name":"rec",
"type":"record","fields":[
{"name":"data","type":"int"}
]}`
	v3 := `{"name":"rec",
"type":"record","fields":[
{"name":"data","type":"long"}
]}`

	a.RecordVersion("unstable", 1, v1, "AVRO")
	a.RecordVersion("unstable", 2, v2, "AVRO")
	a.RecordVersion("unstable", 3, v3, "AVRO")

	intel, err := a.AnalyzeIntelligent("unstable")
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, r := range intel.Recommendations {
		if r.Type == "restructure" && r.Field == "data" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected restructure recommendation for unstable field 'data'")
	}
}

func TestRecommendations_UndocumentedFields(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()

	schema := `{"name":"rec",
"type":"record","fields":[
{"name":"a","type":"string"},
{"name":"b","type":"string"}
]}`
	a.RecordVersion("undoc", 1, schema, "AVRO")

	intel, err := a.AnalyzeIntelligent("undoc")
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, r := range intel.Recommendations {
		if r.Type == "optimize" && strings.Contains(r.Description, "lack documentation") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected optimize recommendation for undocumented fields")
	}
}

func TestRecommendations_RequiredWithoutDefaults(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()

	// Inject directly — parser does not set Required
	a.mu.Lock()
	a.history["req-test"] = []SchemaVersion{{
		Version:    1,
		Schema:     `{}`,
		SchemaType: "AVRO",
		Subject:    "req-test",
		Timestamp:  time.Now(),
		Fields: []FieldInfo{
			{Name: "id", Type: "string", Required: true},
		},
	}}
	a.mu.Unlock()

	intel, err := a.AnalyzeIntelligent("req-test")
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, r := range intel.Recommendations {
		if r.Type == "add_field" && r.Field == "id" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected add_field recommendation for required field without default")
	}
}

func TestRecommendations_MixedNaming(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()

	schema := `{"name":"rec",
"type":"record","fields":[
{"name":"user_id","type":"string"},
{"name":"firstName","type":"string"}
]}`
	a.RecordVersion("naming", 1, schema, "AVRO")

	intel, err := a.AnalyzeIntelligent("naming")
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, r := range intel.Recommendations {
		if r.Type == "optimize" && strings.Contains(r.Description, "naming conventions") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected optimize recommendation for mixed naming conventions")
	}
}

// --- predictNextEvolution (tested indirectly through AnalyzeIntelligent) ---

func TestPrediction_SingleVersion(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()

	a.RecordVersion("pred-single", 1, `{"name":"rec",
"type":"record","fields":[
{"name":"a","type":"string"}
]}`, "AVRO")

	intel, err := a.AnalyzeIntelligent("pred-single")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(intel.Prediction, "Insufficient history") {
		t.Errorf("expected 'Insufficient history' prediction, got %q", intel.Prediction)
	}
}

func TestPrediction_RapidGrowth(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()

	v1 := `{"name":"rec",
"type":"record","fields":[
{"name":"f1","type":"string"}
]}`
	v2 := `{"name":"rec",
"type":"record","fields":[
{"name":"f1","type":"string"},
{"name":"f2","type":"string"},
{"name":"f3","type":"string"},
{"name":"f4","type":"string"}
]}`
	v3 := `{"name":"rec",
"type":"record","fields":[
{"name":"f1","type":"string"},
{"name":"f2","type":"string"},
{"name":"f3","type":"string"},
{"name":"f4","type":"string"},
{"name":"f5","type":"string"},
{"name":"f6","type":"string"},
{"name":"f7","type":"string"}
]}`

	a.RecordVersion("pred-growth", 1, v1, "AVRO")
	a.RecordVersion("pred-growth", 2, v2, "AVRO")
	a.RecordVersion("pred-growth", 3, v3, "AVRO")

	intel, err := a.AnalyzeIntelligent("pred-growth")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(intel.Prediction, "Rapid additive growth") {
		t.Errorf("expected rapid growth prediction, got %q", intel.Prediction)
	}
}

func TestPrediction_StableSchema(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()

	schema := `{"name":"rec",
"type":"record","fields":[
{"name":"a","type":"string"},
{"name":"b","type":"string"}
]}`

	a.RecordVersion("pred-stable", 1, schema, "AVRO")
	a.RecordVersion("pred-stable", 2, schema, "AVRO")
	a.RecordVersion("pred-stable", 3, schema, "AVRO")

	intel, err := a.AnalyzeIntelligent("pred-stable")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(intel.Prediction, "Stable schema") {
		t.Errorf("expected stable schema prediction, got %q", intel.Prediction)
	}
}

// --- inferFieldUsage (tested indirectly through AnalyzeIntelligent) ---

func TestAnalyzeIntelligent_FieldUsageInference(t *testing.T) {
	t.Parallel()
	a := newTestAdvisor()

	schema := `{"name":"rec",
"type":"record","fields":[
{"name":"user_id","type":"string"},
{"name":"created_at","type":"long"},
{"name":"metadata_tags","type":"string"},
{"name":"amount","type":"double"}
]}`
	a.RecordVersion("usage", 1, schema, "AVRO")

	intel, err := a.AnalyzeIntelligent("usage")
	if err != nil {
		t.Fatal(err)
	}

	patternMap := make(map[string]UsagePattern)
	for _, p := range intel.UsagePatterns {
		patternMap[p.FieldName] = p
	}

	cases := []struct {
		field    string
		expected string
	}{
		{"user_id", "key"},
		{"created_at", "timestamp"},
		{"metadata_tags", "metadata"},
		{"amount", "value"},
	}
	for _, tc := range cases {
		p, ok := patternMap[tc.field]
		if !ok {
			t.Errorf("missing usage pattern for field %q", tc.field)
			continue
		}
		if p.UsageType != tc.expected {
			t.Errorf("field %q: expected usage %q, got %q", tc.field, tc.expected, p.UsageType)
		}
	}
}
