package chaos

import (
	"log/slog"
	"sync"
	"testing"
	"time"
)

func TestInject_ValidExperiment(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	exp, err := e.Inject(TypeBrokerOutage, "cluster-1", []string{"topic-a"}, []string{"group-1"}, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exp.Status != StatusRunning {
		t.Errorf("expected running, got %s", exp.Status)
	}
	if exp.TargetCluster != "cluster-1" {
		t.Errorf("expected cluster-1, got %s", exp.TargetCluster)
	}
	if len(exp.AffectedItems) != 2 {
		t.Errorf("expected 2 affected items (1 topic + 1 group), got %d", len(exp.AffectedItems))
	}
}

func TestInject_EmptyCluster(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, err := e.Inject(TypeBrokerOutage, "", nil, nil, nil, nil)
	if err == nil {
		t.Fatal("expected error for empty cluster")
	}
}

func TestInject_BlastRadiusExceeded(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	guard := &SafetyGuard{MaxAffectedTopics: 2, MaxDuration: time.Minute, AutoRollbackEnabled: false}
	_, err := e.Inject(TypeBrokerOutage, "c", []string{"t1", "t2", "t3"}, nil, nil, guard)
	if err == nil {
		t.Fatal("expected blast radius error")
	}
}

func TestInject_ExcludeTopics(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	guard := &SafetyGuard{
		MaxAffectedTopics:   10,
		MaxDuration:         time.Minute,
		AutoRollbackEnabled: false,
		ExcludeTopics:       []string{"critical-topic"},
	}
	_, err := e.Inject(TypeBrokerOutage, "c", []string{"critical-topic"}, nil, nil, guard)
	if err == nil {
		t.Fatal("expected error for excluded topic")
	}
}

func TestInject_CustomSafetyGuard(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	guard := &SafetyGuard{
		MaxAffectedTopics:   10,
		MaxDuration:         time.Minute,
		AutoRollbackEnabled: false,
	}
	exp, err := e.Inject(TypeSlowConsumer, "c", []string{"t1"}, nil, nil, guard)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exp.SafetyGuard.AutoRollbackEnabled {
		t.Error("expected AutoRollbackEnabled=false from custom guard")
	}
}

func TestStatus_Found(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	exp, _ := e.Inject(TypeBrokerOutage, "c", nil, nil, nil, &SafetyGuard{MaxAffectedTopics: 5, MaxDuration: time.Minute, AutoRollbackEnabled: false})
	got, err := e.Status(exp.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.ID != exp.ID {
		t.Errorf("expected ID %s, got %s", exp.ID, got.ID)
	}
}

func TestStatus_NotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, err := e.Status("nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent experiment")
	}
}

func TestStop_RunningExperiment(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	guard := &SafetyGuard{MaxAffectedTopics: 5, MaxDuration: time.Minute, AutoRollbackEnabled: false}
	exp, _ := e.Inject(TypeBrokerOutage, "c", []string{"t1"}, nil, nil, guard)

	stopped, err := e.Stop(exp.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stopped.Status != StatusStopped {
		t.Errorf("expected stopped, got %s", stopped.Status)
	}
	if stopped.CompletedAt == "" {
		t.Error("expected CompletedAt to be set")
	}
	if stopped.HealthReport == nil {
		t.Error("expected HealthReport")
	}
	for _, item := range stopped.AffectedItems {
		if !item.Recovered {
			t.Errorf("expected all items recovered, %s was not", item.Name)
		}
	}
}

func TestStop_AlreadyStopped(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	guard := &SafetyGuard{MaxAffectedTopics: 5, MaxDuration: time.Minute, AutoRollbackEnabled: false}
	exp, _ := e.Inject(TypeBrokerOutage, "c", nil, nil, nil, guard)
	e.Stop(exp.ID)

	_, err := e.Stop(exp.ID)
	if err == nil {
		t.Fatal("expected error when stopping already-stopped experiment")
	}
}

func TestStop_NonExistent(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, err := e.Stop("nope")
	if err == nil {
		t.Fatal("expected error for non-existent experiment")
	}
}

func TestListExperiments_NoFilter(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	guard := &SafetyGuard{MaxAffectedTopics: 5, MaxDuration: time.Minute, AutoRollbackEnabled: false}
	e.Inject(TypeBrokerOutage, "c", nil, nil, nil, guard)
	e.Inject(TypeSlowConsumer, "c", nil, nil, nil, guard)

	all := e.ListExperiments("")
	if len(all) != 2 {
		t.Errorf("expected 2 experiments, got %d", len(all))
	}
}

func TestListExperiments_StatusFilter(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	guard := &SafetyGuard{MaxAffectedTopics: 5, MaxDuration: time.Minute, AutoRollbackEnabled: false}
	exp, _ := e.Inject(TypeBrokerOutage, "c", nil, nil, nil, guard)
	e.Inject(TypeSlowConsumer, "c", nil, nil, nil, guard)
	e.Stop(exp.ID)

	running := e.ListExperiments("running")
	if len(running) != 1 {
		t.Errorf("expected 1 running, got %d", len(running))
	}
	stopped := e.ListExperiments("stopped")
	if len(stopped) != 1 {
		t.Errorf("expected 1 stopped, got %d", len(stopped))
	}
}

func TestListScenarios_All(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	all := e.ListScenarios("")
	if len(all) < 5 {
		t.Errorf("expected at least 5 scenarios, got %d", len(all))
	}
}

func TestListScenarios_CategoryFilter(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	consumer := e.ListScenarios("consumer")
	for _, s := range consumer {
		if s.Category != "consumer" {
			t.Errorf("expected category consumer, got %s", s.Category)
		}
	}
	if len(consumer) < 1 {
		t.Error("expected at least 1 consumer scenario")
	}
}

func TestListScenarios_NonExistentCategory(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	result := e.ListScenarios("nonexistent")
	if len(result) != 0 {
		t.Errorf("expected 0 scenarios for nonexistent category, got %d", len(result))
	}
}

func TestGetScenario_Found(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	s, err := e.GetScenario("split-brain")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Name != "split-brain" {
		t.Errorf("expected split-brain, got %s", s.Name)
	}
}

func TestGetScenario_NotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, err := e.GetScenario("nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent scenario")
	}
}

func TestConcurrentInject(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	guard := &SafetyGuard{MaxAffectedTopics: 5, MaxDuration: time.Minute, AutoRollbackEnabled: false}

	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := e.Inject(TypeBrokerOutage, "c", nil, nil, nil, guard)
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent inject error: %v", err)
	}
	all := e.ListExperiments("")
	if len(all) != 10 {
		t.Errorf("expected 10 experiments from concurrent inject, got %d", len(all))
	}
}

func TestHealthReport_ExperimentTypes(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	guard := &SafetyGuard{MaxAffectedTopics: 5, MaxDuration: time.Minute, AutoRollbackEnabled: false}

	types := []ExperimentType{TypeBrokerOutage, TypeSlowConsumer, TypeRebalanceStorm, TypeSchemaCorruption, TypeProducerFlood}
	for _, et := range types {
		exp, err := e.Inject(et, "c", nil, nil, nil, guard)
		if err != nil {
			t.Fatalf("inject %s: %v", et, err)
		}
		stopped, err := e.Stop(exp.ID)
		if err != nil {
			t.Fatalf("stop %s: %v", et, err)
		}
		if stopped.HealthReport == nil {
			t.Errorf("expected health report for %s", et)
		}
		if len(stopped.HealthReport.Findings) == 0 {
			t.Errorf("expected findings for %s", et)
		}
	}
}
