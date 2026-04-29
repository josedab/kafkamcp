package devmode

import (
	"log/slog"
	"testing"
)

func TestGetDevEnvironment(t *testing.T) {
	e := NewEngine(slog.Default())
	env := e.GetDevEnvironment()

	if env.Status != "ready" {
		t.Errorf("expected ready, got %s", env.Status)
	}
	if len(env.SampleTopics) != 5 {
		t.Errorf("expected 5 topics, got %d", len(env.SampleTopics))
	}
	if len(env.QuickStart) == 0 {
		t.Error("expected quick start steps")
	}
}

func TestGenerateSampleData(t *testing.T) {
	e := NewEngine(slog.Default())

	tests := []struct {
		generator string
		topic     string
	}{
		{"ecommerce-orders", "orders"},
		{"user-events", "user-events"},
		{"iot-sensors", "iot-sensors"},
		{"click-stream", "click-stream"},
		{"payment-events", "payments"},
	}

	for _, tt := range tests {
		msgs := e.GenerateSampleData(tt.generator, 10)
		if len(msgs) != 10 {
			t.Errorf("generator=%s: expected 10 messages, got %d", tt.generator, len(msgs))
		}
		if msgs[0].Topic != tt.topic {
			t.Errorf("generator=%s: expected topic %s, got %s", tt.generator, tt.topic, msgs[0].Topic)
		}
	}
}

func TestGenerateMaxLimit(t *testing.T) {
	e := NewEngine(slog.Default())
	msgs := e.GenerateSampleData("ecommerce-orders", 200)
	if len(msgs) > 100 {
		t.Errorf("expected max 100 messages, got %d", len(msgs))
	}
}

func TestTutorials(t *testing.T) {
	e := NewEngine(slog.Default())

	all := e.ListTutorials("")
	if len(all) != 5 {
		t.Fatalf("expected 5 tutorials, got %d", len(all))
	}

	beginners := e.ListTutorials("beginner")
	if len(beginners) != 1 {
		t.Fatalf("expected 1 beginner tutorial, got %d", len(beginners))
	}

	tut, err := e.GetTutorial("getting-started")
	if err != nil {
		t.Fatalf("GetTutorial failed: %v", err)
	}
	if len(tut.Steps) < 3 {
		t.Error("expected multiple steps")
	}
}
