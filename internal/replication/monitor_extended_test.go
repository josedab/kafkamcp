package replication

import (
	"testing"
)

func TestGetLagDetails(t *testing.T) {
	t.Run("link found with topics", func(t *testing.T) {
		m := NewMonitor(nil)
		m.RegisterLink("src", "dst", "mirrormaker2")
		m.UpdateTopicLag("src", "dst", "orders", 0, 1000, 900)
		m.UpdateTopicLag("src", "dst", "orders", 1, 2000, 1800)
		m.UpdateTopicLag("src", "dst", "events", 0, 500, 500)

		details, err := m.GetLagDetails("src", "dst")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(details) != 2 {
			t.Fatalf("expected 2 topics, got %d", len(details))
		}

		topicMap := make(map[string]TopicReplication)
		for _, d := range details {
			topicMap[d.Topic] = d
		}

		orders, ok := topicMap["orders"]
		if !ok {
			t.Fatal("expected orders topic")
		}
		if orders.TotalLag != 300 {
			t.Errorf("expected orders total lag 300, got %d", orders.TotalLag)
		}
		if len(orders.Partitions) != 2 {
			t.Errorf("expected 2 partitions, got %d", len(orders.Partitions))
		}

		events, ok := topicMap["events"]
		if !ok {
			t.Fatal("expected events topic")
		}
		if events.TotalLag != 0 {
			t.Errorf("expected events total lag 0, got %d", events.TotalLag)
		}
	})

	t.Run("link not found", func(t *testing.T) {
		m := NewMonitor(nil)

		_, err := m.GetLagDetails("no-src", "no-dst")
		if err == nil {
			t.Fatal("expected error for missing link")
		}
	})
}

func TestEvaluateFailoverReadiness(t *testing.T) {
	t.Run("ready - all synced", func(t *testing.T) {
		m := NewMonitor(nil)
		m.RegisterLink("src", "dst", "mirrormaker2")
		m.UpdateTopicLag("src", "dst", "topic1", 0, 1000, 1000)
		m.UpdateTopicLag("src", "dst", "topic2", 0, 500, 500)

		fr, err := m.EvaluateFailoverReadiness("src", "dst")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if fr.Status != "ready" {
			t.Errorf("expected status ready, got %s", fr.Status)
		}
		if fr.ReadinessScore != 100 {
			t.Errorf("expected score 100, got %d", fr.ReadinessScore)
		}
		if fr.MaxLag != 0 {
			t.Errorf("expected max lag 0, got %d", fr.MaxLag)
		}
		if fr.TopicsInSync != 2 {
			t.Errorf("expected 2 topics in sync, got %d", fr.TopicsInSync)
		}
		if fr.TopicsOutOfSync != 0 {
			t.Errorf("expected 0 topics out of sync, got %d", fr.TopicsOutOfSync)
		}
		if fr.EstimatedRPO != "0s (fully synced)" {
			t.Errorf("unexpected RPO: %s", fr.EstimatedRPO)
		}
	})

	t.Run("at_risk - some lag", func(t *testing.T) {
		m := NewMonitor(nil)
		m.SetThresholds(1000, 10000)
		m.RegisterLink("src", "dst", "mirrormaker2")
		// One synced, one with moderate lag above warning threshold
		m.UpdateTopicLag("src", "dst", "topic1", 0, 5000, 5000)
		m.UpdateTopicLag("src", "dst", "topic2", 0, 5000, 2000)

		fr, err := m.EvaluateFailoverReadiness("src", "dst")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if fr.Status != "at_risk" {
			t.Errorf("expected status at_risk, got %s (score=%d)", fr.Status, fr.ReadinessScore)
		}
		if fr.TopicsInSync != 1 {
			t.Errorf("expected 1 topic in sync, got %d", fr.TopicsInSync)
		}
		if fr.TopicsOutOfSync != 1 {
			t.Errorf("expected 1 topic out of sync, got %d", fr.TopicsOutOfSync)
		}
		if fr.MaxLag != 3000 {
			t.Errorf("expected max lag 3000, got %d", fr.MaxLag)
		}
	})

	t.Run("not_ready - no topics", func(t *testing.T) {
		m := NewMonitor(nil)
		m.RegisterLink("src", "dst", "mirrormaker2")

		fr, err := m.EvaluateFailoverReadiness("src", "dst")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if fr.Status != "not_ready" {
			t.Errorf("expected status not_ready, got %s", fr.Status)
		}
		if fr.ReadinessScore != 0 {
			t.Errorf("expected score 0, got %d", fr.ReadinessScore)
		}
		if fr.EstimatedRPO != "unknown" {
			t.Errorf("expected RPO unknown, got %s", fr.EstimatedRPO)
		}
		if len(fr.Recommendations) != 1 || fr.Recommendations[0] != "No topics are being replicated" {
			t.Errorf("unexpected recommendations: %v", fr.Recommendations)
		}
	})

	t.Run("link not found", func(t *testing.T) {
		m := NewMonitor(nil)

		_, err := m.EvaluateFailoverReadiness("no-src", "no-dst")
		if err == nil {
			t.Fatal("expected error for missing link")
		}
	})
}

func TestGetRecentAlerts(t *testing.T) {
	t.Run("filtered by source and target", func(t *testing.T) {
		m := NewMonitor(nil)
		m.SetThresholds(100, 500)
		m.RegisterLink("src1", "dst1", "mirrormaker2")
		m.RegisterLink("src2", "dst2", "mirrormaker2")

		m.UpdateTopicLag("src1", "dst1", "topic1", 0, 1000, 800)
		m.UpdateTopicLag("src2", "dst2", "topic2", 0, 1000, 700)

		alerts := m.GetRecentAlerts("src1", "dst1", 50)
		if len(alerts) != 1 {
			t.Fatalf("expected 1 alert, got %d", len(alerts))
		}
		if alerts[0].Source != "src1" || alerts[0].Target != "dst1" {
			t.Errorf("unexpected alert source/target: %s->%s", alerts[0].Source, alerts[0].Target)
		}
	})

	t.Run("limit", func(t *testing.T) {
		m := NewMonitor(nil)
		m.SetThresholds(10, 500)
		m.RegisterLink("src", "dst", "mirrormaker2")

		for i := int32(0); i < 10; i++ {
			m.UpdateTopicLag("src", "dst", "topic", i, 1000, 900)
		}

		alerts := m.GetRecentAlerts("src", "dst", 3)
		if len(alerts) != 3 {
			t.Fatalf("expected 3 alerts, got %d", len(alerts))
		}
	})

	t.Run("empty results", func(t *testing.T) {
		m := NewMonitor(nil)

		alerts := m.GetRecentAlerts("no-src", "no-dst", 10)
		if len(alerts) != 0 {
			t.Errorf("expected 0 alerts, got %d", len(alerts))
		}
	})

	t.Run("empty source and target returns all", func(t *testing.T) {
		m := NewMonitor(nil)
		m.SetThresholds(100, 500)
		m.RegisterLink("src1", "dst1", "mirrormaker2")
		m.RegisterLink("src2", "dst2", "mirrormaker2")

		m.UpdateTopicLag("src1", "dst1", "t1", 0, 1000, 800)
		m.UpdateTopicLag("src2", "dst2", "t2", 0, 1000, 700)

		alerts := m.GetRecentAlerts("", "", 50)
		if len(alerts) != 2 {
			t.Fatalf("expected 2 alerts, got %d", len(alerts))
		}
	})
}
