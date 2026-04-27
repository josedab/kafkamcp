package nlquery

import (
	"testing"
)

func FuzzParse(f *testing.F) {
	f.Add("show me all orders in the last 2 hours")
	f.Add("count errors in error-logs in the last 1 hour")
	f.Add("show me orders where status is failed")
	f.Add("average latency in events")
	f.Add("top 5 topics by throughput")
	f.Add("group messages by region")
	f.Add("")
	f.Add("show")
	f.Add("a]]]")
	f.Add("where x = y and a = b")

	topics := []string{"orders", "events", "logs", "error-logs"}

	f.Fuzz(func(t *testing.T, query string) {
		// Should never panic
		plan, err := NewEngine(nil).Parse(query, topics)
		if err != nil {
			return
		}
		if plan == nil {
			t.Error("nil plan without error")
		}
	})
}

func FuzzSemanticParse(f *testing.F) {
	f.Add("count messages in orders")
	f.Add("show where status = failed and region = us-east-1")
	f.Add("show errors in the last 2 hours")
	f.Add("top 10 errors by count")
	f.Add("sort by timestamp desc")
	f.Add("")
	f.Add("x")
	f.Add("where \" = ' OR 1=1 --")

	topics := []string{"orders", "events", "logs"}

	f.Fuzz(func(t *testing.T, query string) {
		// Should never panic
		plan, err := NewEngine(nil).SemanticParse(query, topics)
		if err != nil {
			return
		}
		if plan == nil {
			t.Error("nil plan without error")
		}
	})
}
