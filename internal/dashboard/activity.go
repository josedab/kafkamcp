package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func (d *DashboardV2) handleListTailers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	d.mu.RLock()
	defer d.mu.RUnlock()

	var tailers []MessageTailer
	for _, t := range d.tailers {
		tailers = append(tailers, *t)
	}

	writeJSON(w, map[string]any{"tailers": tailers, "count": len(tailers)})
}

// HandleSSETail provides a Server-Sent Events stream for live message tailing.
func (d *DashboardV2) HandleSSETail(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "topic parameter required", http.StatusBadRequest)
		return
	}
	cluster := r.URL.Query().Get("cluster")

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	ctx := r.Context()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	var lastOffset int64 = -1

	fmt.Fprintf(w, "event: connected\ndata: {\"topic\":%q}\n\n", topic)
	flusher.Flush()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tailCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			messages, err := d.kafkaManager.Consume(tailCtx, cluster, topic, -1, "latest", 10, 3000, "")
			cancel()
			if err != nil {
				continue
			}
			for _, m := range messages {
				if m.Offset <= lastOffset {
					continue
				}
				lastOffset = m.Offset

				valStr := fmt.Sprintf("%v", m.Value)
				payload := map[string]any{
					"partition": m.Partition,
					"offset":    m.Offset,
					"key":       fmt.Sprintf("%v", m.Key),
					"timestamp": m.Timestamp,
				}
				if len(valStr) > 1024 {
					payload["value"] = valStr[:1024] + "..."
				} else {
					payload["value"] = valStr
				}

				data, err := json.Marshal(payload)
				if err != nil {
					d.logger.Error("marshaling SSE payload", "error", err)
					continue
				}
				fmt.Fprintf(w, "event: message\ndata: %s\n\n", data)
				flusher.Flush()
			}
		}
	}
}
