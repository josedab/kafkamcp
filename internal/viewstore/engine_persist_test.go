package viewstore_test

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/engine/backend"
	"github.com/kafkamcp/kafkamcp/internal/viewstore"
)

func TestEngine_PersistAndRestore(t *testing.T) {
	logger := slog.Default()
	path := filepath.Join(t.TempDir(), "vs.db")

	be1, err := backend.OpenSQLite(backend.SQLiteOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	ctx := context.Background()

	e1, err := viewstore.NewEngineWithBackend(ctx, logger, be1)
	if err != nil {
		t.Fatalf("NewEngineWithBackend: %v", err)
	}
	view, err := e1.CreateView("orders_by_user", "orders", "user_id", "count", "tumbling", 60_000, 0, nil, nil, "", "user-totals", "tester")
	if err != nil {
		t.Fatalf("CreateView: %v", err)
	}
	if err := e1.IngestRecord(view.ID, "user-1", map[string]any{"amount": float64(100)}, 1); err != nil {
		t.Fatalf("IngestRecord 1: %v", err)
	}
	if err := e1.IngestRecord(view.ID, "user-2", map[string]any{"amount": float64(50)}, 2); err != nil {
		t.Fatalf("IngestRecord 2: %v", err)
	}
	if err := be1.Close(); err != nil {
		t.Fatalf("close #1: %v", err)
	}

	// Reopen and verify state survived.
	be2, err := backend.OpenSQLite(backend.SQLiteOptions{Path: path})
	if err != nil {
		t.Fatalf("OpenSQLite (reopen): %v", err)
	}
	defer be2.Close()
	e2, err := viewstore.NewEngineWithBackend(ctx, logger, be2)
	if err != nil {
		t.Fatalf("NewEngineWithBackend (reopen): %v", err)
	}

	got, err := e2.GetView(view.ID)
	if err != nil {
		t.Fatalf("GetView after restore: %v", err)
	}
	if got.Name != "orders_by_user" {
		t.Fatalf("GetView returned name %q, want orders_by_user", got.Name)
	}

	result, err := e2.Query(view.ID, viewstore.QueryOptions{Limit: 10})
	if err != nil {
		t.Fatalf("Query after restore: %v", err)
	}
	if len(result.Records) != 2 {
		t.Fatalf("Query returned %d records, want 2", len(result.Records))
	}

	// Deleting the view should also remove its persisted records.
	if err := e2.DeleteView(view.ID); err != nil {
		t.Fatalf("DeleteView: %v", err)
	}
	if _, err := be2.Get(ctx, "viewstore.definitions", view.ID); err != backend.ErrNotFound {
		t.Fatalf("definition still present after DeleteView: err=%v", err)
	}
}

func TestEngine_NilBackendIsLegacy(t *testing.T) {
	e, err := viewstore.NewEngineWithBackend(context.Background(), slog.Default(), nil)
	if err != nil {
		t.Fatalf("NewEngineWithBackend(nil): %v", err)
	}
	if _, err := e.CreateView("v", "t", "k", "", "", 0, 0, nil, nil, "", "", "tester"); err != nil {
		t.Fatalf("CreateView: %v", err)
	}
}
