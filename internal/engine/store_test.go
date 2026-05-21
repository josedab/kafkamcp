package engine_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/engine"
	"github.com/kafkamcp/kafkamcp/internal/engine/backend"
)

type sampleValue struct {
	Name    string `json:"name"`
	Counter int    `json:"counter"`
}

func TestStore_RoundTrip(t *testing.T) {
	ctx := context.Background()
	be := backend.NewInMemory()
	defer be.Close()

	store := engine.NewStore[sampleValue](be, "sample")

	if _, ok, err := store.Get(ctx, "missing"); err != nil || ok {
		t.Fatalf("Get missing: ok=%v err=%v", ok, err)
	}

	if err := store.Set(ctx, "a", sampleValue{Name: "alpha", Counter: 1}); err != nil {
		t.Fatalf("Set a: %v", err)
	}
	if err := store.Set(ctx, "b", sampleValue{Name: "beta", Counter: 2}); err != nil {
		t.Fatalf("Set b: %v", err)
	}

	v, ok, err := store.Get(ctx, "a")
	if err != nil || !ok {
		t.Fatalf("Get a: ok=%v err=%v", ok, err)
	}
	if v.Name != "alpha" || v.Counter != 1 {
		t.Fatalf("Get a returned %+v", v)
	}

	all, err := store.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("List returned %d, want 2", len(all))
	}

	if err := store.Delete(ctx, "a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok, _ := store.Get(ctx, "a"); ok {
		t.Fatal("Get a after Delete: still present")
	}

	if store.Backend() != be {
		t.Fatal("Backend() did not return the underlying backend")
	}
	if store.Namespace() != "sample" {
		t.Fatalf("Namespace()=%q, want sample", store.Namespace())
	}
}

func TestOpen_DefaultsToInMemory(t *testing.T) {
	be, err := engine.Open(engine.DefaultConfig(), nil)
	if err != nil {
		t.Fatalf("Open(default): %v", err)
	}
	defer be.Close()
	if _, ok := be.(*backend.InMemory); !ok {
		t.Fatalf("Open(default) returned %T, want *backend.InMemory", be)
	}
}

func TestOpen_SQLite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "open.db")
	be, err := engine.Open(engine.Config{Backend: "sqlite", Path: path}, nil)
	if err != nil {
		t.Fatalf("Open(sqlite): %v", err)
	}
	defer be.Close()
	if _, ok := be.(*backend.SQLite); !ok {
		t.Fatalf("Open(sqlite) returned %T, want *backend.SQLite", be)
	}
}

func TestOpen_SQLiteMissingPath(t *testing.T) {
	if _, err := engine.Open(engine.Config{Backend: "sqlite"}, nil); err == nil {
		t.Fatal("Open(sqlite) without path expected error")
	}
}

func TestOpen_UnknownBackend(t *testing.T) {
	if _, err := engine.Open(engine.Config{Backend: "redis"}, nil); err == nil {
		t.Fatal("Open(redis) expected error")
	}
}
