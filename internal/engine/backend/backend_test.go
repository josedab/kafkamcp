package backend_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/engine/backend"
)

type factory struct {
	name string
	open func(t *testing.T) backend.Backend
}

func backends(t *testing.T) []factory {
	t.Helper()
	return []factory{
		{
			name: "inmem",
			open: func(t *testing.T) backend.Backend {
				return backend.NewInMemory()
			},
		},
		{
			name: "sqlite",
			open: func(t *testing.T) backend.Backend {
				path := filepath.Join(t.TempDir(), "test.db")
				be, err := backend.OpenSQLite(backend.SQLiteOptions{Path: path})
				if err != nil {
					t.Fatalf("OpenSQLite: %v", err)
				}
				t.Cleanup(func() { _ = be.Close() })
				return be
			},
		},
	}
}

func TestBackend_SetGetDelete(t *testing.T) {
	ctx := context.Background()
	for _, f := range backends(t) {
		t.Run(f.name, func(t *testing.T) {
			be := f.open(t)
			defer be.Close()

			if _, err := be.Get(ctx, "ns", "missing"); !errors.Is(err, backend.ErrNotFound) {
				t.Fatalf("Get on missing key: want ErrNotFound, got %v", err)
			}
			if err := be.Set(ctx, "ns", "k1", []byte("hello")); err != nil {
				t.Fatalf("Set: %v", err)
			}
			got, err := be.Get(ctx, "ns", "k1")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if string(got) != "hello" {
				t.Fatalf("Get returned %q, want hello", got)
			}

			// Overwrite.
			if err := be.Set(ctx, "ns", "k1", []byte("world")); err != nil {
				t.Fatalf("Set overwrite: %v", err)
			}
			got, _ = be.Get(ctx, "ns", "k1")
			if string(got) != "world" {
				t.Fatalf("Get after overwrite returned %q, want world", got)
			}

			// Delete is idempotent.
			if err := be.Delete(ctx, "ns", "k1"); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			if err := be.Delete(ctx, "ns", "k1"); err != nil {
				t.Fatalf("Delete (idempotent): %v", err)
			}
			if _, err := be.Get(ctx, "ns", "k1"); !errors.Is(err, backend.ErrNotFound) {
				t.Fatalf("Get after Delete: want ErrNotFound, got %v", err)
			}
		})
	}
}

func TestBackend_List(t *testing.T) {
	ctx := context.Background()
	for _, f := range backends(t) {
		t.Run(f.name, func(t *testing.T) {
			be := f.open(t)
			defer be.Close()

			seed := map[string]string{
				"agent/alice/profile":   "a1",
				"agent/alice/quota":     "a2",
				"agent/bob/profile":     "b1",
				"agent/charlie/profile": "c1",
			}
			for k, v := range seed {
				if err := be.Set(ctx, "ns", k, []byte(v)); err != nil {
					t.Fatalf("Set %s: %v", k, err)
				}
			}

			// Other namespace must not leak.
			if err := be.Set(ctx, "other", "agent/alice/profile", []byte("x")); err != nil {
				t.Fatalf("Set in other namespace: %v", err)
			}

			entries, err := be.List(ctx, "ns", "agent/alice/")
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if len(entries) != 2 {
				t.Fatalf("List with prefix returned %d entries, want 2: %#v", len(entries), entries)
			}
			for _, e := range entries {
				if got, want := string(e.Value), seed[e.Key]; got != want {
					t.Fatalf("List entry %s: got %s, want %s", e.Key, got, want)
				}
				if e.Namespace != "ns" {
					t.Fatalf("List entry namespace=%s, want ns", e.Namespace)
				}
			}

			// Empty prefix returns everything in the namespace.
			all, err := be.List(ctx, "ns", "")
			if err != nil {
				t.Fatalf("List all: %v", err)
			}
			if len(all) != len(seed) {
				t.Fatalf("List all returned %d entries, want %d", len(all), len(seed))
			}
		})
	}
}

func TestBackend_Closed(t *testing.T) {
	ctx := context.Background()
	for _, f := range backends(t) {
		t.Run(f.name, func(t *testing.T) {
			be := f.open(t)
			if err := be.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if _, err := be.Get(ctx, "ns", "k"); !errors.Is(err, backend.ErrClosed) {
				t.Fatalf("Get on closed: want ErrClosed, got %v", err)
			}
			if err := be.Set(ctx, "ns", "k", []byte("v")); !errors.Is(err, backend.ErrClosed) {
				t.Fatalf("Set on closed: want ErrClosed, got %v", err)
			}
			if err := be.Delete(ctx, "ns", "k"); !errors.Is(err, backend.ErrClosed) {
				t.Fatalf("Delete on closed: want ErrClosed, got %v", err)
			}
			if _, err := be.List(ctx, "ns", ""); !errors.Is(err, backend.ErrClosed) {
				t.Fatalf("List on closed: want ErrClosed, got %v", err)
			}
		})
	}
}

func TestSQLite_PathValidation(t *testing.T) {
	if _, err := backend.OpenSQLite(backend.SQLiteOptions{Path: ""}); err == nil {
		t.Fatal("OpenSQLite(\"\") expected error")
	}
	if _, err := backend.OpenSQLite(backend.SQLiteOptions{Path: "foo/../bar"}); err == nil {
		t.Fatal("OpenSQLite with parent traversal expected error")
	}
}

func TestSQLite_PersistsAcrossOpen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "persist.db")

	be1, err := backend.OpenSQLite(backend.SQLiteOptions{Path: path})
	if err != nil {
		t.Fatalf("open #1: %v", err)
	}
	if err := be1.Set(ctx, "agentmemory", "alice/profile", []byte("hello")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := be1.Close(); err != nil {
		t.Fatalf("close #1: %v", err)
	}

	be2, err := backend.OpenSQLite(backend.SQLiteOptions{Path: path})
	if err != nil {
		t.Fatalf("open #2: %v", err)
	}
	defer be2.Close()
	got, err := be2.Get(ctx, "agentmemory", "alice/profile")
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("Get after reopen returned %q, want hello", got)
	}
}
