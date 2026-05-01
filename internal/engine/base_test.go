package engine

import (
	"sort"
	"testing"
)

type testItem struct {
	Name  string
	Value int
}

func TestNewStore(t *testing.T) {
	s := NewStore[testItem]()
	if s.Count() != 0 {
		t.Errorf("expected empty store, got %d items", s.Count())
	}
}

func TestSetAndGet(t *testing.T) {
	s := NewStore[testItem]()
	item := &testItem{Name: "a", Value: 1}
	s.Set("a", item)

	got, ok := s.Get("a")
	if !ok {
		t.Fatal("expected item to exist")
	}
	if got.Name != "a" || got.Value != 1 {
		t.Errorf("unexpected item: %+v", got)
	}
}

func TestGetMissing(t *testing.T) {
	s := NewStore[testItem]()
	_, ok := s.Get("missing")
	if ok {
		t.Fatal("expected item not to exist")
	}
}

func TestMustGet(t *testing.T) {
	s := NewStore[testItem]()
	s.Set("a", &testItem{Name: "a"})

	if _, err := s.MustGet("a"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := s.MustGet("missing"); err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestCreate(t *testing.T) {
	s := NewStore[testItem]()
	if err := s.Create("a", &testItem{Name: "a"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.Create("a", &testItem{Name: "a2"}); err == nil {
		t.Fatal("expected error for duplicate key")
	}
}

func TestDelete(t *testing.T) {
	s := NewStore[testItem]()
	s.Set("a", &testItem{Name: "a"})

	if !s.Delete("a") {
		t.Fatal("expected Delete to return true")
	}
	if s.Delete("a") {
		t.Fatal("expected Delete to return false for missing key")
	}
}

func TestMustDelete(t *testing.T) {
	s := NewStore[testItem]()
	s.Set("a", &testItem{Name: "a"})

	if err := s.MustDelete("a"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.MustDelete("a"); err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestList(t *testing.T) {
	s := NewStore[testItem]()
	s.Set("a", &testItem{Name: "a"})
	s.Set("b", &testItem{Name: "b"})

	items := s.List()
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
}

func TestKeys(t *testing.T) {
	s := NewStore[testItem]()
	s.Set("b", &testItem{Name: "b"})
	s.Set("a", &testItem{Name: "a"})

	keys := s.Keys()
	sort.Strings(keys)
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
		t.Errorf("unexpected keys: %v", keys)
	}
}

func TestHas(t *testing.T) {
	s := NewStore[testItem]()
	s.Set("a", &testItem{Name: "a"})

	if !s.Has("a") {
		t.Fatal("expected Has to return true")
	}
	if s.Has("missing") {
		t.Fatal("expected Has to return false")
	}
}

func TestUpdate(t *testing.T) {
	s := NewStore[testItem]()
	s.Set("a", &testItem{Name: "a", Value: 1})

	err := s.Update("a", func(item *testItem) {
		item.Value = 42
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, _ := s.Get("a")
	if got.Value != 42 {
		t.Errorf("expected value 42, got %d", got.Value)
	}

	err = s.Update("missing", func(item *testItem) {})
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestForEach(t *testing.T) {
	s := NewStore[testItem]()
	s.Set("a", &testItem{Name: "a", Value: 1})
	s.Set("b", &testItem{Name: "b", Value: 2})
	s.Set("c", &testItem{Name: "c", Value: 3})

	count := 0
	s.ForEach(func(key string, item *testItem) bool {
		count++
		return true
	})
	if count != 3 {
		t.Errorf("expected 3 iterations, got %d", count)
	}
}

func TestForEachEarlyStop(t *testing.T) {
	s := NewStore[testItem]()
	s.Set("a", &testItem{Name: "a"})
	s.Set("b", &testItem{Name: "b"})

	count := 0
	s.ForEach(func(key string, item *testItem) bool {
		count++
		return false
	})
	if count != 1 {
		t.Errorf("expected 1 iteration, got %d", count)
	}
}

func TestFilter(t *testing.T) {
	s := NewStore[testItem]()
	s.Set("a", &testItem{Name: "a", Value: 10})
	s.Set("b", &testItem{Name: "b", Value: 20})
	s.Set("c", &testItem{Name: "c", Value: 5})

	result := s.Filter(func(key string, item *testItem) bool {
		return item.Value >= 10
	})
	if len(result) != 2 {
		t.Errorf("expected 2 items, got %d", len(result))
	}
}

func TestClear(t *testing.T) {
	s := NewStore[testItem]()
	s.Set("a", &testItem{Name: "a"})
	s.Set("b", &testItem{Name: "b"})

	s.Clear()
	if s.Count() != 0 {
		t.Errorf("expected empty store after clear, got %d", s.Count())
	}
}

func TestSetOverwrite(t *testing.T) {
	s := NewStore[testItem]()
	s.Set("a", &testItem{Name: "a", Value: 1})
	s.Set("a", &testItem{Name: "a", Value: 2})

	got, _ := s.Get("a")
	if got.Value != 2 {
		t.Errorf("expected overwritten value 2, got %d", got.Value)
	}
	if s.Count() != 1 {
		t.Errorf("expected count 1, got %d", s.Count())
	}
}
