package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestListSubjectsDetailed_AllSucceed(t *testing.T) {
	avroSchema := `{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.URL.Path == "/subjects":
			json.NewEncoder(w).Encode([]string{"user-value", "order-value", "payment-value"})

		case strings.HasSuffix(r.URL.Path, "/versions"):
			subject := strings.TrimPrefix(r.URL.Path, "/subjects/")
			subject = strings.TrimSuffix(subject, "/versions")
			versions := map[string][]int{
				"user-value":    {1, 2, 3},
				"order-value":   {1},
				"payment-value": {1, 2},
			}
			json.NewEncoder(w).Encode(versions[subject])

		case strings.Contains(r.URL.Path, "/versions/latest"):
			subject := strings.TrimPrefix(r.URL.Path, "/subjects/")
			subject = strings.TrimSuffix(subject, "/versions/latest")
			json.NewEncoder(w).Encode(map[string]any{
				"subject":    subject,
				"version":    1,
				"id":         10,
				"schemaType": "AVRO",
				"schema":     avroSchema,
			})

		case strings.HasPrefix(r.URL.Path, "/config/"):
			json.NewEncoder(w).Encode(map[string]string{"compatibilityLevel": "BACKWARD"})

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "", "", 100, 300)
	subjects, err := c.ListSubjectsDetailed(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(subjects) != 3 {
		t.Fatalf("expected 3 subjects, got %d", len(subjects))
	}

	for _, s := range subjects {
		if s.SubjectName == "" {
			t.Error("expected non-empty subject name")
		}
		if s.SchemaType != "AVRO" {
			t.Errorf("expected AVRO schema type for %s, got %q", s.SubjectName, s.SchemaType)
		}
		if s.CompatibilityLevel != "BACKWARD" {
			t.Errorf("expected BACKWARD compatibility for %s, got %q", s.SubjectName, s.CompatibilityLevel)
		}
	}

	// Verify latest version numbers
	for _, s := range subjects {
		switch s.SubjectName {
		case "user-value":
			if s.LatestVersion != 3 {
				t.Errorf("expected user-value latest version 3, got %d", s.LatestVersion)
			}
		case "order-value":
			if s.LatestVersion != 1 {
				t.Errorf("expected order-value latest version 1, got %d", s.LatestVersion)
			}
		case "payment-value":
			if s.LatestVersion != 2 {
				t.Errorf("expected payment-value latest version 2, got %d", s.LatestVersion)
			}
		}
	}
}

func TestListSubjectsDetailed_PartialFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.URL.Path == "/subjects":
			json.NewEncoder(w).Encode([]string{"good-subject", "bad-subject"})

		case strings.Contains(r.URL.Path, "good-subject/versions") && !strings.Contains(r.URL.Path, "latest"):
			json.NewEncoder(w).Encode([]int{1, 2})

		case strings.Contains(r.URL.Path, "good-subject/versions/latest"):
			json.NewEncoder(w).Encode(map[string]any{
				"subject":    "good-subject",
				"version":    2,
				"id":         5,
				"schemaType": "AVRO",
				"schema":     `{"type":"record","name":"Good","fields":[]}`,
			})

		case strings.Contains(r.URL.Path, "bad-subject/versions"):
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error_code":500}`))

		case strings.HasPrefix(r.URL.Path, "/config/"):
			json.NewEncoder(w).Encode(map[string]string{"compatibilityLevel": "FULL"})

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "", "", 100, 300)
	subjects, err := c.ListSubjectsDetailed(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(subjects) != 2 {
		t.Fatalf("expected 2 subjects (including partial), got %d", len(subjects))
	}

	// good-subject should have full info
	if subjects[0].SubjectName != "good-subject" {
		t.Errorf("expected first subject 'good-subject', got %q", subjects[0].SubjectName)
	}

	// bad-subject should still be included with partial info
	if subjects[1].SubjectName != "bad-subject" {
		t.Errorf("expected second subject 'bad-subject', got %q", subjects[1].SubjectName)
	}
}

func TestListSubjectsDetailed_EmptySubjects(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]string{})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "", "", 100, 300)
	subjects, err := c.ListSubjectsDetailed(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(subjects) != 0 {
		t.Errorf("expected 0 subjects, got %d", len(subjects))
	}
}

func TestListSubjectsDetailed_SemaphoreLimiting(t *testing.T) {
	// Create 15 subjects to test the semaphore (limit: 10 concurrent)
	numSubjects := 15
	subjectNames := make([]string, numSubjects)
	for i := 0; i < numSubjects; i++ {
		subjectNames[i] = fmt.Sprintf("subject-%d", i)
	}

	var maxConcurrent atomic.Int32
	var currentConcurrent atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.URL.Path == "/subjects":
			json.NewEncoder(w).Encode(subjectNames)

		case strings.HasSuffix(r.URL.Path, "/versions") && !strings.Contains(r.URL.Path, "latest"):
			cur := currentConcurrent.Add(1)
			// Track max concurrency
			for {
				old := maxConcurrent.Load()
				if cur <= old || maxConcurrent.CompareAndSwap(old, cur) {
					break
				}
			}
			json.NewEncoder(w).Encode([]int{1})
			currentConcurrent.Add(-1)

		case strings.Contains(r.URL.Path, "/versions/latest"):
			json.NewEncoder(w).Encode(map[string]any{
				"subject": "test", "version": 1, "id": 1,
				"schemaType": "AVRO", "schema": `{"type":"string"}`,
			})

		case strings.HasPrefix(r.URL.Path, "/config/"):
			json.NewEncoder(w).Encode(map[string]string{"compatibilityLevel": "NONE"})

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "", "", 100, 300)
	subjects, err := c.ListSubjectsDetailed(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(subjects) != numSubjects {
		t.Errorf("expected %d subjects, got %d", numSubjects, len(subjects))
	}

	// Verify semaphore limited concurrency (should be ≤ 10)
	if max := maxConcurrent.Load(); max > 10 {
		t.Errorf("expected max concurrency ≤ 10, got %d", max)
	}
}

func TestListSubjectsDetailed_CompatibilityParsing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.URL.Path == "/subjects":
			json.NewEncoder(w).Encode([]string{"test-subject"})

		case strings.HasSuffix(r.URL.Path, "/versions") && !strings.Contains(r.URL.Path, "latest"):
			json.NewEncoder(w).Encode([]int{1})

		case strings.Contains(r.URL.Path, "/versions/latest"):
			json.NewEncoder(w).Encode(map[string]any{
				"subject":    "test-subject",
				"version":    1,
				"id":         1,
				"schemaType": "PROTOBUF",
				"schema":     `syntax = "proto3"; message Test {}`,
			})

		case strings.HasPrefix(r.URL.Path, "/config/"):
			json.NewEncoder(w).Encode(map[string]string{"compatibilityLevel": "FULL_TRANSITIVE"})

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "", "", 100, 300)
	subjects, err := c.ListSubjectsDetailed(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(subjects) != 1 {
		t.Fatalf("expected 1 subject, got %d", len(subjects))
	}
	if subjects[0].SchemaType != "PROTOBUF" {
		t.Errorf("expected PROTOBUF, got %q", subjects[0].SchemaType)
	}
	if subjects[0].CompatibilityLevel != "FULL_TRANSITIVE" {
		t.Errorf("expected FULL_TRANSITIVE, got %q", subjects[0].CompatibilityLevel)
	}
}

func TestListSubjectsDetailed_ListError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "", "", 100, 300)
	_, err := c.ListSubjectsDetailed(context.Background())
	if err == nil {
		t.Fatal("expected error when ListSubjects fails")
	}
}
