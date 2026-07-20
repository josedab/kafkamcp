package schema

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestListSubjectsDetailedCharacterization_CancellationAfterList(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client, err := NewClient("http://registry.test", "", "", 100, 300)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	client.httpClient.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body: &cancelAtEOFBody{
				Reader: strings.NewReader(`["first","second"]`),
				cancel: cancel,
			},
			Request: request,
		}, nil
	})

	subjects, err := client.ListSubjectsDetailed(ctx)
	if err != nil {
		t.Fatalf("ListSubjectsDetailed: %v", err)
	}
	if len(subjects) != 2 {
		t.Fatalf("subjects length = %d, want 2", len(subjects))
	}
	if subjects[0] != (Subject{}) || subjects[1] != (Subject{}) {
		t.Fatalf("canceled enrichment currently returns zero-value slots: %#v", subjects)
	}
}

func TestListSubjectsDetailedCharacterization_LatestSchemaFailureIsPartial(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch {
		case request.URL.Path == "/subjects":
			_ = json.NewEncoder(w).Encode([]string{"orders-value"})
		case request.URL.Path == "/subjects/orders-value/versions":
			_ = json.NewEncoder(w).Encode([]int{1, 2})
		case request.URL.Path == "/subjects/orders-value/versions/latest":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	client, _ := NewClient(server.URL, "", "", 100, 300)
	subjects, err := client.ListSubjectsDetailed(context.Background())
	if err != nil {
		t.Fatalf("ListSubjectsDetailed: %v", err)
	}
	want := Subject{SubjectName: "orders-value", LatestVersion: 2}
	if len(subjects) != 1 || subjects[0] != want {
		t.Fatalf("subjects = %#v, want %#v", subjects, []Subject{want})
	}
}

func TestGetSchemaCharacterization_CompatibilityFailureIsEmptyMetadata(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch {
		case request.URL.Path == "/subjects/orders-value/versions/latest":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"subject":    "orders-value",
				"version":    2,
				"id":         42,
				"schemaType": "AVRO",
				"schema":     `{"type":"record","name":"Order","fields":[]}`,
			})
		case request.URL.Path == "/config/orders-value":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	client, _ := NewClient(server.URL, "", "", 100, 300)
	info, err := client.GetSchema(context.Background(), "orders-value", "latest")
	if err != nil {
		t.Fatalf("GetSchema: %v", err)
	}
	if info.CompatibilityLevel != "" {
		t.Fatalf("compatibility level = %q, want empty string after compatibility failure", info.CompatibilityLevel)
	}
	if info.SchemaType != "AVRO" || info.Subject != "orders-value" {
		t.Fatalf("schema metadata = %#v", info)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

type cancelAtEOFBody struct {
	io.Reader
	cancel context.CancelFunc
}

func (body *cancelAtEOFBody) Read(buffer []byte) (int, error) {
	n, err := body.Reader.Read(buffer)
	if err == io.EOF {
		body.cancel()
	}
	return n, err
}

func (body *cancelAtEOFBody) Close() error {
	return nil
}
