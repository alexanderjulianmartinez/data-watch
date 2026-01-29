package debezium

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alexanderjulianmartinez/data-watch/internal/config"
)

func TestConnectorHealthDetection(t *testing.T) {
	// Test server responding to connector list, connector config and status
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/connectors/":
			json.NewEncoder(w).Encode([]string{"foo"})
		case "/connectors/foo":
			json.NewEncoder(w).Encode(map[string]map[string]string{"config": {"snapshot.mode": "never"}})
		case "/connectors/foo/status":
			json.NewEncoder(w).Encode(map[string]any{
				"name":      "foo",
				"connector": map[string]string{"state": "RUNNING", "worker_id": "w"},
				"tasks":     []map[string]any{{"id": 0, "state": "FAILED", "worker_id": "w-1"}},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	cfg := config.CDCConfig{ConnectURL: ts.URL}
	i := New(cfg)
	res, err := i.Inspect(context.Background())
	if err != nil {
		t.Fatalf("inspect error: %v", err)
	}
	// Expect exact warnings about snapshot.mode, health, failed task, and restart loop
	expected := []string{
		"Connector foo has snapshot.mode=never; snapshots disabled or schema-only (CDC may miss initial data). This check will not attempt to trigger snapshots.",
		"Connector foo health: connector=RUNNING tasks=[0:FAILED]",
		"Connector foo has failed task(s): [0]",
		"Connector foo may be in restart loop: connector RUNNING but tasks failing",
	}
	if len(res.Warnings) != len(expected) {
		t.Fatalf("expected %d warnings, got %d: %v", len(expected), len(res.Warnings), res.Warnings)
	}
	for i, w := range expected {
		if res.Warnings[i] != w {
			t.Errorf("warning %d: expected %q, got %q", i, w, res.Warnings[i])
		}
	}
	// Fail if any mismatch was found
	for i, w := range expected {
		if res.Warnings[i] != w {
			t.Fatalf("warnings mismatch; expected: %v, got: %v", expected, res.Warnings)
		}
	}
}
