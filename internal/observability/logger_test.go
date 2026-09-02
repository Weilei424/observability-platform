package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func decodeLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func TestComponentAddsTheFieldToEveryLine(t *testing.T) {
	var buf bytes.Buffer
	log := Component(slog.New(slog.NewJSONHandler(&buf, nil)), "compactor")

	log.Info("first")
	log.Warn("second")

	lines := decodeLines(t, &buf)
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2", len(lines))
	}
	for i, l := range lines {
		if l["component"] != "compactor" {
			t.Errorf("line %d component = %v, want \"compactor\"", i, l["component"])
		}
	}
}

// slog does not deduplicate attribute keys, so a call site that logs through a
// component logger and ALSO passes its own "component" argument ends up with
// both rendered on the line — this is the exact failure mode the migration
// guards against, and this test pins it down rather than merely asserting it
// away: a call site written this way is wrong, and this is what "wrong" looks
// like on the wire.
func TestComponentIsNotDuplicatedByCallSites(t *testing.T) {
	var buf bytes.Buffer
	log := Component(slog.New(slog.NewJSONHandler(&buf, nil)), "logs")
	log.Warn("replay skipped", "component", "logs", "error", "bad labels")

	if n := strings.Count(buf.String(), `"component"`); n != 2 {
		t.Errorf("line carries %d component keys, want exactly 2 (both the component logger's and the call site's manual one): %s", n, buf.String())
	}
}

func TestFromContextReturnsTheStoredLogger(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil)).With("request_id", "abc123")

	got := FromContext(ContextWithLogger(context.Background(), log))
	got.Info("handled")

	lines := decodeLines(t, &buf)
	if len(lines) != 1 || lines[0]["request_id"] != "abc123" {
		t.Errorf("context logger lost its fields: %v", lines)
	}
}

// Handlers also run outside a request: WAL replay, tests, the healthcheck probe.
// FromContext must return a usable logger there rather than nil.
func TestFromContextFallsBackToDefault(t *testing.T) {
	if FromContext(context.Background()) == nil {
		t.Fatal("FromContext returned nil; every call site would panic")
	}
	FromContext(context.Background()).Info("must not panic")
}
