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

// slog does not deduplicate attribute keys, so a logger that already carries
// "component" before a second Component() call is layered on top ends up with
// BOTH rendered on the line, not the second replacing the first. That is
// exactly how production used to double up: cmd/server/main.go handed the
// request middleware a logger already wrapped in Component(log, "api"), so
// every handler's own observability.Component(observability.FromContext(ctx),
// "<subsystem>") call added a SECOND "component" key instead of contributing
// the request's only one.
//
// The fix is that a context-provided logger must never carry "component"
// already. FromContext here returns exactly that shape — a logger with only
// "request_id", the same shape middleware.Logger installs post-fix — so the
// single Component call below must be the only source of "component" on the
// line. A context logger that (incorrectly) already carried "component"
// before this call would push the count to 2 and fail this assertion; that is
// the regression this test guards against.
func TestComponentIsNotDuplicatedByCallSites(t *testing.T) {
	var buf bytes.Buffer
	base := slog.New(slog.NewJSONHandler(&buf, nil)).With(slog.String("request_id", "abc123"))
	ctx := ContextWithLogger(context.Background(), base)

	log := Component(FromContext(ctx), "logs")
	log.Warn("replay skipped", "error", "bad labels")

	if n := strings.Count(buf.String(), `"component"`); n != 1 {
		t.Errorf("line carries %d component keys, want exactly 1 (a context logger that already carried \"component\" before this call would produce 2): %s", n, buf.String())
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
