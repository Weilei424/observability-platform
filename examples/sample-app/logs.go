// Log-line generation and the Loki push half of the sample app. The metrics
// half lives in metrics.go; main.go owns the flags and the loop that drives
// both.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
)

// envLabel is carried by every stream. It is constant on purpose: it gives
// /loki/api/v1/labels a third label name so Grafana's label browser has
// something to show beyond the two labels the demo queries use.
const envLabel = "local"

// entry is one generated log line plus the stream it belongs to.
type entry struct {
	service string
	level   string
	tsNs    int64
	line    string
}

var (
	methods    = []string{"GET", "POST"}
	paths      = []string{"/api/v1/query", "/api/v1/query_range", "/api/v1/ingest/metrics", "/loki/api/v1/push"}
	jobs       = []string{"compaction", "retention", "flush"}
	serverErrs = []int{500, 503}
)

func requestID(r *rand.Rand) string {
	return fmt.Sprintf("%06x", r.Intn(1<<24))
}

// apiEntry generates one api-service line: 80% info, 12% warn, 8% error.
// Lines are plain text, never JSON or logfmt — `| json` and `| logfmt` return
// explicit errors in this backend, so structured lines would invite an
// unsupported pipeline on a viewer's first click.
func apiEntry(r *rand.Rand, tsNs int64) entry {
	method := methods[r.Intn(len(methods))]
	path := paths[r.Intn(len(paths))]
	id := requestID(r)
	switch roll := r.Float64(); {
	case roll < 0.80:
		return entry{"api", "info", tsNs,
			fmt.Sprintf("%s %s 200 in %dms request_id=%s", method, path, 1+r.Intn(200), id)}
	case roll < 0.92:
		return entry{"api", "warn", tsNs,
			fmt.Sprintf("slow request: %s %s 200 in %dms request_id=%s exceeded 500ms budget", method, path, 500+r.Intn(1000), id)}
	default:
		return entry{"api", "error", tsNs,
			fmt.Sprintf("%s %s %d in %dms request_id=%s upstream timeout after 30s", method, path, serverErrs[r.Intn(len(serverErrs))], 1+r.Intn(60), id)}
	}
}

// workerEntry generates one worker-service line: 90% info, 10% error.
func workerEntry(r *rand.Rand, tsNs int64) entry {
	job := jobs[r.Intn(len(jobs))]
	id := fmt.Sprintf("block-%04d", r.Intn(10000))
	if r.Float64() < 0.90 {
		return entry{"worker", "info", tsNs,
			fmt.Sprintf("job %s=%s completed in %dms", job, id, 100+r.Intn(3000))}
	}
	return entry{"worker", "error", tsNs,
		fmt.Sprintf("job %s=%s failed: deadline exceeded", job, id)}
}

// buildBatch generates one tick: three api lines and one worker line.
func buildBatch(r *rand.Rand, tsNs int64) []entry {
	out := make([]entry, 0, 4)
	for i := 0; i < 3; i++ {
		out = append(out, apiEntry(r, tsNs))
	}
	return append(out, workerEntry(r, tsNs))
}

// encodePush groups entries by stream and marshals the Loki push payload:
// {"streams":[{"stream":{...},"values":[["<unix_nano>","<line>"]]}]}
func encodePush(entries []entry) ([]byte, error) {
	type stream struct {
		Stream map[string]string `json:"stream"`
		Values [][2]string       `json:"values"`
	}
	byKey := make(map[string]*stream)
	var order []string
	for _, e := range entries {
		key := e.service + "/" + e.level
		s, ok := byKey[key]
		if !ok {
			s = &stream{Stream: map[string]string{"service": e.service, "level": e.level, "env": envLabel}}
			byKey[key] = s
			order = append(order, key)
		}
		s.Values = append(s.Values, [2]string{strconv.FormatInt(e.tsNs, 10), e.line})
	}
	streams := make([]*stream, 0, len(order))
	for _, k := range order {
		streams = append(streams, byKey[k])
	}
	return json.Marshal(map[string]any{"streams": streams})
}

// postBatch sends one push and reports whether the batch was accepted.
//
// The Loki push contract is exactly 204 No Content, so that is the only status
// counted as a delivered batch. Accepting the whole 2xx range instead would let
// a backend that regressed to 200 or 202 — a real compatibility break for every
// Loki client, not just this one — keep incrementing the success counter while
// the demo silently stopped matching the API it claims to implement.
func postBatch(client *http.Client, addr string, body []byte) error {
	resp, err := client.Post(addr+"/loki/api/v1/push", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("unexpected status %d, want %d: %s",
			resp.StatusCode, http.StatusNoContent, strings.TrimSpace(string(snippet)))
	}
	return nil
}
