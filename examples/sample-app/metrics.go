// The metrics half of the sample app: a simulated request/worker workload
// published as sample_app_* series through the backend's native ingest API.
//
// The names are deliberately disjoint from the load generator's http_* series.
// The separation has to be by metric NAME, not by a service label: the
// provisioned metrics dashboard aggregates sum by (method)(rate(
// http_requests_total[1m])) with no service filter, so a second writer using
// that name would silently fold into panels it has nothing to do with.
//
// Values come from this file's own random walk. They are not derived from the
// log lines logs.go generates — the two signals describe the same fictional
// service without agreeing on any particular event.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
)

// serviceLabel rides on every sample. The metric names already separate this
// app from the load generator; this label is what makes the provenance visible
// in Grafana's label browser and /api/v1/label/service/values.
const serviceLabel = "sample-app"

// Worker-count bounds for the random walk behind sample_app_active_workers.
const (
	minWorkers = 1
	maxWorkers = 8
)

// metricSample is one entry in the backend's native ingest payload.
type metricSample struct {
	Name        string            `json:"name"`
	Labels      map[string]string `json:"labels"`
	TimestampMs int64             `json:"timestamp_ms"`
	Value       float64           `json:"value"`
}

// workload is the simulated state behind the sample_app_* series. Counters are
// cumulative for the process lifetime, because the ingest API takes an absolute
// value per sample rather than a delta.
//
// It is driven from the single select loop in main.go and holds no lock: one
// goroutine owns it, as it owns the *rand.Rand passed to tick.
type workload struct {
	getTotal, postTotal     int64
	getErrors, postErrors   int64
	getLatency, postLatency float64
	activeWorkers           float64
}

func newWorkload() *workload {
	return &workload{activeWorkers: 4}
}

// tick advances the simulation one step: one GET and one POST served, a ~5%
// error roll on each, fresh latencies, and a bounded step for the worker count.
func (w *workload) tick(r *rand.Rand) {
	w.getTotal++
	w.postTotal++

	if r.Float64() < 0.05 {
		w.getErrors++
	}
	if r.Float64() < 0.05 {
		w.postErrors++
	}

	// GET [1ms, 200ms], POST [5ms, 500ms] — the same shapes the load generator
	// simulates, so the two dashboards read on comparable scales.
	w.getLatency = 0.001 + r.Float64()*0.199
	w.postLatency = 0.005 + r.Float64()*0.495

	step := float64(r.Intn(3) + 1) // magnitude 1–3
	if r.Intn(2) == 0 {
		step = -step
	}
	w.activeWorkers += step
	if w.activeWorkers < minWorkers {
		w.activeWorkers = minWorkers
	} else if w.activeWorkers > maxWorkers {
		w.activeWorkers = maxWorkers
	}
}

// samples renders the current state as the seven series this app publishes.
//
// Every series is emitted on every tick, error counters included even while
// they sit at zero. A counter that first appears on its own increment cannot
// satisfy rate()'s two-sample requirement, so the Error Rate panel would render
// empty for exactly as long as nothing was going wrong.
func (w *workload) samples(tsMs int64) []metricSample {
	return []metricSample{
		{
			Name:        "sample_app_requests_total",
			Labels:      map[string]string{"service": serviceLabel, "method": "GET", "status": "200"},
			TimestampMs: tsMs,
			Value:       float64(w.getTotal),
		},
		{
			Name:        "sample_app_requests_total",
			Labels:      map[string]string{"service": serviceLabel, "method": "POST", "status": "201"},
			TimestampMs: tsMs,
			Value:       float64(w.postTotal),
		},
		{
			Name:        "sample_app_errors_total",
			Labels:      map[string]string{"service": serviceLabel, "method": "GET", "status": "500"},
			TimestampMs: tsMs,
			Value:       float64(w.getErrors),
		},
		{
			Name:        "sample_app_errors_total",
			Labels:      map[string]string{"service": serviceLabel, "method": "POST", "status": "503"},
			TimestampMs: tsMs,
			Value:       float64(w.postErrors),
		},
		{
			Name:        "sample_app_request_duration_seconds",
			Labels:      map[string]string{"service": serviceLabel, "method": "GET"},
			TimestampMs: tsMs,
			Value:       w.getLatency,
		},
		{
			Name:        "sample_app_request_duration_seconds",
			Labels:      map[string]string{"service": serviceLabel, "method": "POST"},
			TimestampMs: tsMs,
			Value:       w.postLatency,
		},
		{
			Name:        "sample_app_active_workers",
			Labels:      map[string]string{"service": serviceLabel},
			TimestampMs: tsMs,
			Value:       w.activeWorkers,
		},
	}
}

// encodeMetrics marshals the ingest payload: {"metrics":[{...}]}
func encodeMetrics(samples []metricSample) ([]byte, error) {
	return json.Marshal(map[string]any{"metrics": samples})
}

// postMetrics sends one ingest request and reports whether it was accepted.
//
// The ingest contract is exactly 204 No Content, so that is the only status
// counted as delivered — the same rule postBatch applies to the push path, and
// for the same reason: a backend that regressed to 200 or 202 would otherwise
// keep incrementing the success counter while no longer matching the API this
// project claims to implement.
func postMetrics(client *http.Client, addr string, body []byte) error {
	resp, err := client.Post(addr+"/api/v1/ingest/metrics", "application/json", bytes.NewReader(body))
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
