package rpc

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type recordingMetrics struct{ got []metrics.PendingSample }

func (r *recordingMetrics) Append(l metrics.Labels, ts int64, v float64) error {
	r.got = append(r.got, metrics.PendingSample{Labels: l, TimestampMs: ts, Value: v})
	return nil
}

type recordingLogs struct{ got []logs.PendingEntry }

func (r *recordingLogs) Append(l logs.StreamLabels, ts int64, line string) error {
	r.got = append(r.got, logs.PendingEntry{Labels: l, TimestampNs: ts, Line: line})
	return nil
}

func pushServer(t *testing.T) (*Client, *recordingMetrics, *recordingLogs, *observability.IngestMetrics) {
	t.Helper()
	m, l, im := &recordingMetrics{}, &recordingLogs{}, observability.NewIngestMetrics()
	r := chi.NewRouter()
	r.Route("/internal/v1", func(r chi.Router) { MountWrites(r, m, l, im) })
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	c, err := NewClient("ingester", srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	return c, m, l, im
}

func TestPushSamplesRoundTrip(t *testing.T) {
	c, m, _, im := pushServer(t)
	a, _ := metrics.NewLabels(map[string]string{"__name__": "a", "job": "<x&y>"})
	b, _ := metrics.NewLabels(map[string]string{"__name__": "b"})
	in := []metrics.PendingSample{
		{Labels: a, TimestampMs: 1, Value: 1.5},
		{Labels: b, TimestampMs: 2, Value: math.MaxFloat64},
		{Labels: a, TimestampMs: 3, Value: -0.25},
	}
	if err := c.PushSamples(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if len(m.got) != 3 {
		t.Fatalf("appended %d samples, want 3", len(m.got))
	}
	for i := range in { // per-series order is preserved; series order is not promised
		found := false
		for _, g := range m.got {
			if g.Labels.Hash() == in[i].Labels.Hash() && g.TimestampMs == in[i].TimestampMs && g.Value == in[i].Value {
				found = true
			}
		}
		if !found {
			t.Errorf("sample %+v not appended", in[i])
		}
	}
	if v := testutil.ToFloat64(im.SamplesIngested); v != 3 {
		t.Errorf("obs_samples_ingested_total = %v, want 3", v)
	}
}

func TestPushEntriesRoundTrip(t *testing.T) {
	c, _, l, im := pushServer(t)
	s, _ := logs.NewStreamLabels(map[string]string{"service": "api"})
	in := []logs.PendingEntry{{Labels: s, TimestampNs: 10, Line: `{"msg":"<b>"}`}, {Labels: s, TimestampNs: 11, Line: "é"}}
	if err := c.PushEntries(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if len(l.got) != 2 || l.got[0].Line != in[0].Line || l.got[1].Line != "é" || l.got[1].TimestampNs != 11 {
		t.Fatalf("appended %+v", l.got)
	}
	if v := testutil.ToFloat64(im.LogLinesIngested); v != 2 {
		t.Errorf("obs_log_lines_ingested_total = %v, want 2", v)
	}
}

func TestPushRoutesRefuseBadBodies(t *testing.T) {
	c, _, _, _ := pushServer(t)
	for _, tc := range []struct{ path, body string }{
		{"metrics/push", `{"series":[{"labels":{"__name__":""},"samples":[[1,"1"]]}]}`},  // invalid labels
		{"metrics/push", `{"series":[{"labels":{"__name__":"a"},"samples":[[1,"x"]]}]}`}, // bad value
		{"metrics/push", `{"series":[],"extra":1}`},                                      // unknown field
		{"logs/push", `{"streams":[{"labels":{"":"v"},"entries":[[1,"l"]]}]}`},           // invalid labels
	} {
		u := c.base.JoinPath("internal", "v1", tc.path).String()
		resp, err := http.Post(u, "application/json", strings.NewReader(tc.body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s %s = %d, want 400", tc.path, tc.body, resp.StatusCode)
		}
	}
}
