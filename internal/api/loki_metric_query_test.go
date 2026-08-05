package api_test

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"
)

// metricEnd is the query end for the metric tests: two seconds past the fixture
// base. The log tests in loki_query_test.go query ns(0)..ns(1000) — a
// one-microsecond window — which is fine for streams but useless for a metric
// query, where a one-second step would put the only tick at start, before every
// entry, and every result would be empty.
func metricEnd() string { return ns(int64(2 * time.Second)) }

// levelStreams carries the `level` label the log-volume query groups by, which
// twoStreams in loki_query_test.go deliberately does not.
var levelStreams = fmt.Sprintf(`{"streams":[
 {"stream":{"service":"api","level":"info"},"values":[[%q,"GET /a ok"],[%q,"GET /b ok"]]},
 {"stream":{"service":"api","level":"error"},"values":[[%q,"GET /c failed"]]}
]}`, ns(100), ns(200), ns(300))

type lokiMatrixResp struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Values [][2]any          `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

// TestLokiQueryRange_LogVolumeQuery runs the exact expression Grafana Explore
// issues for its log-volume histogram. This is the phase's reason to exist.
func TestLokiQueryRange_LogVolumeQuery(t *testing.T) {
	srv := newLokiServer(t)
	pushLogs(t, srv, levelStreams)

	q := url.Values{}
	q.Set("query", `sum by (level) (count_over_time({service="api"}[5m]))`)
	q.Set("start", ns(0))
	q.Set("end", metricEnd())
	q.Set("step", "1") // seconds — ticks land at start, +1s, +2s
	w := getLoki(t, srv, "/loki/api/v1/query_range", q)
	if w.Code != 200 {
		t.Fatalf("code = %d, body = %q", w.Code, w.Body.String())
	}

	var resp lokiMatrixResp
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v (body %s)", err, w.Body.String())
	}
	if resp.Status != "success" || resp.Data.ResultType != "matrix" {
		t.Fatalf("envelope = %+v, want a success matrix", resp)
	}
	if len(resp.Data.Result) != 2 {
		t.Fatalf("got %d series, want one per level, in %s", len(resp.Data.Result), w.Body.String())
	}
	seen := map[string]bool{}
	for _, s := range resp.Data.Result {
		level := s.Metric["level"]
		seen[level] = true
		if len(s.Metric) != 1 {
			t.Errorf("series labels = %v, want only the grouped label", s.Metric)
		}
		if len(s.Values) == 0 {
			t.Fatalf("series %v has no values", s.Metric)
		}
		for _, v := range s.Values {
			if _, ok := v[0].(float64); !ok {
				t.Errorf("timestamp %v is not a JSON number", v[0])
			}
			if _, ok := v[1].(string); !ok {
				t.Errorf("value %v is not a string", v[1])
			}
		}
		// levelStreams has two info lines and one error line, all inside the
		// 5m window, so the last tick holds the full count for each level.
		want := "1"
		if level == "info" {
			want = "2"
		}
		if got := s.Values[len(s.Values)-1][1]; got != want {
			t.Errorf("level %q final value = %v, want %q", level, got, want)
		}
	}
	if !seen["info"] || !seen["error"] {
		t.Errorf("levels = %v, want info and error", seen)
	}
}

// TestLokiQueryRange_MetricOps covers every supported op reaching the engine
// through HTTP and coming back as a matrix.
func TestLokiQueryRange_MetricOps(t *testing.T) {
	srv := newLokiServer(t)
	pushLogs(t, srv, levelStreams)

	for _, expr := range []string{
		`count_over_time({service="api"}[5m])`,
		`rate({service="api"}[5m])`,
		`bytes_over_time({service="api"}[5m])`,
		`bytes_rate({service="api"}[5m])`,
		`sum(count_over_time({service="api"}[5m]))`,
		`sum without (level) (count_over_time({service="api"}[5m]))`,
		`count_over_time({service="api"} |= "" [5m])`,
	} {
		q := url.Values{}
		q.Set("query", expr)
		q.Set("start", ns(0))
		q.Set("end", metricEnd())
		w := getLoki(t, srv, "/loki/api/v1/query_range", q)
		if w.Code != 200 {
			t.Errorf("%s: code = %d, body = %q", expr, w.Code, w.Body.String())
			continue
		}
		if !strings.Contains(w.Body.String(), `"resultType":"matrix"`) {
			t.Errorf("%s: body = %s, want a matrix", expr, w.Body.String())
		}
	}
}

// TestLokiQueryRange_MetricStepHandling covers the two step paths: an absent step
// is derived rather than rejected, and a step fine enough to exceed Loki's
// points-per-timeseries limit is a 400.
func TestLokiQueryRange_MetricStepHandling(t *testing.T) {
	srv := newLokiServer(t)
	pushLogs(t, srv, levelStreams)

	q := url.Values{}
	q.Set("query", `count_over_time({service="api"}[5m])`)
	q.Set("start", ns(0))
	q.Set("end", metricEnd())
	if w := getLoki(t, srv, "/loki/api/v1/query_range", q); w.Code != 200 {
		t.Fatalf("absent step: code = %d, body = %q", w.Code, w.Body.String())
	}

	q.Set("start", "0")
	q.Set("end", "100000") // 100,000 seconds
	q.Set("step", "1")     // 100,000 points > 11,000
	w := getLoki(t, srv, "/loki/api/v1/query_range", q)
	if w.Code != 400 || !strings.Contains(w.Body.String(), "maximum resolution") {
		t.Fatalf("too many points: code = %d, body = %q", w.Code, w.Body.String())
	}
}

// TestLokiInstantQuery_Metric proves an instant metric query returns a labeled
// vector, and that the constant-expression health check still works alongside it.
func TestLokiInstantQuery_Metric(t *testing.T) {
	srv := newLokiServer(t)
	pushLogs(t, srv, levelStreams)

	q := url.Values{}
	q.Set("query", `sum by (level) (count_over_time({service="api"}[1h]))`)
	q.Set("time", ns(1000))
	w := getLoki(t, srv, "/loki/api/v1/query", q)
	if w.Code != 200 {
		t.Fatalf("code = %d, body = %q", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if !strings.Contains(body, `"resultType":"vector"`) {
		t.Fatalf("body = %s, want a vector", body)
	}
	// levelStreams: two info lines, one error line, all inside the 1h window.
	for _, want := range []string{`"level":"info"`, `"level":"error"`, `,"2"]`, `,"1"]`} {
		if !strings.Contains(body, want) {
			t.Errorf("body = %s, want %s", body, want)
		}
	}

	// The health check must be unaffected by the new dispatch.
	q = url.Values{}
	q.Set("query", "vector(1)+vector(1)")
	q.Set("time", "4000000000")
	w = getLoki(t, srv, "/loki/api/v1/query", q)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `,"2"]`) {
		t.Fatalf("health check regressed: code = %d, body = %q", w.Code, w.Body.String())
	}
}

// TestLokiMetricQuery_Unsupported keeps the guardrail live: everything outside the
// subset is a plain-text 400, and a constant expression on query_range says where
// it is supported instead of complaining about a missing stream selector.
func TestLokiMetricQuery_Unsupported(t *testing.T) {
	srv := newLokiServer(t)

	for _, expr := range []string{
		`avg_over_time({service="api"} | unwrap duration [5m])`,
		`topk(5, count_over_time({service="api"}[5m]))`,
		`count_over_time({service="api"}[5m]) * 2`,
		`count_over_time({service=~"api"}[5m])`,
		`count_over_time({service="api"} | json [5m])`,
	} {
		q := url.Values{}
		q.Set("query", expr)
		q.Set("start", ns(0))
		q.Set("end", ns(1000))
		w := getLoki(t, srv, "/loki/api/v1/query_range", q)
		if w.Code != 400 {
			t.Errorf("%s: code = %d, want 400", expr, w.Code)
		}
		if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
			t.Errorf("%s: content-type = %q, want text/plain", expr, ct)
		}
	}

	q := url.Values{}
	q.Set("query", "vector(1)+vector(1)")
	q.Set("start", ns(0))
	q.Set("end", ns(1000))
	w := getLoki(t, srv, "/loki/api/v1/query_range", q)
	if w.Code != 400 || !strings.Contains(w.Body.String(), "instant query endpoint") {
		t.Fatalf("constant expression on query_range: code = %d, body = %q", w.Code, w.Body.String())
	}
}
