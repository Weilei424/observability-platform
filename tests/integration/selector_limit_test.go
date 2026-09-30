package integration_test

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/rpc"
)

// TestSelectorLimitParity sends the same oversized and worst-case selectors to
// an all-in-one server and to a split cluster's gateway: both must answer every
// request with the same status. A selector at the cap is built from \x01 bytes,
// each of which JSON writes as six ("\u0001"), so it is the largest internal
// select body an accepted query can produce; one byte over the cap, or a
// selector larger than a whole internal select body (which split mode once
// answered 500 while all-in-one answered 200), is refused with 400 before it
// reaches a peer.
func TestSelectorLimitParity(t *testing.T) {
	aio := startAllInOne(t)
	c := startCluster(t)

	// selector returns an equality selector of exactly n bytes whose value is
	// all \x01; head opens it (`up{job="` for PromQL, `{job="` for LogQL).
	selector := func(head string, n int) string {
		return head + strings.Repeat("\x01", n-len(head)-len(`"}`)) + `"}`
	}
	type request struct {
		name, method, path string
		form               url.Values
		want               int
	}
	var requests []request
	for _, n := range []int{rpc.MaxSelectorBytes, rpc.MaxSelectorBytes + 1, 9 * rpc.MaxSelectorBytes} {
		prom := selector(`up{job="`, n)
		want := http.StatusOK
		if n > rpc.MaxSelectorBytes {
			want = http.StatusBadRequest
		}
		requests = append(requests,
			request{fmt.Sprintf("prom query %d", n), http.MethodPost, "/api/v1/query", url.Values{"query": {prom}, "time": {"100"}}, want},
			request{fmt.Sprintf("prom query_range %d", n), http.MethodPost, "/api/v1/query_range",
				url.Values{"query": {prom}, "start": {"0"}, "end": {"100"}, "step": {"10"}}, want},
			request{fmt.Sprintf("prom series %d", n), http.MethodPost, "/api/v1/series", url.Values{"match[]": {prom}}, want},
		)
		// The Loki reads are GET-only: a URL past net/http's 1 MiB header limit
		// is answered 431 before any handler runs, in every topology alike.
		if n <= rpc.MaxSelectorBytes+1 {
			requests = append(requests, request{fmt.Sprintf("loki query_range %d", n), http.MethodGet, "/loki/api/v1/query_range",
				url.Values{"query": {selector(`{job="`, n)}, "start": {"0"}, "end": {"100"}}, want})
		}
	}

	do := func(base string, r request) (int, string) {
		t.Helper()
		var (
			resp *http.Response
			err  error
		)
		if r.method == http.MethodPost {
			resp, err = httpClient.PostForm(base+r.path, r.form)
		} else {
			resp, err = httpClient.Get(base + r.path + "?" + r.form.Encode())
		}
		if err != nil {
			t.Fatalf("%s: %v", r.name, err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return resp.StatusCode, string(body)
	}
	aioURL := "http://" + aio.cfg.HTTPAddr
	for _, r := range requests {
		aioCode, aioBody := do(aioURL, r)
		splitCode, splitBody := do(c.gatewayURL, r)
		if aioCode != r.want || splitCode != r.want {
			t.Errorf("%s: all-in-one %d, split %d, want %d in both\nall-in-one: %.200q\nsplit: %.200q",
				r.name, aioCode, splitCode, r.want, aioBody, splitBody)
		}
	}
}

// startAllInOne starts an all-in-one server for a parity comparison against a
// split cluster.
func startAllInOne(t *testing.T) *process {
	t.Helper()
	aio := &process{t: t, cfg: &config.Config{
		Target: config.TargetAllInOne, HTTPAddr: freeAddr(t), DataDir: t.TempDir(), LogLevel: "info",
		WALSegmentMaxBytes: 1 << 20, WALSyncEveryN: 1, LogsFlushThresholdBytes: 1 << 20,
		MaintenanceInterval: time.Hour, FlushInterval: time.Hour, FlushSealedChunks: 1,
		CompactionBaseRange: 2 * time.Hour, CompactionMultiplier: 4, CompactionLevels: 3,
	}}
	aio.start()
	t.Cleanup(aio.stop)
	return aio
}

// TestSelectorInvalidUTF8Parity stores a label value of U+FFFD in both
// topologies, then queries with an invalid byte in its place. Split mode's JSON
// transport once turned that byte into U+FFFD and returned the series, while
// all-in-one compared the byte literally and returned nothing; both must now
// refuse the query with 400.
func TestSelectorInvalidUTF8Parity(t *testing.T) {
	aioURL := "http://" + startAllInOne(t).cfg.HTTPAddr
	c := startCluster(t)

	post := func(url, body string) {
		t.Helper()
		resp, err := httpClient.Post(url, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			b, _ := io.ReadAll(resp.Body)
			t.Fatalf("POST %s = %d: %s", url, resp.StatusCode, b)
		}
	}
	for _, base := range []string{aioURL, c.gatewayURL} {
		post(base+"/api/v1/ingest/metrics", `{"metrics":[{"name":"up","labels":{"job":"\ufffd"},"timestamp_ms":100000,"value":1}]}`)
		post(base+"/loki/api/v1/push", `{"streams":[{"stream":{"job":"\ufffd"},"values":[["100000000000","line"]]}]}`)
	}

	type request struct {
		name, method, path string
		form               url.Values
	}
	requests := []request{
		{"prom query", http.MethodPost, "/api/v1/query", url.Values{"query": {"up{job=\"\xff\"}"}, "time": {"100"}}},
		{"prom series", http.MethodPost, "/api/v1/series", url.Values{"match[]": {"up{job=\"\xff\"}"}}},
		{"loki raw byte", http.MethodGet, "/loki/api/v1/query_range",
			url.Values{"query": {"{job=\"\xff\"}"}, "start": {"0"}, "end": {"200000000000"}}},
		{"loki escape", http.MethodGet, "/loki/api/v1/query_range",
			url.Values{"query": {`{job="\xff"}`}, "start": {"0"}, "end": {"200000000000"}}},
	}
	for _, r := range requests {
		for _, side := range []struct{ name, base string }{{"all-in-one", aioURL}, {"split", c.gatewayURL}} {
			var (
				resp *http.Response
				err  error
			)
			if r.method == http.MethodPost {
				resp, err = httpClient.PostForm(side.base+r.path, r.form)
			} else {
				resp, err = httpClient.Get(side.base + r.path + "?" + r.form.Encode())
			}
			if err != nil {
				t.Fatalf("%s %s: %v", side.name, r.name, err)
			}
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("%s %s: status %d, want 400; body %.200q", side.name, r.name, resp.StatusCode, body)
			}
		}
	}
}
