package api_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/api"
	"github.com/masonwheeler/observability-platform/internal/rpc"
)

// selectorOfLen returns a valid equality selector exactly n bytes long: head
// opens it (`up{job="` for PromQL, `{job="` for LogQL).
func selectorOfLen(head string, n int) string {
	const tail = `"}`
	return head + strings.Repeat("a", n-len(head)-len(tail)) + tail
}

// postForm sends form as a POST body, the only way a selector longer than a
// URL can reach the Prometheus endpoints.
func postForm(t *testing.T, srv *api.Server, path string, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	return rr
}

// Every public string that becomes an internal select is capped at
// rpc.MaxSelectorBytes with a 400, in every topology; one byte under the cap is
// an ordinary query.
func TestPromSelectorLimit(t *testing.T) {
	srv, _ := newQueryTestServer(t)
	for _, tc := range []struct {
		path, param string
		extra       url.Values
	}{
		{"/api/v1/query", "query", url.Values{"time": {"100"}}},
		{"/api/v1/query_range", "query", url.Values{"start": {"0"}, "end": {"100"}, "step": {"10"}}},
		{"/api/v1/series", "match[]", nil},
		{"/api/v1/labels", "match[]", nil},
		{"/api/v1/label/job/values", "match[]", nil},
	} {
		for _, n := range []int{rpc.MaxSelectorBytes, rpc.MaxSelectorBytes + 1} {
			form := url.Values{tc.param: {selectorOfLen(`up{job="`, n)}}
			for k, v := range tc.extra {
				form[k] = v
			}
			rr := postForm(t, srv, tc.path, form)
			want := http.StatusOK
			if n > rpc.MaxSelectorBytes {
				want = http.StatusBadRequest
			}
			if rr.Code != want {
				t.Errorf("%s %s of %d bytes: status %d, want %d; body %.200s", tc.path, tc.param, n, rr.Code, want, rr.Body.String())
			}
			if want == http.StatusBadRequest && !strings.Contains(rr.Body.String(), `"errorType":"bad_data"`) {
				t.Errorf("%s: body %.200s, want a bad_data error", tc.path, rr.Body.String())
			}
		}
	}
}

func TestPromLabelNameLimit(t *testing.T) {
	srv, _ := newQueryTestServer(t)
	rr := getQuery(t, srv, "/api/v1/label/"+strings.Repeat("a", rpc.MaxSelectorBytes+1)+"/values")
	if rr.Code != http.StatusBadRequest {
		t.Errorf("label name over the cap: status %d, want 400", rr.Code)
	}
}

func TestLokiSelectorLimit(t *testing.T) {
	srv := newLokiServer(t)
	for _, path := range []string{"/loki/api/v1/query", "/loki/api/v1/query_range"} {
		for _, n := range []int{rpc.MaxSelectorBytes, rpc.MaxSelectorBytes + 1} {
			q := url.Values{"query": {selectorOfLen(`{job="`, n)}}
			if path == "/loki/api/v1/query_range" {
				q.Set("start", "0")
				q.Set("end", "100")
			}
			rr := getQuery(t, srv, path+"?"+q.Encode())
			want := http.StatusOK
			if n > rpc.MaxSelectorBytes {
				want = http.StatusBadRequest
			}
			if rr.Code != want {
				t.Errorf("%s query of %d bytes: status %d, want %d; body %.200s", path, n, rr.Code, want, rr.Body.String())
			}
		}
	}
	rr := getQuery(t, srv, "/loki/api/v1/label/"+strings.Repeat("a", rpc.MaxSelectorBytes+1)+"/values")
	if rr.Code != http.StatusBadRequest {
		t.Errorf("Loki label name over the cap: status %d, want 400", rr.Code)
	}
}
