package rpc_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
	"github.com/masonwheeler/observability-platform/internal/observability"
	"github.com/masonwheeler/observability-platform/internal/rpc"
)

type countingMetrics struct{ n int }

func (c *countingMetrics) Append(metrics.Labels, int64, float64) error { c.n++; return nil }

type countingLogs struct{ n int }

func (c *countingLogs) Append(logs.StreamLabels, int64, string) error { c.n++; return nil }

func pushWithTenant(t *testing.T, url, tenantID, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Scope-OrgID", tenantID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// A push's series must all belong to the tenant its header names: a payload
// carrying another tenant's label, or a mix, is refused before anything lands.
func TestInternalPushBindsPayloadTenantsToTheHeader(t *testing.T) {
	m, l := &countingMetrics{}, &countingLogs{}
	r := chi.NewRouter()
	r.Route("/internal/v1", func(r chi.Router) {
		rpc.MountWrites(r, m, l, observability.NewIngestMetrics(), nil, nil)
	})
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	metricsURL, logsURL := srv.URL+"/internal/v1/metrics/push", srv.URL+"/internal/v1/logs/push"

	series := func(tenantLabel string) string {
		labels := `"__name__":"up"`
		if tenantLabel != "" {
			labels += `,"__tenant__":"` + tenantLabel + `"`
		}
		return `{"labels":{` + labels + `},"samples":[[1000,"1"]]}`
	}
	stream := func(tenantLabel string) string {
		labels := `"service":"api"`
		if tenantLabel != "" {
			labels += `,"__tenant__":"` + tenantLabel + `"`
		}
		return `{"labels":{` + labels + `},"entries":[[1000,"x"]]}`
	}

	for _, tc := range []struct {
		name, header, metricsBody, logsBody string
		ok                                  bool
	}{
		{"own tenant", "team-b", `{"series":[` + series("team-b") + `]}`, `{"streams":[` + stream("team-b") + `]}`, true},
		{"default tenant", "anonymous", `{"series":[` + series("") + `]}`, `{"streams":[` + stream("") + `]}`, true},
		{"another tenant's label", "team-a", `{"series":[` + series("team-b") + `]}`, `{"streams":[` + stream("team-b") + `]}`, false},
		{"default label under a named header", "team-b", `{"series":[` + series("") + `]}`, `{"streams":[` + stream("") + `]}`, false},
		{"named label under the default header", "anonymous", `{"series":[` + series("team-b") + `]}`, `{"streams":[` + stream("team-b") + `]}`, false},
		{"mixed tenants", "team-b", `{"series":[` + series("team-b") + `,` + series("") + `]}`, `{"streams":[` + stream("team-b") + `,` + stream("") + `]}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m.n, l.n = 0, 0
			for _, p := range []struct{ url, body string }{{metricsURL, tc.metricsBody}, {logsURL, tc.logsBody}} {
				code, body := pushWithTenant(t, p.url, tc.header, p.body)
				switch {
				case tc.ok && code != http.StatusNoContent:
					t.Fatalf("POST %s = %d %s, want 204", p.url, code, body)
				case !tc.ok && (code != http.StatusBadRequest || !strings.Contains(body, "tenant")):
					t.Fatalf("POST %s = %d %s, want 400 naming the tenant mismatch", p.url, code, body)
				}
			}
			if !tc.ok && (m.n != 0 || l.n != 0) {
				t.Fatalf("a refused push appended %d samples and %d entries", m.n, l.n)
			}
		})
	}
}
