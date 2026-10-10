package app_test

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/app"
	"github.com/masonwheeler/observability-platform/internal/config"
	"github.com/masonwheeler/observability-platform/internal/rpc"
)

// An ingester that knows its own URL refuses an internal request addressed to
// another member, and takes one addressed to it or to no one.
func TestIngesterRefusesAnotherMembersRequest(t *testing.T) {
	cfg := withPeers(testConfig(t, config.TargetIngester))
	cfg.IngesterSelfURL = "http://ingester-1:8080"
	a, err := app.Build(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	t.Cleanup(a.Close)
	const push = `{"series":[{"labels":{"__name__":"m"},"samples":[[1000,"1"]]}]}`
	for _, tc := range []struct {
		member string
		want   int
	}{
		{"http://ingester-2:8080", http.StatusMisdirectedRequest},
		{"http://ingester-1:8080", http.StatusNoContent},
		{"", http.StatusNoContent},
	} {
		req := httptest.NewRequest(http.MethodPost, "/internal/v1/metrics/push", strings.NewReader(push))
		if tc.member != "" {
			req.Header.Set(rpc.MemberHeader, tc.member)
		}
		rec := httptest.NewRecorder()
		a.Handler.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Errorf("push addressed to %q = %d, want %d; body %s", tc.member, rec.Code, tc.want, rec.Body)
		}
	}
}
