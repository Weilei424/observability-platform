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

// A store that knows its own URL refuses an internal request addressed to
// another store, and routes one addressed to it or to no one.
func TestStoreRefusesAnotherMembersRequest(t *testing.T) {
	cfg := withPeers(testConfig(t, config.TargetStore))
	cfg.StoreSelfURL = "http://store-1:8080"
	a, err := app.Build(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	t.Cleanup(a.Close)
	for _, tc := range []struct {
		member string
		misdir bool
	}{
		{"http://store-2:8080", true},
		{"http://store-1:8080", false},
		{"", false},
	} {
		req := httptest.NewRequest(http.MethodPost, "/internal/v1/logs/flush", strings.NewReader(`{}`))
		if tc.member != "" {
			req.Header.Set(rpc.MemberHeader, tc.member)
		}
		rec := httptest.NewRecorder()
		a.Handler.ServeHTTP(rec, req)
		if got := rec.Code == http.StatusMisdirectedRequest; got != tc.misdir {
			t.Errorf("flush addressed to %q = %d, misdirected = %v, want %v; body %s", tc.member, rec.Code, got, tc.misdir, rec.Body)
		}
	}
}
