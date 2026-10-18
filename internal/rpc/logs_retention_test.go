package rpc_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/compactor"
	"github.com/masonwheeler/observability-platform/internal/rpc"
	"github.com/masonwheeler/observability-platform/internal/tenant"
)

var _ compactor.LogManager = (*rpc.LogsRetentionClient)(nil)

func flushLogs(t *testing.T, f storeFixture, body string) {
	t.Helper()
	if resp, out := post(t, f.srv.URL+"/internal/v1/logs/flush", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("flush: %d %s", resp.StatusCode, out)
	}
}

func TestLogsRetentionRoute(t *testing.T) {
	f := newStoreFixture(t)
	flushLogs(t, f, `{"streams":[{"labels":{"service":"api"},"entries":[[1,"a"]]},`+
		`{"labels":{"service":"api","__tenant__":"keep"},"entries":[[1,"k"]]}]}`)
	resp, out := post(t, f.srv.URL+"/internal/v1/logs/retention",
		`{"now_ms":10000000000000,"retention_ms":1,"overrides_ms":{"keep":0}}`)
	if resp.StatusCode != http.StatusOK || !strings.Contains(out, `"deleted":1`) {
		t.Fatalf("retention: %d %s, want 200 with deleted 1", resp.StatusCode, out)
	}
	for name, body := range map[string]string{
		"negative retention": `{"now_ms":0,"retention_ms":-1}`,
		"negative override":  `{"now_ms":0,"retention_ms":1,"overrides_ms":{"a":-1}}`,
		"unknown field":      `{"now_ms":0,"retention_ms":1,"extra":1}`,
	} {
		if resp, out := post(t, f.srv.URL+"/internal/v1/logs/retention", body); resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: %d %s, want 400", name, resp.StatusCode, out)
		}
	}
}

func TestLogsRetentionClientReturnsTheCount(t *testing.T) {
	f := newStoreFixture(t)
	flushLogs(t, f, `{"streams":[{"labels":{"service":"a"},"entries":[[1,"a"]]},{"labels":{"service":"b"},"entries":[[1,"b"]]}]}`)
	lr := rpc.NewLogsRetentionClient(client(t, f.srv.URL))
	n, err := lr.ApplyLogsRetention(time.UnixMilli(10_000_000_000_000), tenant.RetentionPolicy{Default: time.Millisecond})
	if err != nil || n != 2 {
		t.Fatalf("ApplyLogsRetention = %d, %v; want 2, nil", n, err)
	}
	n, err = lr.ApplyLogsRetention(time.UnixMilli(10_000_000_000_000), tenant.RetentionPolicy{Default: time.Millisecond})
	if err != nil || n != 0 {
		t.Fatalf("second pass = %d, %v; want 0, nil", n, err)
	}
}
