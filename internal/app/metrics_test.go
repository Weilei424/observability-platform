package app_test

import (
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/app"
	"github.com/masonwheeler/observability-platform/internal/config"
)

// Each component exports only the metrics for work it does; a counter for work
// it cannot do would read as a flat zero on a dashboard that means "idle".
func TestEachTargetExposesOnlyWhatItOwns(t *testing.T) {
	type want struct{ has, lacks []string }
	cases := map[config.Target]want{
		config.TargetAllInOne: {has: []string{"obs_active_series", "obs_blocks_total", "obs_wal_bytes", "obs_log_streams_total",
			"obs_compactions_total", "obs_flushes_total", "obs_samples_ingested_total", "obs_log_flushes_total"}},
		config.TargetIngester: {
			has:   []string{"obs_active_series", "obs_wal_bytes", "obs_flushes_total", "obs_samples_ingested_total", "obs_log_flushes_total"},
			lacks: []string{"obs_blocks_total", "obs_compactions_total", "obs_log_streams_total"},
		},
		config.TargetStore: {
			has:   []string{"obs_blocks_total", "obs_log_chunks_total", "obs_log_streams_total"},
			lacks: []string{"obs_active_series", "obs_wal_bytes", "obs_flushes_total", "obs_compactions_total", "obs_samples_ingested_total"},
		},
		config.TargetCompactor: {
			has:   []string{"obs_compactions_total", "obs_retention_deleted_blocks_total"},
			lacks: []string{"obs_flushes_total", "obs_samples_ingested_total", "obs_active_series", "obs_blocks_total"},
		},
		config.TargetQuerier: {
			has:   []string{"obs_collector_errors_total"},
			lacks: []string{"obs_active_series", "obs_flushes_total", "obs_compactions_total", "obs_samples_ingested_total", "obs_blocks_total"},
		},
		config.TargetGateway: {
			has:   []string{"obs_collector_errors_total", "obs_ring_members", "obs_samples_rejected_total"},
			lacks: []string{"obs_active_series", "obs_flushes_total", "obs_compactions_total", "obs_blocks_total"},
		},
	}
	for target, w := range cases {
		t.Run(string(target), func(t *testing.T) {
			a, err := app.Build(withPeers(testConfig(t, target)), slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(a.Close)
			body := do(a.Handler, http.MethodGet, "/metrics", "").Body.String()
			for _, name := range w.has {
				if !strings.Contains(body, "# TYPE "+name+" ") {
					t.Errorf("%s does not export %s", target, name)
				}
			}
			for _, name := range w.lacks {
				if strings.Contains(body, "# TYPE "+name+" ") {
					t.Errorf("%s exports %s, which belongs to another component", target, name)
				}
			}
		})
	}
}
