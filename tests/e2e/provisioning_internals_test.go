package e2e_test

// Static validation for the self-observability datasource and dashboard. Reads
// the checked-in files only — no Docker, no Grafana — so it runs in `go test ./...`.
//
// This dashboard is the one whose queries are NOT bound by the backend's PromQL
// subset: it reads a real Prometheus. See
// TestInternalsDashboardIsExemptFromTheSubsetRule below.

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/observability"
)

const (
	internalsDatasourcePath = "../../observability/grafana/datasources/prometheus-internals.yml"
	internalsDashboardPath  = "../../observability/grafana/dashboards/self-observability.json"

	internalsDatasourceName = "observability-platform-internals"
	internalsDatasourceUID  = "obs-internals"
	internalsDashboardUID   = "obs-self-v1"
	internalsDashboardTitle = "Observability Platform Internals"

	prometheusComposeName = "prometheus"
)

func TestInternalsDatasourceProvisioning(t *testing.T) {
	ds := loadDatasource(t, internalsDatasourcePath).Datasources[0]

	if ds.Name != internalsDatasourceName {
		t.Errorf("name = %q, want %q", ds.Name, internalsDatasourceName)
	}
	if ds.Type != "prometheus" {
		t.Errorf("type = %q, want \"prometheus\"", ds.Type)
	}
	if ds.UID != internalsDatasourceUID {
		t.Errorf("uid = %q, want %q — the internals dashboard references it", ds.UID, internalsDatasourceUID)
	}
	if ds.Access != "proxy" {
		t.Errorf("access = %q, want \"proxy\"; the URL is a compose service name that only resolves server-side", ds.Access)
	}
	// Two prometheus-typed datasources now exist. Only the backend's is default;
	// a second default would make panels saved without an explicit datasource
	// resolve unpredictably.
	if ds.IsDefault {
		t.Error("isDefault = true; obs-prometheus is the default datasource, not this one")
	}
}

// The URL must name the prometheus service, NOT the backend. Pointing this
// datasource at the backend would silently query the wrong TSDB — one that holds
// no obs_* series at all — and every panel would read "No data" with a healthy
// datasource test.
func TestInternalsDatasourceURLMatchesComposePrometheus(t *testing.T) {
	datasourceURLMatchesComposeService(t, internalsDatasourcePath, prometheusComposeName)
}

func TestHelmGrafanaTemplatesTheInternalsDatasource(t *testing.T) {
	b, err := os.ReadFile(filepath.FromSlash("../../deployments/helm/grafana/templates/configmap-datasources.yaml"))
	if err != nil {
		t.Fatalf("read grafana datasource configmap: %v", err)
	}
	body := string(b)
	for _, want := range []string{internalsDatasourceName, internalsDatasourceUID, "internals.url"} {
		if !strings.Contains(body, want) {
			t.Errorf("Helm datasource ConfigMap does not mention %q; the Kubernetes demo would have no internals datasource", want)
		}
	}
}

func TestInternalsDashboardIdentity(t *testing.T) {
	d := loadDashboard(t, internalsDashboardPath)
	if d.UID != internalsDashboardUID {
		t.Errorf("uid = %q, want %q", d.UID, internalsDashboardUID)
	}
	if d.Title != internalsDashboardTitle {
		t.Errorf("title = %q, want %q", d.Title, internalsDashboardTitle)
	}
}

func TestInternalsDashboardTargetsUseTheInternalsDatasource(t *testing.T) {
	ds := loadDatasource(t, internalsDatasourcePath).Datasources[0]
	for _, p := range loadDashboard(t, internalsDashboardPath).Panels {
		checkDatasourceRef(t, "self-observability.json panel "+p.Title, p.Datasource, ds.Type, ds.UID, filepath.Base(internalsDatasourcePath))
		for _, tgt := range p.Targets {
			checkDatasourceRef(t, "self-observability.json panel "+p.Title+" target "+tgt.RefID, tgt.Datasource, ds.Type, ds.UID, filepath.Base(internalsDatasourcePath))
		}
	}
}

// The regression this phase is most likely to produce: someone renames a metric,
// every Go test still passes, and a panel silently goes empty until a human opens
// the dashboard. Every metric the dashboard names must be one the server registers.
func TestInternalsDashboardOnlyQueriesRegisteredMetrics(t *testing.T) {
	registered := registeredMetricNames(t)

	for _, p := range loadDashboard(t, internalsDashboardPath).Panels {
		for _, tgt := range p.Targets {
			if tgt.Expr == "" {
				t.Errorf("panel %q target %s has an empty expr", p.Title, tgt.RefID)
				continue
			}
			for _, name := range metricNamesIn(tgt.Expr) {
				if !registered[name] {
					t.Errorf("panel %q queries %q, which the server does not register; the panel would render empty", p.Title, name)
				}
			}
		}
	}
}

func TestInternalsDashboardCoversIngestQueryAndStorage(t *testing.T) {
	var titles []string
	for _, p := range loadDashboard(t, internalsDashboardPath).Panels {
		titles = append(titles, strings.ToLower(p.Title))
	}
	joined := strings.Join(titles, " | ")
	// The DoD names these three; a dashboard missing one does not meet it.
	for _, want := range []string{"ingest", "quer", "wal"} {
		if !strings.Contains(joined, want) {
			t.Errorf("no panel title mentions %q; panels are: %s", want, joined)
		}
	}
}

// registeredMetricNames gathers the names a real registry exposes, so the
// dashboard is checked against the server rather than against a hand-kept list
// that would drift.
//
// Every instrument must be given one observation first: Gather returns nothing at
// all for a counter or histogram that has never been touched, so a naive version
// of this helper reports that obs_http_requests_total is unregistered and fails
// every panel that queries it.
func registeredMetricNames(t *testing.T) map[string]bool {
	t.Helper()
	reg, inst := observability.NewRegistry(observability.RegistryOptions{
		Cardinality: constCardinality{},
		Storage:     constStorage{},
		WALs: []observability.WALSource{
			{Name: "metrics", Stats: func() (int64, int, error) { return 0, 0, nil }},
			// A failing source so obs_collector_errors_total has an observation
			// too — the dashboard graphs it, and it is emitted only on error.
			{Name: "broken", Stats: func() (int64, int, error) { return 0, 0, errors.New("seed") }},
		},
		Logs: constLogStats{},
	})

	inst.HTTP.Observe("/seed", "GET", 200, time.Millisecond)
	inst.Ingest.SamplesIngested.Inc()
	inst.Ingest.SamplesRejected.WithLabelValues("other").Inc()
	inst.Ingest.LogLinesIngested.Inc()
	inst.Ingest.LogLinesRejected.WithLabelValues("other").Inc()
	inst.Maintenance.CompactionsTotal.Inc()
	inst.Maintenance.CompactionFailuresTotal.Inc()
	inst.Maintenance.CompactionDuration.Observe(0.1)
	inst.Maintenance.RetentionDeletedTotal.Inc()
	inst.Maintenance.FlushesTotal.Inc()
	inst.Maintenance.FlushFailuresTotal.Inc()

	// Gather once and discard the result before the real read below.
	//
	// obs_collector_errors_total is a CounterVec registered directly, while the
	// "broken" WAL source's failure is only counted as a side effect of
	// walCollector's OWN Collect (a separately-registered Collector) calling
	// WithLabelValues("wal").Inc() lazily. prometheus.Registry.Gather runs every
	// registered Collector's Collect concurrently, so a single Gather can race
	// the CounterVec's Collect (which only reports label combinations that
	// already exist) against that lazy Inc — on an unlucky ordering, this one
	// Gather call snapshots the vec before "wal" exists and the family is
	// absent, exactly like the never-touched-instrument gap this helper exists
	// to avoid, but flaky instead of constant. The label combination exists in
	// the vec from this point on regardless of ordering, so a second Gather
	// always reports it.
	if _, err := reg.Gather(); err != nil {
		t.Fatalf("Gather (priming pass): %v", err)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	names := make(map[string]bool, len(families))
	for _, f := range families {
		names[f.GetName()] = true
		// A histogram named X is queried as X_bucket, X_sum, and X_count.
		names[f.GetName()+"_bucket"] = true
		names[f.GetName()+"_sum"] = true
		names[f.GetName()+"_count"] = true
	}
	return names
}

// metricNamesIn pulls the metric identifiers out of a PromQL expression: bare
// identifiers that are not PromQL keywords, functions, or aggregation modifiers.
func metricNamesIn(expr string) []string {
	notMetrics := map[string]bool{
		"sum": true, "rate": true, "irate": true, "by": true, "without": true,
		"histogram_quantile": true, "increase": true, "avg": true, "max": true,
		"min": true, "count": true, "topk": true, "le": true, "on": true,
		"ignoring": true, "group_left": true, "group_right": true, "and": true,
		"or": true, "unless": true, "offset": true, "bool": true,
	}
	var out []string
	for _, tok := range strings.FieldsFunc(expr, func(r rune) bool {
		return !(r == '_' || r == ':' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'))
	}) {
		if notMetrics[tok] || tok == "" {
			continue
		}
		if tok[0] >= '0' && tok[0] <= '9' {
			continue // a duration or a literal
		}
		if !strings.HasPrefix(tok, "obs_") {
			continue // a label name or a legend token, not a metric
		}
		out = append(out, tok)
	}
	return out
}

type constCardinality struct{}

func (constCardinality) Cardinality() (int, int, int) { return 0, 0, 0 }

type constStorage struct{}

func (constStorage) StorageStats() (int, int64) { return 0, 0 }

type constLogStats struct{}

func (constLogStats) Stats() (int, int, int64, error) { return 0, 0, 0, nil }
