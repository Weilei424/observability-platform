package e2e_test

// The metrics-side counterpart to provisioning_test.go. Phase 4.5 gave the logs
// half of the demo static validation and a real-stack test; the metrics half had
// neither, so a broken Prometheus datasource URL, a panel pointed at the wrong
// uid, or an expression outside the supported PromQL subset would have reached a
// viewer's browser with every suite green.
//
// Like its Loki counterpart, this reads the checked-in files only — no Docker,
// no Grafana, no backend — so it runs inside `go test ./...`.

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	yaml "go.yaml.in/yaml/v3"

	"github.com/masonwheeler/observability-platform/internal/metrics"
)

const (
	promDatasourcePath     = "../../observability/grafana/datasources/prometheus.yml"
	promScrapeConfigPath   = "../../observability/prometheus/prometheus.yml"
	metricsDashboardPath   = "../../observability/grafana/dashboards/metrics.json"
	sampleAppDashboardPath = "../../observability/grafana/dashboards/sample-app.json"

	promDatasourceName = "observability-platform"
	promDatasourceUID  = "obs-prometheus"

	metricsDashboardUID     = "obs-metrics-v1"
	metricsDashboardTitle   = "Observability Platform Metrics"
	sampleAppDashboardUID   = "obs-sample-app-v1"
	sampleAppDashboardTitle = "Observability Platform Sample App"
)

// metricDashboardPaths is every dashboard driven by the BACKEND's Prometheus
// datasource (obs-prometheus). Their expressions must stay inside the PromQL
// subset this backend implements.
var metricDashboardPaths = []string{metricsDashboardPath, sampleAppDashboardPath}

// internalsDashboardPaths is every dashboard driven by the real Prometheus
// (obs-internals). These are DELIBERATELY exempt from the subset rule below:
// they query Prometheus itself, so histogram_quantile and binary operations are
// correct there and would be wrong to reject.
//
// Kept as an explicit list rather than by simply omitting the dashboard, so that
// the exemption reads as a decision. An unexplained omission invites the next
// person either to "fix" it by adding the dashboard back — breaking the suite —
// or to loosen the subset rule for the dashboards that still need it.
var internalsDashboardPaths = []string{internalsDashboardPath}

// TestPrometheusDatasourceProvisioning pins the fields Grafana and the runbook
// both depend on: the name the runbook tells you to click, the uid every target
// references, and the proxy access mode the compose-internal URL relies on.
func TestPrometheusDatasourceProvisioning(t *testing.T) {
	ds := loadDatasource(t, promDatasourcePath).Datasources[0]

	if ds.Name != promDatasourceName {
		t.Errorf("name = %q, want %q (docs/runbooks/grafana-demo.md navigates by this name)", ds.Name, promDatasourceName)
	}
	if ds.Type != "prometheus" {
		t.Errorf("type = %q, want \"prometheus\"", ds.Type)
	}
	if ds.UID != promDatasourceUID {
		t.Errorf("uid = %q, want %q — every dashboard target references it", ds.UID, promDatasourceUID)
	}
	if ds.Access != "proxy" {
		t.Errorf("access = %q, want \"proxy\"; the URL is a compose service name that only resolves server-side", ds.Access)
	}
	if !ds.IsDefault {
		t.Error("isDefault = false; a panel saved without an explicit datasource would resolve to the Loki one and send PromQL down the log path")
	}
}

// TestPrometheusDatasourceURLMatchesComposeBackend cross-references the URL
// against docker-compose.yml rather than asserting a magic string, so renaming
// the service or moving the port fails here instead of in a browser. The
// check itself is shared with the Loki datasource's version of this test
// (TestLokiDatasourceURLMatchesComposeBackend) via
// datasourceURLMatchesComposeBackend in provisioning_test.go.
func TestPrometheusDatasourceURLMatchesComposeBackend(t *testing.T) {
	datasourceURLMatchesComposeBackend(t, promDatasourcePath)
}

// TestMetricDashboardIdentity pins the uids the runbooks and ARCHITECTURE_NOTES
// name, and the titles the runbooks navigate by.
func TestMetricDashboardIdentity(t *testing.T) {
	cases := []struct{ path, uid, title string }{
		{metricsDashboardPath, metricsDashboardUID, metricsDashboardTitle},
		{sampleAppDashboardPath, sampleAppDashboardUID, sampleAppDashboardTitle},
	}
	for _, tc := range cases {
		d := loadDashboard(t, tc.path)
		if d.UID != tc.uid {
			t.Errorf("%s: uid = %q, want %q", tc.path, d.UID, tc.uid)
		}
		if d.Title != tc.title {
			t.Errorf("%s: title = %q, want %q", tc.path, d.Title, tc.title)
		}
	}
}

// TestSampleAppDashboardPanels pins the panel set by id, type, title, and target
// count. A panel whose targets went to [] still renders — as an empty panel — so
// a dashboard-wide target count could not notice it.
func TestSampleAppDashboardPanels(t *testing.T) {
	want := []struct {
		id      int
		kind    string
		title   string
		expr    string
		targets int
	}{
		{1, "timeseries", "Request Rate by Method", "sum by (method)(rate(sample_app_requests_total[1m]))", 1},
		{2, "timeseries", "Error Rate", "rate(sample_app_errors_total[1m])", 1},
		{3, "timeseries", "Request Duration", "sample_app_request_duration_seconds", 1},
		{4, "stat", "Active Workers", "sample_app_active_workers", 1},
	}

	panels := loadDashboard(t, sampleAppDashboardPath).Panels
	if len(panels) != len(want) {
		t.Fatalf("panels = %d, want exactly %d", len(panels), len(want))
	}
	for i, w := range want {
		p := panels[i]
		if p.ID != w.id {
			t.Errorf("panel %d has id %d, want %d", i, p.ID, w.id)
		}
		if p.Type != w.kind {
			t.Errorf("panel %d (%q) type = %q, want %q", p.ID, p.Title, p.Type, w.kind)
		}
		if p.Title != w.title {
			t.Errorf("panel %d title = %q, want %q", p.ID, p.Title, w.title)
		}
		if len(p.Targets) != w.targets {
			t.Errorf("panel %d (%q) has %d targets, want %d; a panel without its query renders empty",
				p.ID, p.Title, len(p.Targets), w.targets)
		}
		if len(p.Targets) > 0 && p.Targets[0].Expr != w.expr {
			t.Errorf("panel %d (%q) expr = %q, want %q", p.ID, p.Title, p.Targets[0].Expr, w.expr)
		}
	}
}

// TestMetricDashboardTargetsUseTheProvisionedDatasource cross-references every
// reference against prometheus.yml's own type and uid. A loki-typed ref sends
// PromQL down the log query path while the uid still looks plausible.
func TestMetricDashboardTargetsUseTheProvisionedDatasource(t *testing.T) {
	ds := loadDatasource(t, promDatasourcePath).Datasources[0]

	for _, path := range metricDashboardPaths {
		for _, p := range loadDashboard(t, path).Panels {
			checkDatasourceRef(t, path+" panel "+p.Title, p.Datasource, ds.Type, ds.UID, filepath.Base(promDatasourcePath))
			for _, tgt := range p.Targets {
				checkDatasourceRef(t, path+" panel "+p.Title+" target "+tgt.RefID, tgt.Datasource, ds.Type, ds.UID, filepath.Base(promDatasourcePath))
			}
		}
	}
}

// TestMetricDashboardQueriesStayInsideTheSupportedSubset runs every panel
// expression through the backend's own parser. An unsupported function or a
// binary operation returns 400 at query time; this fails at build time instead.
func TestMetricDashboardQueriesStayInsideTheSupportedSubset(t *testing.T) {
	for _, exempt := range internalsDashboardPaths {
		if slices.Contains(metricDashboardPaths, exempt) {
			t.Fatalf("%s is in both metricDashboardPaths and internalsDashboardPaths; the subset rule would be enforced on a dashboard that is exempt from it", exempt)
		}
	}

	for _, path := range metricDashboardPaths {
		for _, p := range loadDashboard(t, path).Panels {
			for _, tgt := range p.Targets {
				if tgt.Expr == "" {
					t.Errorf("%s panel %q target %s has an empty expr", path, p.Title, tgt.RefID)
					continue
				}
				if _, err := metrics.ParseExpr(tgt.Expr); err != nil {
					t.Errorf("%s panel %q expr %q is outside the supported PromQL subset: %v",
						path, p.Title, tgt.Expr, err)
				}
			}
		}
	}
}

// TestComposeGatesProducersOnBackendHealth pins the startup ordering. Without
// the health condition both producers start against a backend that is not
// listening yet and spend their first seconds logging connection refusals; the
// healthcheck is what makes `make local-up` come up clean.
func TestComposeGatesProducersOnBackendHealth(t *testing.T) {
	b, err := os.ReadFile(filepath.FromSlash(composePath))
	if err != nil {
		t.Fatalf("read %s: %v", composePath, err)
	}
	var compose struct {
		Name     string `yaml:"name"`
		Services map[string]struct {
			Healthcheck struct {
				Test []string `yaml:"test"`
			} `yaml:"healthcheck"`
			DependsOn map[string]struct {
				Condition string `yaml:"condition"`
			} `yaml:"depends_on"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(b, &compose); err != nil {
		t.Fatalf("%s is not valid YAML: %v", composePath, err)
	}

	if compose.Name == "" {
		t.Error("compose file declares no project name; it would default to the file's directory basename, \"docker\"")
	}

	backend, ok := compose.Services[backendComposeName]
	if !ok {
		t.Fatalf("%s has no %q service", composePath, backendComposeName)
	}
	if len(backend.Healthcheck.Test) == 0 {
		t.Fatal("backend declares no healthcheck; depends_on: service_healthy would never be satisfied")
	}
	// The image is distroless, so the only executable available is the binary.
	joined := strings.Join(backend.Healthcheck.Test, " ")
	if !strings.Contains(joined, "/server") || !strings.Contains(joined, "-healthcheck") {
		t.Errorf("backend healthcheck = %v, want it to exec `/server -healthcheck`; nothing else exists in a distroless image", backend.Healthcheck.Test)
	}

	for _, producer := range []string{"sample-app", "load-generator"} {
		svc, ok := compose.Services[producer]
		if !ok {
			t.Errorf("%s has no %q service", composePath, producer)
			continue
		}
		dep, ok := svc.DependsOn[backendComposeName]
		if !ok {
			t.Errorf("%s does not depend on %q", producer, backendComposeName)
			continue
		}
		if dep.Condition != "service_healthy" {
			t.Errorf("%s depends_on %s condition = %q, want \"service_healthy\"", producer, backendComposeName, dep.Condition)
		}
	}
}

// TestComposePrometheusScrapesTheBackend ties the scrape config to the compose
// topology. A job pointed at a host that is not a compose service resolves to
// nothing, and the only symptom is an empty dashboard.
func TestComposePrometheusScrapesTheBackend(t *testing.T) {
	b, err := os.ReadFile(filepath.FromSlash(promScrapeConfigPath))
	if err != nil {
		t.Fatalf("read %s: %v", promScrapeConfigPath, err)
	}
	var cfg struct {
		Global struct {
			ScrapeInterval string `yaml:"scrape_interval"`
		} `yaml:"global"`
		ScrapeConfigs []struct {
			JobName       string `yaml:"job_name"`
			MetricsPath   string `yaml:"metrics_path"`
			StaticConfigs []struct {
				Targets []string `yaml:"targets"`
			} `yaml:"static_configs"`
		} `yaml:"scrape_configs"`
	}
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		t.Fatalf("%s is not valid YAML — Prometheus would exit at startup: %v", promScrapeConfigPath, err)
	}
	if len(cfg.ScrapeConfigs) != 1 {
		t.Fatalf("scrape_configs has %d jobs, want exactly 1", len(cfg.ScrapeConfigs))
	}
	job := cfg.ScrapeConfigs[0]
	if job.MetricsPath != "" && job.MetricsPath != "/metrics" {
		t.Errorf("metrics_path = %q, want \"/metrics\" or unset", job.MetricsPath)
	}
	if len(job.StaticConfigs) != 1 || len(job.StaticConfigs[0].Targets) != 1 {
		t.Fatalf("want exactly one static target, got %+v", job.StaticConfigs)
	}
	target := job.StaticConfigs[0].Targets[0]
	host, port, found := strings.Cut(target, ":")
	if !found {
		t.Fatalf("target %q is not host:port", target)
	}
	if host != backendComposeName {
		t.Errorf("scrape target host = %q, want the compose service %q", host, backendComposeName)
	}
	assertComposeServiceExposesPort(t, host, port)
}

// TestComposeRunsPrometheus pins the service itself: image, config mount, and the
// health gate the smoke test waits on.
func TestComposeRunsPrometheus(t *testing.T) {
	svc := loadComposeService(t, "prometheus")
	if svc.Image == "" {
		t.Error("prometheus service has no image")
	}
	if !strings.Contains(svc.Image, ":") || strings.HasSuffix(svc.Image, ":latest") {
		t.Errorf("image = %q, want an explicitly pinned tag", svc.Image)
	}
	if len(svc.Healthcheck.Test) == 0 {
		t.Error("prometheus declares no healthcheck; compose_smoke.sh gates on it")
	}
	if dep, ok := svc.DependsOn[backendComposeName]; !ok || dep.Condition != "service_healthy" {
		t.Errorf("prometheus must depend on %s being healthy, got %+v", backendComposeName, svc.DependsOn)
	}
}

// loadComposeService reads the docker-compose.yml and returns the service with
// the given name, including Image, Healthcheck.Test, DependsOn, and Ports.
func loadComposeService(t *testing.T, name string) struct {
	Image       string
	Ports       []string
	Healthcheck struct {
		Test []string
	}
	DependsOn map[string]struct {
		Condition string
	}
} {
	t.Helper()
	b, err := os.ReadFile(filepath.FromSlash(composePath))
	if err != nil {
		t.Fatalf("read %s: %v", composePath, err)
	}
	var compose struct {
		Services map[string]struct {
			Image       string `yaml:"image"`
			Ports       []any  `yaml:"ports"`
			Healthcheck struct {
				Test []string `yaml:"test"`
			} `yaml:"healthcheck"`
			DependsOn map[string]struct {
				Condition string `yaml:"condition"`
			} `yaml:"depends_on"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(b, &compose); err != nil {
		t.Fatalf("%s is not valid YAML: %v", composePath, err)
	}

	svc, ok := compose.Services[name]
	if !ok {
		t.Fatalf("%s has no %q service", composePath, name)
	}

	var ports []string
	for _, p := range svc.Ports {
		ports = append(ports, containerPort(p))
	}

	// Convert DependsOn to remove yaml tags
	dependsOn := make(map[string]struct {
		Condition string
	})
	for k, v := range svc.DependsOn {
		dependsOn[k] = struct {
			Condition string
		}{
			Condition: v.Condition,
		}
	}

	return struct {
		Image       string
		Ports       []string
		Healthcheck struct {
			Test []string
		}
		DependsOn map[string]struct {
			Condition string
		}
	}{
		Image: svc.Image,
		Ports: ports,
		Healthcheck: struct {
			Test []string
		}{
			Test: svc.Healthcheck.Test,
		},
		DependsOn: dependsOn,
	}
}

// assertComposeServiceExposesPort checks that the given service exposes the
// given port.
func assertComposeServiceExposesPort(t *testing.T, service, port string) {
	t.Helper()
	svc := loadComposeService(t, service)
	if !slices.Contains(svc.Ports, port) {
		t.Errorf("compose service %q does not expose port %q (exposed ports: %v)", service, port, svc.Ports)
	}
}
