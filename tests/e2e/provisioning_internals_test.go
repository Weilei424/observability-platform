package e2e_test

// Static validation for the self-observability datasource and dashboard. Reads
// the checked-in files only — no Docker, no Grafana — so it runs in `go test ./...`.
//
// This dashboard is the one whose queries are NOT bound by the backend's PromQL
// subset: it reads a real Prometheus. See
// TestInternalsDashboardIsExemptFromTheSubsetRule below.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
