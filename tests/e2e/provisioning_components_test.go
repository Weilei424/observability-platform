package e2e_test

import (
	"os"
	"strings"
	"testing"

	yaml "go.yaml.in/yaml/v3"
)

// edgeFilter restricts HTTP panels to the process a client talks to. In the
// split topology a proxied request is served twice — by the gateway and by the
// querier or ingester — and counting both would double every rate.
const edgeFilter = `component=~"all-in-one|gateway"`

func TestInternalsDashboardCountsHTTPAtTheEdgeOnly(t *testing.T) {
	edgePanels := map[string]bool{
		"HTTP Request Rate by Route":  true,
		"Query Latency p50/p95/p99":   true,
		"Total HTTP Errors by Status": true,
	}
	seen := 0
	for _, p := range loadDashboard(t, internalsDashboardPath).Panels {
		if !edgePanels[p.Title] {
			continue
		}
		seen++
		for _, tgt := range p.Targets {
			if !strings.Contains(tgt.Expr, edgeFilter) {
				t.Errorf("panel %q target %s lacks %s: %q", p.Title, tgt.RefID, edgeFilter, tgt.Expr)
			}
		}
	}
	if seen != len(edgePanels) {
		t.Fatalf("found %d of the %d HTTP panels; one was renamed", seen, len(edgePanels))
	}
}

func TestInternalsDashboardShowsComponentHealth(t *testing.T) {
	for _, p := range loadDashboard(t, internalsDashboardPath).Panels {
		if p.Title != "Component Health" {
			continue
		}
		for _, tgt := range p.Targets {
			if strings.HasPrefix(tgt.Expr, "up{") && strings.Contains(tgt.LegendFormat, "{{component}}") {
				return
			}
		}
		t.Fatalf("Component Health does not plot up by component: %+v", p.Targets)
	}
	t.Fatal(`no "Component Health" panel`)
}

func TestComposeScrapeConfigLabelsTheComponent(t *testing.T) {
	raw, err := os.ReadFile("../../observability/prometheus/prometheus.yml")
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct {
		ScrapeConfigs []struct {
			StaticConfigs []struct {
				Labels map[string]string `yaml:"labels"`
			} `yaml:"static_configs"`
		} `yaml:"scrape_configs"`
	}
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, sc := range cfg.ScrapeConfigs {
		for _, st := range sc.StaticConfigs {
			n++
			if st.Labels["component"] != "all-in-one" {
				t.Errorf("an all-in-one scrape target is labelled component=%q, want all-in-one", st.Labels["component"])
			}
		}
	}
	if n == 0 {
		t.Fatal("no static scrape targets found")
	}
}

func TestHelmScrapeConfigLabelsTheComponent(t *testing.T) {
	cm := findObject(t, renderChart(t, prometheusChart), "ConfigMap", "")
	if !strings.Contains(rawOf(t, cm), "component: all-in-one") {
		t.Error("the prometheus chart's all-in-one scrape target carries no component label")
	}
}
