package e2e_test

import (
	"regexp"
	"testing"
)

// In split, the prometheus chart scrapes every component, each target must be
// a Service the split backend render creates that reaches that component, and
// every target is labelled with it.
//
// Named with a Helm prefix (unlike the brief's suggested name) because
// compose_split_test.go already declares TestPrometheusSplitScrapesEveryComponent
// for the Compose-based prometheus.split.yml in the same package; this is the
// Helm-chart analogue of that check.
func TestHelmPrometheusSplitScrapesEveryComponent(t *testing.T) {
	backend := renderSplit(t)
	cm := findObject(t, renderChart(t, prometheusChart, "topology=split"), "ConfigMap", "")
	raw := rawOf(t, cm)

	blocks := regexp.MustCompile(`targets: \["([^"]+)"\]\s+labels:\s+service: observability-platform\s+component: ([a-z]+)`).FindAllStringSubmatch(raw, -1)
	if len(blocks) != 5 {
		t.Fatalf("split scrape config has %d labelled targets, want 5:\n%s", len(blocks), raw)
	}
	for _, b := range blocks {
		host, port := hostPortFromURL(t, "http://"+b[1])
		svc := findObject(t, backend, "Service", host)
		if !servicePortExists(svc, port) {
			t.Errorf("target %s: Service %s has no port %s", b[1], host, port)
		}
		got := workloadSelectedBy(backend, svc)
		if len(got) != 1 || splitNames[got[0]] != b[2] {
			t.Errorf("target %s is labelled component=%s but reaches %v", b[1], b[2], got)
		}
	}
}
