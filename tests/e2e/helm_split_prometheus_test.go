package e2e_test

import (
	"regexp"
	"strings"
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
	if len(blocks) != 7 {
		t.Fatalf("split scrape config has %d labelled targets (four components plus three ingester pods), want 7:\n%s", len(blocks), raw)
	}
	for _, b := range blocks {
		host, port := hostPortFromURL(t, "http://"+b[1])
		svcName, err := peerService(backend, host)
		if err != nil {
			t.Errorf("target %s: %v", b[1], err)
			continue
		}
		svc := findObject(t, backend, "Service", svcName)
		if !servicePortExists(svc, port) {
			t.Errorf("target %s: Service %s has no port %s", b[1], svcName, port)
		}
		got := workloadSelectedBy(backend, svc)
		if len(got) != 1 || splitNames[got[0]] != b[2] {
			t.Errorf("target %s is labelled component=%s but reaches %v", b[1], b[2], got)
		}
	}
}

// The ingester scrape targets are the ring: they must be exactly the list the
// backend chart gives the gateway at its default replicas, in the same order.
func TestHelmPrometheusIngesterTargetsMatchTheRing(t *testing.T) {
	var ring string
	for _, o := range renderSplit(t) {
		if o.Kind == "ConfigMap" && o.Data["OBS_TARGET"] == "gateway" {
			ring = o.Data["OBS_INGESTER_URL"]
		}
	}
	if ring == "" {
		t.Fatal("gateway ConfigMap has no OBS_INGESTER_URL")
	}
	raw := rawOf(t, findObject(t, renderChart(t, prometheusChart, "topology=split"), "ConfigMap", ""))
	var got []string
	for _, b := range regexp.MustCompile(`targets: \["([^"]+)"\]\s+labels:\s+service: observability-platform\s+component: ingester\b`).FindAllStringSubmatch(raw, -1) {
		got = append(got, "http://"+b[1])
	}
	if strings.Join(got, ",") != ring {
		t.Errorf("ingester scrape targets %v, want the gateway's ring %s", got, ring)
	}
}
