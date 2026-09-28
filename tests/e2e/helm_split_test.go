package e2e_test

import (
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"testing"

	yaml "go.yaml.in/yaml/v3"

	"github.com/masonwheeler/observability-platform/internal/config"
)

// componentOf reads the component a split object belongs to from its name.
var splitNames = map[string]string{
	"observability-backend":   "gateway",
	"observability-querier":   "querier",
	"observability-compactor": "compactor",
	"observability-ingester":  "ingester",
	"observability-store":     "store",
}

func renderSplit(t *testing.T, extra ...string) []k8sObject {
	t.Helper()
	return render(t, backendChart, append([]string{"topology=split"}, extra...)...)
}

func names(objs []k8sObject, kind string) []string {
	var out []string
	for _, o := range objs {
		if o.Kind == kind {
			out = append(out, o.Metadata.Name)
		}
	}
	slices.Sort(out)
	return out
}

// workloadSelectedBy returns the Deployment or StatefulSet whose pod labels
// satisfy svc's selector — the component the Service actually reaches.
func workloadSelectedBy(objs []k8sObject, svc *k8sObject) []string {
	var hits []string
	for _, o := range objs {
		if o.Kind != "Deployment" && o.Kind != "StatefulSet" {
			continue
		}
		match := len(svc.Spec.Selector) > 0
		for k, v := range svc.Spec.Selector {
			if s, _ := v.(string); o.Spec.Template.Metadata.Labels[k] != s {
				match = false
			}
		}
		if match {
			hits = append(hits, o.Metadata.Name)
		}
	}
	return hits
}

func TestSplitTopologyRendersTheFiveComponents(t *testing.T) {
	objs := renderSplit(t)
	if got, want := names(objs, "Deployment"), []string{"observability-backend", "observability-compactor", "observability-querier"}; !slices.Equal(got, want) {
		t.Errorf("Deployments = %v, want %v", got, want)
	}
	if got, want := names(objs, "StatefulSet"), []string{"observability-ingester", "observability-store"}; !slices.Equal(got, want) {
		t.Errorf("StatefulSets = %v, want %v (no all-in-one StatefulSet in split)", got, want)
	}
	wantSvcs := []string{"observability-backend", "observability-compactor", "observability-ingester", "observability-ingester-headless",
		"observability-querier", "observability-store", "observability-store-headless"}
	if got := names(objs, "Service"); !slices.Equal(got, wantSvcs) {
		t.Errorf("Services = %v, want %v", got, wantSvcs)
	}
	if out, err := exec.Command("helm", "lint", backendChart, "--set", "topology=split").CombinedOutput(); err != nil {
		t.Errorf("helm lint --set topology=split failed: %v\n%s", err, out)
	}
}

func TestSplitServicesSelectOnlyTheirComponent(t *testing.T) {
	objs := renderSplit(t)
	for i := range objs {
		svc := &objs[i]
		if svc.Kind != "Service" {
			continue
		}
		want := strings.TrimSuffix(svc.Metadata.Name, "-headless")
		if got := workloadSelectedBy(objs, svc); !slices.Equal(got, []string{want}) {
			t.Errorf("Service %s selects %v, want exactly [%s]", svc.Metadata.Name, got, want)
		}
	}
}

// Each ConfigMap must be an environment config.Load accepts — the right target,
// exactly the peers it needs — and every peer URL must reach that peer.
func TestSplitConfigMapsLoadAndReachTheirPeers(t *testing.T) {
	objs := renderSplit(t)
	peerKeys := map[string]string{"OBS_INGESTER_URL": "ingester", "OBS_STORE_URL": "store", "OBS_QUERIER_URL": "querier"}
	seen := 0
	for _, o := range objs {
		if o.Kind != "ConfigMap" {
			continue
		}
		component := splitNames[strings.TrimSuffix(o.Metadata.Name, "-config")]
		if component == "" {
			t.Errorf("unexpected ConfigMap %s in the split render", o.Metadata.Name)
			continue
		}
		seen++
		t.Run(component, func(t *testing.T) {
			if o.Data["OBS_TARGET"] != component {
				t.Fatalf("OBS_TARGET = %q, want %q", o.Data["OBS_TARGET"], component)
			}
			for _, k := range []string{"OBS_TARGET", "OBS_INGESTER_URL", "OBS_STORE_URL", "OBS_QUERIER_URL"} {
				t.Setenv(k, "")
			}
			for k, v := range o.Data {
				t.Setenv(k, v)
			}
			if _, err := config.Load(); err != nil {
				t.Fatalf("config.Load with %s's ConfigMap: %v", component, err)
			}
			for key, peer := range peerKeys {
				url, ok := o.Data[key]
				if !ok {
					continue
				}
				host, port := hostPortFromURL(t, url)
				svc := findObject(t, objs, "Service", host)
				if !servicePortExists(svc, port) {
					t.Errorf("%s = %s, but Service %s has no port %s", key, url, host, port)
				}
				if got := workloadSelectedBy(objs, svc); len(got) != 1 || splitNames[got[0]] != peer {
					t.Errorf("%s = %s reaches %v, want the %s", key, url, got, peer)
				}
			}
		})
	}
	if seen != 5 {
		t.Fatalf("rendered %d component ConfigMaps, want 5", seen)
	}
}

func TestSplitConfigMapKeysAreReal(t *testing.T) {
	configSrc := readFile(t, configPath)
	for _, o := range renderSplit(t) {
		if o.Kind != "ConfigMap" {
			continue
		}
		for key := range o.Data {
			want := `SetDefault("` + strings.ToLower(strings.TrimPrefix(key, "OBS_")) + `"`
			if !strings.HasPrefix(key, "OBS_") || !strings.Contains(configSrc, want) {
				t.Errorf("%s: key %q has no %s in %s", o.Metadata.Name, key, want, configPath)
			}
		}
	}
}

func TestSplitProbePathsExist(t *testing.T) {
	routerSrc := readFile(t, routerPath)
	checked := 0
	for _, o := range renderSplit(t) {
		if o.Kind != "Deployment" && o.Kind != "StatefulSet" {
			continue
		}
		for _, c := range o.Spec.Template.Spec.Containers {
			for name, p := range map[string]*probe{"startupProbe": c.StartupProbe, "readinessProbe": c.ReadinessProbe, "livenessProbe": c.LivenessProbe} {
				if p == nil || p.HTTPGet == nil {
					t.Errorf("%s container %q has no httpGet %s", o.Metadata.Name, c.Name, name)
					continue
				}
				if !strings.Contains(routerSrc, `r.Get("`+p.HTTPGet.Path+`"`) {
					t.Errorf("%s %s probes %q, which is not a GET route in %s", o.Metadata.Name, name, p.HTTPGet.Path, routerPath)
				}
				checked++
			}
		}
	}
	if checked != 15 {
		t.Fatalf("checked %d probes, want 15 (three for each of five components)", checked)
	}
}

func TestSplitRefusesMoreThanOneSingleton(t *testing.T) {
	helmAvailable(t)
	for component, mention := range map[string]string{"ingester": "6.2", "store": "6.2", "compactor": "two compactors"} {
		out, err := exec.Command("helm", "template", "backend", backendChart,
			"--set", "topology=split", "--set", "split."+component+".replicas=2").CombinedOutput()
		if err == nil {
			t.Errorf("split.%s.replicas=2 rendered; it must fail", component)
			continue
		}
		if !strings.Contains(string(out), mention) {
			t.Errorf("split.%s.replicas=2 failed without saying why (%q): %s", component, mention, out)
		}
	}
	out, err := exec.Command("helm", "template", "backend", backendChart, "--set", "topology=sharded").CombinedOutput()
	if err == nil || !regexp.MustCompile(`topology`).Match(out) {
		t.Errorf("an unknown topology must fail naming topology: %v %s", err, out)
	}
}

// The gateway takes observability-backend, so the grafana and producers charts'
// backend URLs keep resolving in split — to the gateway.
func TestSplitGatewayKeepsTheCrossChartContract(t *testing.T) {
	objs := renderSplit(t)
	for _, chart := range []string{grafanaChart, producersChart} {
		values := renderValues(t, chart)
		backend, _ := values["backend"].(map[string]any)
		url, _ := backend["url"].(string)
		host, port := hostPortFromURL(t, url)
		svc := findObject(t, objs, "Service", host)
		if !servicePortExists(svc, port) {
			t.Errorf("%s backend.url %s: no port %s on %s in split", chart, url, port, host)
		}
		if got := workloadSelectedBy(objs, svc); !slices.Equal(got, []string{"observability-backend"}) || splitNames[got[0]] != "gateway" {
			t.Errorf("%s backend.url %s reaches %v in split, want the gateway", chart, url, got)
		}
	}
}

// statefulPodSpec is the minimal shape needed to read
// terminationGracePeriodSeconds, which k8sObject does not decode.
type statefulPodSpec struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Template struct {
			Spec struct {
				TerminationGracePeriodSeconds *int64 `yaml:"terminationGracePeriodSeconds"`
			} `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

// TestSplitStatefulSetsGetAGracefulShutdownBudget pins the controller ruling:
// the ingester's graceful shutdown can take about 40s against a slow store (a
// 30s metrics flush plus a 10s logs flush), so Kubernetes' default 30s grace
// period would SIGKILL it mid-flush. The Compose split already uses 45s; this
// chart uses 60s for headroom.
func TestSplitStatefulSetsGetAGracefulShutdownBudget(t *testing.T) {
	helmAvailable(t)
	out, err := exec.Command("helm", "template", "obs", backendChart, "--set", "topology=split").CombinedOutput()
	if err != nil {
		t.Fatalf("helm template --set topology=split failed: %v\n%s", err, out)
	}

	checked := 0
	dec := yaml.NewDecoder(strings.NewReader(string(out)))
	for {
		var o statefulPodSpec
		if err := dec.Decode(&o); err != nil {
			break
		}
		if o.Kind != "StatefulSet" {
			continue
		}
		got := o.Spec.Template.Spec.TerminationGracePeriodSeconds
		if got == nil || *got != 60 {
			t.Errorf("%s: terminationGracePeriodSeconds = %v, want 60", o.Metadata.Name, got)
		}
		checked++
	}
	if checked != 2 {
		t.Fatalf("checked %d StatefulSets, want 2 (ingester and store)", checked)
	}
}
