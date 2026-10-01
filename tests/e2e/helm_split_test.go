package e2e_test

import (
	"errors"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"testing"

	yaml "go.yaml.in/yaml/v3"

	"github.com/masonwheeler/observability-platform/internal/config"
)

// splitNames reads the component a split object belongs to from its name.
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
	if out, err := exec.Command("helm", "lint", "--strict", backendChart, "--set", "topology=split").CombinedOutput(); err != nil {
		t.Errorf("helm lint --strict --set topology=split failed: %v\n%s", err, out)
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

// podSpecObject is the minimal shape needed to read
// terminationGracePeriodSeconds and volume wiring, none of which k8sObject
// decodes.
type podSpecObject struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Template struct {
			Spec struct {
				TerminationGracePeriodSeconds *int64 `yaml:"terminationGracePeriodSeconds"`
				Volumes                       []struct {
					Name string `yaml:"name"`
				} `yaml:"volumes"`
				Containers []struct {
					Name         string `yaml:"name"`
					VolumeMounts []struct {
						Name string `yaml:"name"`
					} `yaml:"volumeMounts"`
				} `yaml:"containers"`
			} `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

// renderPodSpecObjects runs `helm template` and decodes the result into
// podSpecObject, the same way render() does for k8sObject — used only for
// the pod-spec fields k8sObject does not decode (terminationGracePeriodSeconds,
// volumes, volumeMounts).
func renderPodSpecObjects(t *testing.T, extra ...string) []podSpecObject {
	t.Helper()
	helmAvailable(t)
	args := append([]string{"template", "obs", backendChart}, extra...)
	out, err := exec.Command("helm", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("helm %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}

	var objs []podSpecObject
	dec := yaml.NewDecoder(strings.NewReader(string(out)))
	for {
		var o podSpecObject
		err := dec.Decode(&o)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("rendered output is not valid YAML: %v", err)
		}
		if o.Kind == "" {
			continue
		}
		objs = append(objs, o)
	}
	return objs
}

// TestSplitStatefulSetsGetAGracefulShutdownBudget pins the controller ruling:
// the ingester's graceful shutdown can take about 40s against a slow store (a
// 30s metrics flush plus a 10s logs flush), so Kubernetes' default 30s grace
// period would SIGKILL it mid-flush. The Compose split already uses 60s; this
// chart uses 60s for headroom.
func TestSplitStatefulSetsGetAGracefulShutdownBudget(t *testing.T) {
	checked := 0
	for _, o := range renderPodSpecObjects(t, "--set", "topology=split") {
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

// baseAllInOneChecksumConfig is the StatefulSet's checksum/config annotation
// rendered from commit 2016c11 (the tip of main immediately before this
// task), with topology's default values (i.e. no --set at all — the render
// every existing all-in-one install upgrades from). Computed once via:
//
//	git archive 2016c11 deployments/helm/backend | tar -x -C <scratch>
//	helm template obs <scratch>/deployments/helm/backend | grep checksum/config
//
// which printed 9fc5ea170f84250f2fe4945ee16a45f65244b8925e15da0af7936b5ae1bee3a2.
//
// A hardcoded hash is brittle to any *intentional* change to
// templates/configmap.yaml (a real key rename, a comment edit inside the
// hashed include, etc.) — such a change must update this constant along with
// it, and the failure message below says so. The alternative of asserting
// two renders (before/after this task's commits) produce the same value
// would not survive past this one commit range, and comparing against a
// second `helm template` of a fresh git checkout on every test run is slower
// and reintroduces a git dependency for no benefit over a literal recorded
// once. A hardcoded hash is preferred here because the ConfigMap's rendered
// content for default values is not expected to change again in this task.
const baseAllInOneChecksumConfig = "9fc5ea170f84250f2fe4945ee16a45f65244b8925e15da0af7936b5ae1bee3a2"

// TestAllInOneChecksumConfigUnchangedFromBase is the regression test for the
// bug fixed in configmap.yaml: `{{- if eq .Values.topology "all-in-one" }}`
// (no trailing `-`) emitted a leading newline into the raw output of
// `include ".../configmap.yaml"` that the checksum hashes, even though Helm
// trims that newline from the written manifest — so the ConfigMap object was
// byte-identical while checksum/config differed. Every `helm upgrade` of an
// existing all-in-one install would then roll its single-replica
// StatefulSet with no actual config change. If this ever regresses again —
// a stray newline from any future edit to the file the include reads, in
// either direction — this test catches it without needing a live cluster.
func TestAllInOneChecksumConfigUnchangedFromBase(t *testing.T) {
	var found string
	for _, o := range render(t, backendChart) {
		if o.Kind != "StatefulSet" {
			continue
		}
		found = o.Spec.Template.Metadata.Annotations["checksum/config"]
	}
	if found == "" {
		t.Fatal("no StatefulSet with a checksum/config annotation was rendered; the all-in-one chart must have changed shape")
	}
	if found != baseAllInOneChecksumConfig {
		t.Errorf("checksum/config = %s, want %s (the value at commit 2016c11 for default values); "+
			"every all-in-one install would restart on upgrade with no config change. "+
			"If templates/configmap.yaml genuinely changed on purpose, recompute and update baseAllInOneChecksumConfig",
			found, baseAllInOneChecksumConfig)
	}
}

// TestSplitStatelessComponentsMountNoVolume pins the gateway, querier, and
// compactor never touching the data dir: they must answer ready without
// disk, so they mount no volume and declare no volumeMounts.
func TestSplitStatelessComponentsMountNoVolume(t *testing.T) {
	checked := 0
	for _, o := range renderPodSpecObjects(t, "--set", "topology=split") {
		if o.Kind != "Deployment" {
			continue
		}
		if len(o.Spec.Template.Spec.Volumes) != 0 {
			t.Errorf("%s: pod template has %d volumes, want 0", o.Metadata.Name, len(o.Spec.Template.Spec.Volumes))
		}
		for _, c := range o.Spec.Template.Spec.Containers {
			if len(c.VolumeMounts) != 0 {
				t.Errorf("%s container %q has %d volumeMounts, want 0", o.Metadata.Name, c.Name, len(c.VolumeMounts))
			}
			checked++
		}
	}
	if checked != 3 {
		t.Fatalf("checked %d stateless containers, want 3 (gateway, querier, compactor)", checked)
	}
}

// TestSplitRejectsTemplateOwnedConfigKeys pins Task 12's refusal in the split
// topology too: the chart, not the operator, decides each component's target
// and peer URLs, and split-configmaps.yaml has its own render path that must
// not bypass that guard.
func TestSplitRejectsTemplateOwnedConfigKeys(t *testing.T) {
	helmAvailable(t)
	for _, key := range []string{"config.OBS_TARGET=gateway", "config.OBS_STORE_URL=http://elsewhere:8080"} {
		out, err := exec.Command("helm", "template", "backend", backendChart,
			"--set", "topology=split", "--set-string", key).CombinedOutput()
		if err == nil {
			t.Errorf("--set-string %s rendered with topology=split; it must fail", key)
			continue
		}
		if !strings.Contains(string(out), "the chart decides each component's target and peer URLs") {
			t.Errorf("--set-string %s failed without Task 12's message: %s", key, out)
		}
	}
}

// Kubernetes name limits the chart must respect: a Service name is a DNS-1035
// label of at most 63 characters, and a StatefulSet name at most 52, because
// its pods carry a controller-revision-hash label of the name plus an
// 11-character suffix, and a label value is at most 63.
var dns1035 = regexp.MustCompile(`^[a-z]([-a-z0-9]*[a-z0-9])?$`)

const (
	maxServiceName     = 63
	maxStatefulSetName = 52
)

// A long fullnameOverride must still render valid, distinct names in both
// topologies, and every name one object uses to reach another — a
// StatefulSet's serviceName, a peer URL — must name a Service that exists.
func TestLongFullnameOverrideRendersValidDistinctNames(t *testing.T) {
	for _, name := range []string{
		strings.Repeat("a", 52),              // the longest accepted
		strings.Repeat("a", 44) + "-backend", // the same, with the "-backend" split names drop
		strings.Repeat("a", 46),              // Codex's case: an over-long ingester headless Service
		strings.Repeat("a", 43) + "-storex",  // truncates to the store's prefix, yet stays distinct from it
	} {
		for _, topology := range []string{"all-in-one", "split"} {
			t.Run(fmt.Sprintf("%s/%s", topology, name), func(t *testing.T) {
				objs := render(t, backendChart, "topology="+topology, "fullnameOverride="+name)
				seen := map[string]bool{}
				services := map[string]bool{}
				for _, o := range objs {
					key := o.Kind + "/" + o.Metadata.Name
					if seen[key] {
						t.Errorf("two objects named %s", key)
					}
					seen[key] = true
					if o.Kind == "Service" {
						services[o.Metadata.Name] = true
						if len(o.Metadata.Name) > maxServiceName || !dns1035.MatchString(o.Metadata.Name) {
							t.Errorf("Service %q (%d chars) is not a DNS-1035 label of at most %d", o.Metadata.Name, len(o.Metadata.Name), maxServiceName)
						}
					}
					if o.Kind == "StatefulSet" && len(o.Metadata.Name) > maxStatefulSetName {
						t.Errorf("StatefulSet %q is %d chars, want at most %d", o.Metadata.Name, len(o.Metadata.Name), maxStatefulSetName)
					}
				}
				if !services[name] {
					t.Errorf("no Service named %q: the name other charts point at must be fullnameOverride exactly", name)
				}
				for _, o := range objs {
					if o.Kind == "StatefulSet" && !services[o.Spec.ServiceName] {
						t.Errorf("StatefulSet %s: serviceName %q is not a rendered Service", o.Metadata.Name, o.Spec.ServiceName)
					}
					if o.Kind != "ConfigMap" {
						continue
					}
					for _, k := range []string{"OBS_INGESTER_URL", "OBS_STORE_URL", "OBS_QUERIER_URL"} {
						if v := o.Data[k]; v != "" {
							host := strings.Split(strings.TrimPrefix(v, "http://"), ":")[0]
							if !services[host] {
								t.Errorf("ConfigMap %s: %s = %q names no rendered Service", o.Metadata.Name, k, v)
							}
						}
					}
				}
				if out, err := exec.Command("helm", "lint", "--strict", backendChart,
					"--set", "topology="+topology, "--set", "fullnameOverride="+name).CombinedOutput(); err != nil {
					t.Errorf("helm lint --strict: %v\n%s", err, out)
				}
			})
		}
	}
}

// fullnameOverride names the Service other charts point at, so it is never
// truncated: one too long for the StatefulSet limit fails the render instead.
func TestOverlongFullnameOverrideFailsTheRender(t *testing.T) {
	helmAvailable(t)
	for _, topology := range []string{"all-in-one", "split"} {
		out, err := exec.Command("helm", "template", "obs", backendChart, "--set", "topology="+topology,
			"--set", "fullnameOverride="+strings.Repeat("a", maxStatefulSetName+1)).CombinedOutput()
		if err == nil || !strings.Contains(string(out), "fullnameOverride") {
			t.Errorf("%s: a %d-char fullnameOverride rendered (err %v), want a failure naming fullnameOverride\n%.300s",
				topology, maxStatefulSetName+1, err, out)
		}
	}
}

// A split component's name truncates the shared prefix, so an override made of
// that truncated prefix plus a component's own suffix would name the component
// exactly what the gateway is named. Two prefix lengths reach it: 43 characters
// survive trunc 43 whole, and 42 do once trimSuffix drops the hyphen trunc 43
// kept. Split mode refuses that render; all-in-one has no component names and
// renders it.
func TestFullnameOverrideCollidingWithAComponentFailsSplit(t *testing.T) {
	helmAvailable(t)
	for _, component := range []string{"ingester", "querier", "store", "compactor"} {
		for _, prefix := range []int{42, 43} {
			name := strings.Repeat("a", prefix) + "-" + component
			if len(name) > maxStatefulSetName {
				continue // 43 + "-compactor": refused by the length cap instead
			}
			checkCollisionRefused(t, name, component)
		}
	}
}

// checkCollisionRefused requires split mode to refuse fullnameOverride name for
// colliding with component, and all-in-one to render it.
func checkCollisionRefused(t *testing.T, name, component string) {
	t.Helper()
	out, err := exec.Command("helm", "template", "obs", backendChart, "--set", "topology=split",
		"--set", "fullnameOverride="+name).CombinedOutput()
	if err == nil || !strings.Contains(string(out), "collides with the "+component+" component") {
		t.Errorf("split, fullnameOverride %q: rendered (err %v), want a failure saying it collides with the %s component\n%.300s",
			name, err, component, out)
	}
	if _, err := exec.Command("helm", "template", "obs", backendChart, "--set", "topology=all-in-one",
		"--set", "fullnameOverride="+name).CombinedOutput(); err != nil {
		t.Errorf("all-in-one, fullnameOverride %q: %v, want it to render", name, err)
	}
}
