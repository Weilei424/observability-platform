package e2e_test

// Cluster-free validation of the Helm charts. These run inside `go test ./...`
// on every machine and every CI run — no cluster, no Docker.
//
// They exist for the failures `helm lint` cannot see. Lint checks one chart's
// syntax; it has no idea that the grafana chart's datasource URL names a
// Service the BACKEND chart is responsible for creating, or that a probe path
// corresponds to a route the server actually serves. Both of those are silent
// until something is deployed, and one of them (a bad probe path) fails every
// pod while every test stays green.

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	yaml "go.yaml.in/yaml/v3"
)

const (
	backendChart   = "../../deployments/helm/backend"
	grafanaChart   = "../../deployments/helm/grafana"
	producersChart = "../../deployments/helm/producers"

	routerPath = "../../internal/api/router.go"
	configPath = "../../internal/config/config.go"
)

// k8sObject is the subset of a rendered manifest these tests reason about.
type k8sObject struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Ports []struct {
			Port int    `yaml:"port"`
			Name string `yaml:"name"`
		} `yaml:"ports"`
		Template struct {
			Spec struct {
				Containers []struct {
					Name           string `yaml:"name"`
					StartupProbe   *probe `yaml:"startupProbe"`
					ReadinessProbe *probe `yaml:"readinessProbe"`
					LivenessProbe  *probe `yaml:"livenessProbe"`
					Env            []struct {
						Name  string `yaml:"name"`
						Value string `yaml:"value"`
					} `yaml:"env"`
				} `yaml:"containers"`
				Volumes []struct {
					Name      string `yaml:"name"`
					ConfigMap *struct {
						Name     string `yaml:"name"`
						Optional *bool  `yaml:"optional"`
					} `yaml:"configMap"`
				} `yaml:"volumes"`
			} `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
	Data map[string]string `yaml:"data"`
}

type probe struct {
	HTTPGet *struct {
		Path string `yaml:"path"`
	} `yaml:"httpGet"`
	Exec map[string]any `yaml:"exec"`
}

// helmAvailable skips the test when helm is not installed. These tests shell
// out, so a missing binary is an environment gap, not a failure of the charts.
func helmAvailable(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed; skipping chart validation")
	}
}

// render runs `helm template` and parses the result into objects.
func render(t *testing.T, chart string, setArgs ...string) []k8sObject {
	t.Helper()
	helmAvailable(t)

	args := []string{"template", "obs", chart}
	for _, s := range setArgs {
		args = append(args, "--set", s)
	}
	out, err := exec.Command("helm", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("helm template %s failed: %v\n%s", chart, err, out)
	}

	var objs []k8sObject
	dec := yaml.NewDecoder(strings.NewReader(string(out)))
	for {
		var o k8sObject
		err := dec.Decode(&o)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// A parse failure means helm rendered something kubectl would also
			// reject. Breaking here would silently pass the test on a truncated
			// render, so it is fatal.
			t.Fatalf("%s: rendered output is not valid YAML: %v", chart, err)
		}
		if o.Kind == "" {
			continue // empty document from a disabled toggle
		}
		if o.APIVersion == "" {
			t.Errorf("%s: rendered a %s with no apiVersion; kubectl would reject it", chart, o.Kind)
		}
		objs = append(objs, o)
	}
	if len(objs) == 0 {
		t.Fatalf("%s rendered no objects", chart)
	}
	return objs
}

func TestChartsLint(t *testing.T) {
	helmAvailable(t)
	cases := []struct {
		chart string
		set   []string
	}{
		{backendChart, nil},
		// The grafana chart refuses to render without a password by design, so
		// linting it requires supplying one.
		{grafanaChart, []string{"--set", "admin.password=lint-only"}},
		{producersChart, nil},
	}
	for _, tc := range cases {
		args := append([]string{"lint", tc.chart}, tc.set...)
		if out, err := exec.Command("helm", args...).CombinedOutput(); err != nil {
			t.Errorf("helm lint %s failed: %v\n%s", tc.chart, err, out)
		}
	}
}

// TestCrossChartBackendURLResolves is the reason this file exists.
//
// The grafana and producers charts each carry a backend URL that names a
// Service the BACKEND chart is responsible for creating. Helm never checks a
// claim across charts, so renaming the backend Service leaves both other charts
// pointing at a name nothing serves — Grafana shows "no data" and the producers
// log connection failures, with every chart still linting clean.
//
// This is the Kubernetes analogue of TestLokiDatasourceURLMatchesComposeBackend,
// which cross-references docker-compose.yml rather than trusting a literal.
func TestCrossChartBackendURLResolves(t *testing.T) {
	backend := render(t, backendChart)

	// Collect every Service name and the ports it exposes.
	services := map[string]map[int]bool{}
	for _, o := range backend {
		if o.Kind != "Service" {
			continue
		}
		ports := map[int]bool{}
		for _, p := range o.Spec.Ports {
			ports[p.Port] = true
		}
		services[o.Metadata.Name] = ports
	}
	if len(services) == 0 {
		t.Fatal("backend chart rendered no Services")
	}

	// Every backend URL asserted by the other two charts.
	urls := map[string]string{}
	for _, o := range render(t, grafanaChart, "admin.password=test") {
		if o.Kind != "ConfigMap" {
			continue
		}
		ds, ok := o.Data["datasources.yaml"]
		if !ok {
			continue
		}
		// Both datasources (prometheus and loki) carry a url. Keying by index
		// rather than a fixed string checks each one; a single key would let a
		// later entry overwrite an earlier broken one.
		n := 0
		for _, line := range strings.Split(ds, "\n") {
			line = strings.TrimSpace(line)
			if after, found := strings.CutPrefix(line, "url:"); found {
				n++
				urls[fmt.Sprintf("grafana datasource #%d", n)] = strings.TrimSpace(after)
			}
		}
		if n != 2 {
			t.Errorf("grafana datasources.yaml has %d url entries, want 2 (prometheus and loki)", n)
		}
	}
	for _, o := range render(t, producersChart) {
		for _, c := range o.Spec.Template.Spec.Containers {
			for _, e := range c.Env {
				if e.Name == "OBS_BACKEND_ADDR" {
					urls["producers/"+c.Name] = e.Value
				}
			}
		}
	}
	if len(urls) < 3 {
		t.Fatalf("expected at least 3 backend URLs (grafana + 2 producers), found %d: %v", len(urls), urls)
	}

	for who, url := range urls {
		host, port, ok := splitBackendURL(url)
		if !ok {
			t.Errorf("%s: %q is not an http://host:port URL", who, url)
			continue
		}
		ports, found := services[host]
		if !found {
			names := make([]string, 0, len(services))
			for n := range services {
				names = append(names, n)
			}
			t.Errorf("%s points at Service %q, which the backend chart does not create; it creates %v", who, host, names)
			continue
		}
		if !ports[port] {
			t.Errorf("%s points at %s:%d, but that Service exposes %v", who, host, port, ports)
		}
	}
}

// splitBackendURL parses http://host:port into its parts.
func splitBackendURL(url string) (host string, port int, ok bool) {
	rest, found := strings.CutPrefix(url, "http://")
	if !found {
		return "", 0, false
	}
	rest = strings.TrimSuffix(rest, "/")
	h, p, found := strings.Cut(rest, ":")
	if !found {
		return "", 0, false
	}
	n := 0
	for _, r := range p {
		if r < '0' || r > '9' {
			return "", 0, false
		}
		n = n*10 + int(r-'0')
	}
	return h, n, true
}

// TestBackendProbePathsExist checks every probe against the real router.
//
// A probe pointed at a path the server does not serve gets a 404, which the
// kubelet counts as a failure: the pod never becomes ready, or is restarted
// forever. Nothing else in this suite would notice, because the manifest is
// perfectly valid YAML.
func TestBackendProbePathsExist(t *testing.T) {
	routerSrc := readFile(t, routerPath)

	var checked int
	for _, o := range render(t, backendChart) {
		if o.Kind != "StatefulSet" {
			continue
		}
		for _, c := range o.Spec.Template.Spec.Containers {
			for name, p := range map[string]*probe{
				"startupProbe":   c.StartupProbe,
				"readinessProbe": c.ReadinessProbe,
				"livenessProbe":  c.LivenessProbe,
			} {
				if p == nil {
					t.Errorf("container %q has no %s", c.Name, name)
					continue
				}
				if p.Exec != nil {
					t.Errorf("container %q %s uses an exec probe; kubelet performs httpGet itself, so the distroless no-shell constraint does not apply here", c.Name, name)
				}
				if p.HTTPGet == nil {
					t.Errorf("container %q %s is not an httpGet probe", c.Name, name)
					continue
				}
				want := `r.Get("` + p.HTTPGet.Path + `"`
				if !strings.Contains(routerSrc, want) {
					t.Errorf("container %q %s probes %q, which is not a GET route in %s", c.Name, name, p.HTTPGet.Path, routerPath)
				}
				checked++
			}
		}
	}
	if checked == 0 {
		t.Fatal("no probes were checked; the StatefulSet render must have changed shape")
	}
}

// TestBackendConfigKeysAreReal guards against a typo that viper would swallow.
//
// The config loader calls v.AutomaticEnv() with the OBS prefix, so it reads only
// the keys it declares a default for. An OBS_* key that does not correspond to
// one is not an error — it is silently ignored, and the operator sees the
// default behavior while believing they configured something.
func TestBackendConfigKeysAreReal(t *testing.T) {
	configSrc := readFile(t, configPath)

	var checked int
	for _, o := range render(t, backendChart) {
		if o.Kind != "ConfigMap" {
			continue
		}
		for key := range o.Data {
			suffix, found := strings.CutPrefix(key, "OBS_")
			if !found {
				t.Errorf("ConfigMap key %q does not start with OBS_; the backend would never read it", key)
				continue
			}
			want := `SetDefault("` + strings.ToLower(suffix) + `"`
			if !strings.Contains(configSrc, want) {
				t.Errorf("ConfigMap key %q has no matching %s in %s; viper ignores unknown keys silently", key, want, configPath)
			}
			checked++
		}
	}
	if checked == 0 {
		t.Fatal("no ConfigMap keys were checked; the backend chart must have changed shape")
	}
}

// TestGrafanaChartShipsNoPassword pins the no-secrets-in-git rule.
func TestGrafanaChartShipsNoPassword(t *testing.T) {
	helmAvailable(t)

	// Rendering without any admin credential must fail, not quietly produce a
	// Grafana with a default login.
	out, err := exec.Command("helm", "template", "obs", grafanaChart).CombinedOutput()
	if err == nil {
		t.Error("grafana chart rendered with no admin.password and no admin.existingSecret; it must fail instead of shipping a default")
	}
	if !strings.Contains(string(out), "admin.password") {
		t.Errorf("failure message does not tell the operator what to set:\n%s", out)
	}

	// An operator-supplied Secret is honored, and no Secret is generated.
	for _, o := range render(t, grafanaChart, "admin.existingSecret=my-secret") {
		if o.Kind == "Secret" {
			t.Errorf("chart generated Secret %q even though admin.existingSecret was set", o.Metadata.Name)
		}
	}

	// No password literal in the shipped defaults.
	values := readFile(t, grafanaChart+"/values.yaml")
	for _, line := range strings.Split(values, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			continue
		}
		if strings.HasPrefix(trimmed, "password:") && trimmed != `password: ""` {
			t.Errorf("values.yaml ships a password default: %q", trimmed)
		}
	}
}

// TestGrafanaDashboardsVolumeIsNotOptional pins a deliberate design decision:
// the dashboards ConfigMap volume must NOT carry `optional: true`. If an
// operator forgets to create the grafana-dashboards ConfigMap, the pod must
// sit in ContainerCreating naming exactly what is missing, rather than
// Grafana starting happily with three empty dashboards. A CI job in a later
// task always creates that ConfigMap, so nothing there would catch an
// accidentally-optional mount — this test is what catches it.
func TestGrafanaDashboardsVolumeIsNotOptional(t *testing.T) {
	var found bool
	for _, o := range render(t, grafanaChart, "admin.password=test") {
		if o.Kind != "Deployment" {
			continue
		}
		for _, v := range o.Spec.Template.Spec.Volumes {
			if v.Name != "dashboards" {
				continue
			}
			found = true
			if v.ConfigMap == nil {
				t.Errorf("volume %q has no configMap source", v.Name)
				continue
			}
			if v.ConfigMap.Optional != nil && *v.ConfigMap.Optional {
				t.Errorf("volume %q configMap is optional: true; it must be required so a missing grafana-dashboards ConfigMap blocks pod startup instead of silently starting Grafana with no dashboards", v.Name)
			}
		}
	}
	if !found {
		t.Fatal(`no "dashboards" volume found; the grafana Deployment must have changed shape`)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}
