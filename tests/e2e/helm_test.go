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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"slices"
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
		// Selector is a Service's pod selector: a flat map[string]string. A
		// Deployment/StatefulSet also has a spec.selector key, but shaped as
		// {matchLabels: {...}} — a nested map, not a flat string map — so this
		// field is typed loosely (map[string]any) to decode either shape
		// without error, and is only interpreted as a flat string map where the
		// caller has already checked Kind == "Service".
		Selector map[string]any `yaml:"selector"`
		Template struct {
			Metadata struct {
				Labels      map[string]string `yaml:"labels"`
				Annotations map[string]string `yaml:"annotations"`
			} `yaml:"metadata"`
			Spec struct {
				Containers []struct {
					Name           string `yaml:"name"`
					StartupProbe   *probe `yaml:"startupProbe"`
					ReadinessProbe *probe `yaml:"readinessProbe"`
					LivenessProbe  *probe `yaml:"livenessProbe"`
					Env            []struct {
						Name      string `yaml:"name"`
						Value     string `yaml:"value"`
						ValueFrom *struct {
							FieldRef *struct {
								FieldPath string `yaml:"fieldPath"`
							} `yaml:"fieldRef"`
						} `yaml:"valueFrom"`
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
// out, so a missing binary is an environment gap, not a failure of the charts
// — except in CI, where a missing helm means the "Set up Helm" step regressed
// or was removed, and a silent skip would look identical to a pass. CI sets
// the CI environment variable by convention (GitHub Actions, among others),
// so that's what distinguishes the two cases.
func helmAvailable(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		if os.Getenv("CI") != "" {
			t.Fatalf("helm is not installed, and CI is set; these chart tests must not silently skip in CI: %v", err)
		}
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

	// Collect every Service name, the ports it exposes, and its pod selector.
	services := map[string]map[int]bool{}
	selectors := map[string]map[string]string{}
	for _, o := range backend {
		if o.Kind != "Service" {
			continue
		}
		ports := map[int]bool{}
		for _, p := range o.Spec.Ports {
			ports[p.Port] = true
		}
		services[o.Metadata.Name] = ports
		sel := map[string]string{}
		for k, v := range o.Spec.Selector {
			if s, ok := v.(string); ok {
				sel[k] = s
			}
		}
		selectors[o.Metadata.Name] = sel
	}
	if len(services) == 0 {
		t.Fatal("backend chart rendered no Services")
	}

	// The StatefulSet's pod template labels, to check each Service selector
	// actually matches pods the backend chart creates — a Service can exist,
	// name the right port, and still select zero pods (a typo'd label, or a
	// selector that drifted from the pod template), leaving Grafana with a
	// perfectly valid-looking datasource pointed at an endpoint-less Service.
	// helm lint and every check above this pass regardless.
	var podLabels map[string]string
	for _, o := range backend {
		if o.Kind != "StatefulSet" {
			continue
		}
		podLabels = o.Spec.Template.Metadata.Labels
	}
	if len(podLabels) == 0 {
		t.Fatal("backend chart rendered no StatefulSet with pod template labels")
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
	// The exact set, not a minimum count. Four consumers point at the backend:
	// Grafana's two datasources and both producer containers. A `>= 3` bar
	// passes when one of them is missing entirely — drop OBS_BACKEND_ADDR from
	// the sample-app container and three URLs remain, all of them valid, while
	// the sample-app and logs dashboards go permanently empty. Naming the keys
	// also catches a container being renamed out from under this test, which a
	// count alone cannot see.
	wantURLs := []string{
		"grafana datasource #1",
		"grafana datasource #2",
		"producers/load-generator",
		"producers/sample-app",
	}
	for _, who := range wantURLs {
		if _, ok := urls[who]; !ok {
			t.Errorf("no backend URL found for %q; found %v", who, urls)
		}
	}
	if len(urls) != len(wantURLs) {
		t.Fatalf("expected exactly %d backend URLs (%v), found %d: %v",
			len(wantURLs), wantURLs, len(urls), urls)
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

		// A Service can name the right port and still route to nothing: its
		// selector must be a subset of the actual pod template labels, or
		// Kubernetes creates the Service with zero Endpoints and every request
		// through it fails, silently, forever.
		sel := selectors[host]
		if len(sel) == 0 {
			t.Errorf("%s points at Service %q, which has no pod selector; it would have no Endpoints", who, host)
			continue
		}
		for k, v := range sel {
			if podLabels[k] != v {
				t.Errorf("%s points at Service %q, whose selector %v is not satisfied by the backend pod template labels %v (key %q wants %q, pod has %q); the Service would have no Endpoints", who, host, sel, podLabels, k, v, podLabels[k])
			}
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

// backendSchemaConfigKeys returns the config keys the backend chart's
// values.schema.json allows, and whether it forbids everything else.
func backendSchemaConfigKeys(t *testing.T) (keys []string, closed bool) {
	t.Helper()
	var schema struct {
		Properties struct {
			Config struct {
				AdditionalProperties *bool                      `json:"additionalProperties"`
				Properties           map[string]json.RawMessage `json:"properties"`
			} `json:"config"`
		} `json:"properties"`
	}
	raw := readFile(t, backendChart+"/values.schema.json")
	if err := json.Unmarshal([]byte(raw), &schema); err != nil {
		t.Fatalf("values.schema.json does not parse: %v", err)
	}
	for k := range schema.Properties.Config.Properties {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	closed = schema.Properties.Config.AdditionalProperties != nil && !*schema.Properties.Config.AdditionalProperties
	return keys, closed
}

// TestBackendConfigOverridesAreValidated closes the gap TestBackendConfigKeysAreReal
// leaves open: that test reads the RENDERED ConfigMap, so it only ever sees the
// chart's own defaults. Any --set config.<anything> passed at install time went
// straight through into the ConfigMap.
//
// That matters because viper's AutomaticEnv reads only the keys config.go
// declares a default for. An unknown OBS_* variable is not an error — it is
// silently ignored, so `--set-string config.OBS_LOG_LEVLE=debug` installed
// cleanly, rendered the typo into the ConfigMap, and left the operator watching
// info-level logs while believing they had enabled debug.
//
// values.schema.json is what Helm checks BEFORE rendering, so the typo now fails
// the install. The key list is asserted against config.go in both directions
// here so the schema cannot drift from the code it is supposed to mirror.
func TestBackendConfigOverridesAreValidated(t *testing.T) {
	schemaKeys, closed := backendSchemaConfigKeys(t)
	if !closed {
		t.Error(`values.schema.json does not set "additionalProperties": false under config; unknown keys would be accepted again`)
	}
	if len(schemaKeys) == 0 {
		t.Fatal("values.schema.json declares no config keys")
	}

	// config.go is the source of truth: viper reads exactly the keys it
	// declares a default for.
	configSrc := readFile(t, configPath)
	var codeKeys []string
	for _, m := range regexp.MustCompile(`SetDefault\("([a-z0-9_]+)"`).FindAllStringSubmatch(configSrc, -1) {
		codeKeys = append(codeKeys, "OBS_"+strings.ToUpper(m[1]))
	}
	slices.Sort(codeKeys)
	if len(codeKeys) == 0 {
		t.Fatalf("found no SetDefault calls in %s; this test cannot check anything", configPath)
	}

	for _, k := range schemaKeys {
		if !slices.Contains(codeKeys, k) {
			t.Errorf("values.schema.json allows %q, which has no matching SetDefault in %s; viper would ignore it silently", k, configPath)
		}
	}
	for _, k := range codeKeys {
		if !slices.Contains(schemaKeys, k) {
			t.Errorf("%s declares %q but values.schema.json does not allow it, so operators cannot set a real backend option", configPath, k)
		}
	}

	helmAvailable(t)

	// The behavior all of the above exists for.
	out, err := exec.Command("helm", "template", "backend", backendChart,
		"--set-string", "config.OBS_LOG_LEVLE=debug").CombinedOutput()
	if err == nil {
		t.Errorf("helm template accepted a typo'd config key and rendered it:\n%s", out)
	} else if !strings.Contains(string(out), "OBS_LOG_LEVLE") {
		t.Errorf("the rejection does not name the offending key, so the operator cannot see the typo:\n%s", out)
	}

	// A real key must still be settable — the guard must not be a blanket ban
	// on overrides.
	found := false
	for _, o := range render(t, backendChart, "config.OBS_RETENTION=24h") {
		if o.Kind != "ConfigMap" {
			continue
		}
		if got := o.Data["OBS_RETENTION"]; got != "24h" {
			t.Errorf("OBS_RETENTION = %q after an override, want %q", got, "24h")
		}
		found = true
	}
	if !found {
		t.Error("no ConfigMap rendered with a valid config override")
	}
}

// TestBackendRejectsHTTPAddrOverride pins service.port as the single owner of
// the listen port.
//
// The ConfigMap derives OBS_HTTP_ADDR from service.port and then emits every
// .Values.config entry, so a config.OBS_HTTP_ADDR would render the key twice in
// one ConfigMap: rejected outright by some parsers, and silently last-write-wins
// in others — which leaves the server listening on one port while the Services
// and all three probes still point at another.
func TestBackendRejectsHTTPAddrOverride(t *testing.T) {
	helmAvailable(t)

	out, err := exec.Command("helm", "template", "backend", backendChart,
		"--set-string", "config.OBS_HTTP_ADDR=:9090").CombinedOutput()
	if err == nil {
		t.Fatalf("helm template accepted config.OBS_HTTP_ADDR; it must fail instead.\nRendered:\n%s", out)
	}
	if !strings.Contains(string(out), "service.port") {
		t.Errorf("the rejection message does not name service.port, so it does not tell the operator what to set instead: %s", out)
	}

	// The supported knob still works, and still produces exactly one key.
	for _, o := range render(t, backendChart, "service.port=9090") {
		if o.Kind != "ConfigMap" {
			continue
		}
		if got := o.Data["OBS_HTTP_ADDR"]; got != ":9090" {
			t.Errorf("OBS_HTTP_ADDR = %q with service.port=9090, want %q", got, ":9090")
		}
	}
}

// TestProducersCarryPodInstanceLabel pins the per-pod series identity that makes
// the replicas knobs safe.
//
// Both generators keep their counters in process memory. Two replicas emitting
// identical label sets are two independent counters under one series identity:
// samples interleave at the same timestamps and rate() reads every switch
// between them as a counter reset. The env var below is what the generators turn
// into an `instance` label, so a replicas value above 1 is only correct while it
// is present.
func TestProducersCarryPodInstanceLabel(t *testing.T) {
	var checked int
	for _, o := range render(t, producersChart) {
		for _, c := range o.Spec.Template.Spec.Containers {
			var found bool
			for _, e := range c.Env {
				if e.Name != "OBS_INSTANCE" {
					continue
				}
				found = true
				if e.ValueFrom == nil || e.ValueFrom.FieldRef == nil {
					t.Errorf("%s: OBS_INSTANCE must come from the downward API, not a literal value %q", c.Name, e.Value)
					continue
				}
				if got := e.ValueFrom.FieldRef.FieldPath; got != "metadata.name" {
					t.Errorf("%s: OBS_INSTANCE fieldPath = %q, want %q — anything shared between replicas defeats the label", c.Name, got, "metadata.name")
				}
			}
			if !found {
				t.Errorf("%s: no OBS_INSTANCE env var; with replicas > 1 its samples would collide with the other replicas' under one series identity", c.Name)
			}
			checked++
		}
	}
	if checked != 2 {
		t.Fatalf("checked %d producer containers, want 2 (sample-app and load-generator)", checked)
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

// grafanaPodTemplateAnnotations returns the Grafana Deployment's pod template
// annotations for one set of --set overrides.
func grafanaPodTemplateAnnotations(t *testing.T, setArgs ...string) map[string]string {
	t.Helper()
	for _, o := range render(t, grafanaChart, setArgs...) {
		if o.Kind == "Deployment" {
			return o.Spec.Template.Metadata.Annotations
		}
	}
	t.Fatalf("grafana chart rendered no Deployment with %v", setArgs)
	return nil
}

// TestGrafanaUpgradeRollsThePod covers what `helm upgrade` does NOT do on its
// own: a Deployment whose pod template is byte-identical creates no new
// ReplicaSet, so the running container keeps every value it read at startup.
//
// Grafana reads all three of these once, at startup: the provisioned
// datasources, the dashboard provider, and GF_SECURITY_ADMIN_PASSWORD from the
// Secret. Rotating admin.password used to rewrite the Secret and leave this
// template unchanged — `helm upgrade` reported success, and the old password
// kept working.
func TestGrafanaUpgradeRollsThePod(t *testing.T) {
	before := grafanaPodTemplateAnnotations(t, "admin.password=first-password")

	for _, key := range []string{"checksum/datasources", "checksum/provider", "checksum/admin-secret"} {
		if before[key] == "" {
			t.Errorf("pod template has no %s annotation; a change to that file would not restart Grafana. Have: %v", key, before)
		}
	}

	t.Run("rotating the password changes the pod template", func(t *testing.T) {
		after := grafanaPodTemplateAnnotations(t, "admin.password=second-password")
		if before["checksum/admin-secret"] == after["checksum/admin-secret"] {
			t.Errorf("checksum/admin-secret is %q for two different passwords; helm upgrade would not restart Grafana and the old password would stay live",
				before["checksum/admin-secret"])
		}
		// The unrelated inputs must NOT churn: an annotation that changes on
		// every render restarts Grafana on every upgrade and stops meaning
		// anything.
		if before["checksum/datasources"] != after["checksum/datasources"] {
			t.Error("checksum/datasources changed when only the password did")
		}
	})

	t.Run("repointing the backend changes the pod template", func(t *testing.T) {
		after := grafanaPodTemplateAnnotations(t, "admin.password=first-password", "backend.url=http://elsewhere:9090")
		if before["checksum/datasources"] == after["checksum/datasources"] {
			t.Error("checksum/datasources is unchanged for a different backend.url; Grafana would keep provisioning the old datasource until something else restarted it")
		}
	})

	t.Run("identical values render identically", func(t *testing.T) {
		again := grafanaPodTemplateAnnotations(t, "admin.password=first-password")
		for k, v := range before {
			if again[k] != v {
				t.Errorf("%s is not stable across identical renders: %q then %q — every upgrade would restart Grafana", k, v, again[k])
			}
		}
	})
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
