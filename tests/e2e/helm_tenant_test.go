package e2e_test

// Phase 6.5 packaging: the backend chart's tenants.* values reach exactly the
// targets that accept them, the overrides file is a ConfigMap mounted where the
// config names it, every rendered environment loads through config.Load, and
// the split ingesters carry the gateway's ring so each enforces its share of a
// tenant limit (ceil(limit x RF / N)) rather than the whole of it.

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	yaml "go.yaml.in/yaml/v3"

	"github.com/masonwheeler/observability-platform/internal/config"
)

const (
	tenantMountPath = "/etc/obs/tenant-overrides.yaml"
	tenantDataKey   = "tenant-overrides.yaml"
)

type tenantWorkload struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Data map[string]string `yaml:"data"`
	Spec struct {
		Template struct {
			Spec struct {
				Volumes []struct {
					Name      string `yaml:"name"`
					ConfigMap *struct {
						Name string `yaml:"name"`
					} `yaml:"configMap"`
				} `yaml:"volumes"`
				Containers []struct {
					VolumeMounts []struct {
						Name      string `yaml:"name"`
						MountPath string `yaml:"mountPath"`
						SubPath   string `yaml:"subPath"`
						ReadOnly  bool   `yaml:"readOnly"`
					} `yaml:"volumeMounts"`
				} `yaml:"containers"`
			} `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

func renderTenant(t *testing.T, sets ...string) []tenantWorkload {
	t.Helper()
	helmAvailable(t)
	args := []string{"template", "obs", backendChart}
	for _, s := range sets {
		args = append(args, "--set", s)
	}
	out, err := exec.Command("helm", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("helm %v: %v\n%s", sets, err, out)
	}
	var objs []tenantWorkload
	dec := yaml.NewDecoder(strings.NewReader(string(out)))
	for {
		var o tenantWorkload
		if err := dec.Decode(&o); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("rendered output is not valid YAML: %v", err)
		}
		if o.Kind != "" {
			objs = append(objs, o)
		}
	}
	return objs
}

// componentOf names the target a rendered workload runs.
func componentOf(topology, workload string) string {
	if topology == "all-in-one" {
		return "all-in-one"
	}
	return splitNames[workload]
}

func configMapOf(objs []tenantWorkload, name string) map[string]string {
	for _, o := range objs {
		if o.Kind == "ConfigMap" && o.Metadata.Name == name {
			return o.Data
		}
	}
	return nil
}

func workloads(objs []tenantWorkload) []tenantWorkload {
	var out []tenantWorkload
	for _, o := range objs {
		if o.Kind == "Deployment" || o.Kind == "StatefulSet" {
			out = append(out, o)
		}
	}
	return out
}

func overridesConfigMap(objs []tenantWorkload) (name string, data map[string]string) {
	for _, o := range objs {
		if _, ok := o.Data[tenantDataKey]; ok && o.Kind == "ConfigMap" {
			return o.Metadata.Name, o.Data
		}
	}
	return "", nil
}

var tenantTopologies = map[string][]string{
	"all-in-one": nil,
	"split":      {"topology=split"},
}

func TestTenantOverridesConfigMapIsMountedOnExactlyTheAcceptingTargets(t *testing.T) {
	accepts := []string{"all-in-one", "gateway", "ingester", "compactor"}
	for topology, base := range tenantTopologies {
		t.Run(topology, func(t *testing.T) {
			objs := renderTenant(t, append(slices.Clone(base), "tenants.overrides.team-b.max_active_series=5")...)
			cmName, data := overridesConfigMap(objs)
			if cmName == "" {
				t.Fatal("no overrides ConfigMap rendered for tenants.overrides.team-b")
			}
			var doc struct {
				Overrides map[string]map[string]any `yaml:"overrides"`
			}
			if err := yaml.Unmarshal([]byte(data[tenantDataKey]), &doc); err != nil {
				t.Fatal(err)
			}
			if got := doc.Overrides["team-b"]["max_active_series"]; got != 5 {
				t.Errorf("overrides.team-b.max_active_series = %v, want 5", got)
			}
			seen := 0
			for _, w := range workloads(objs) {
				component := componentOf(topology, w.Metadata.Name)
				want := slices.Contains(accepts, component)
				cfg := configMapOf(objs, w.Metadata.Name+"-config")
				if cfg == nil {
					t.Fatalf("no ConfigMap %s-config", w.Metadata.Name)
				}
				if got := cfg["OBS_TENANT_OVERRIDES_FILE"]; want != (got == tenantMountPath) || (!want && got != "") {
					t.Errorf("%s: OBS_TENANT_OVERRIDES_FILE = %q, accepting=%v", component, got, want)
				}
				mounted := false
				for _, c := range w.Spec.Template.Spec.Containers {
					for _, m := range c.VolumeMounts {
						if m.MountPath == tenantMountPath {
							mounted = true
							if m.SubPath != tenantDataKey || !m.ReadOnly {
								t.Errorf("%s: mount = %+v, want read-only subPath %s", component, m, tenantDataKey)
							}
							for _, v := range w.Spec.Template.Spec.Volumes {
								if v.Name == m.Name && (v.ConfigMap == nil || v.ConfigMap.Name != cmName) {
									t.Errorf("%s: volume %s does not come from ConfigMap %s", component, v.Name, cmName)
								}
							}
						}
					}
				}
				if mounted != want {
					t.Errorf("%s: overrides mounted = %v, want %v", component, mounted, want)
				}
				seen++
			}
			if seen == 0 {
				t.Fatal("no workloads rendered")
			}
		})
	}
}

func TestTenantNoOverridesMeansNoConfigMapAndNoEnv(t *testing.T) {
	for topology, base := range tenantTopologies {
		t.Run(topology, func(t *testing.T) {
			objs := renderTenant(t, base...)
			if name, _ := overridesConfigMap(objs); name != "" {
				t.Errorf("rendered overrides ConfigMap %s with no tenants.overrides", name)
			}
			for _, o := range objs {
				if _, ok := o.Data["OBS_TENANT_OVERRIDES_FILE"]; ok {
					t.Errorf("%s sets OBS_TENANT_OVERRIDES_FILE with no overrides", o.Metadata.Name)
				}
			}
			for _, w := range workloads(objs) {
				for _, c := range w.Spec.Template.Spec.Containers {
					for _, m := range c.VolumeMounts {
						if m.MountPath == tenantMountPath {
							t.Errorf("%s mounts the overrides file with no overrides", w.Metadata.Name)
						}
					}
				}
			}
		})
	}
}

func TestTenantSettingsReachExactlyTheirTargets(t *testing.T) {
	cases := []struct {
		key     string
		set     string
		want    string
		targets []string
	}{
		{"OBS_TENANT_REQUIRED", "tenants.required=true", "true", []string{"all-in-one", "gateway", "querier"}},
		{"OBS_MAX_ACTIVE_SERIES_PER_TENANT", "tenants.maxActiveSeriesPerTenant=300", "300", []string{"all-in-one", "ingester"}},
		{"OBS_MAX_ACTIVE_STREAMS_PER_TENANT", "tenants.maxActiveStreamsPerTenant=40", "40", []string{"all-in-one", "ingester"}},
		{"OBS_MAX_METRICS_PUSH_BYTES", "tenants.maxMetricsPushBytes=2048", "2048", []string{"all-in-one", "gateway"}},
		{"OBS_MAX_LOGS_PUSH_BYTES", "tenants.maxLogsPushBytes=4096", "4096", []string{"all-in-one", "gateway"}},
	}
	for topology, base := range tenantTopologies {
		for _, c := range cases {
			t.Run(topology+"/"+c.key, func(t *testing.T) {
				for _, o := range renderTenant(t, append(slices.Clone(base), c.set)...) {
					if o.Kind != "ConfigMap" || !strings.HasSuffix(o.Metadata.Name, "-config") {
						continue
					}
					component := componentOf(topology, strings.TrimSuffix(o.Metadata.Name, "-config"))
					got, has := o.Data[c.key]
					if slices.Contains(c.targets, component) {
						if got != c.want {
							t.Errorf("%s: %s = %q, want %q", component, c.key, got, c.want)
						}
					} else if has && got != "" {
						t.Errorf("%s: %s = %q; the target refuses it at startup", component, c.key, got)
					}
				}
			})
		}
	}
}

// Required defaults off: no component asks for a tenant header unless told to.
func TestTenantRequiredIsOffByDefault(t *testing.T) {
	for _, o := range renderTenant(t, "topology=split") {
		if v := o.Data["OBS_TENANT_REQUIRED"]; v != "" && v != "false" {
			t.Errorf("%s: OBS_TENANT_REQUIRED = %q by default", o.Metadata.Name, v)
		}
	}
}

// Every rendered environment loads through config.Load -- including the
// overrides file, which Load reads, so the rendered ConfigMap content is
// written out and the mount path swapped for it.
func TestTenantEnvironmentsLoadThroughConfig(t *testing.T) {
	sets := []string{
		"tenants.required=true",
		"tenants.maxActiveSeriesPerTenant=300",
		"tenants.maxActiveStreamsPerTenant=40",
		"tenants.maxMetricsPushBytes=2048",
		"tenants.maxLogsPushBytes=4096",
		"tenants.overrides.team-b.retention=24h",
		"tenants.overrides.team-b.max_active_series=200",
		"tenants.overrides.team-b.max_active_streams=20",
		"tenants.overrides.team-b.max_metrics_push_bytes=1024",
		"tenants.overrides.team-b.max_logs_push_bytes=2048",
	}
	for topology, base := range tenantTopologies {
		objs := renderTenant(t, append(slices.Clone(base), sets...)...)
		_, ov := overridesConfigMap(objs)
		file := filepath.Join(t.TempDir(), tenantDataKey)
		if err := os.WriteFile(file, []byte(ov[tenantDataKey]), 0o600); err != nil {
			t.Fatal(err)
		}
		for _, o := range objs {
			if o.Kind != "ConfigMap" || !strings.HasSuffix(o.Metadata.Name, "-config") {
				continue
			}
			t.Run(topology+"/"+o.Metadata.Name, func(t *testing.T) {
				for _, k := range []string{"OBS_TARGET", "OBS_INGESTER_URL", "OBS_STORE_URL", "OBS_QUERIER_URL", "OBS_REPLICATION_FACTOR", "OBS_INGESTER_SELF_URL", "OBS_STORE_REPLICATION_FACTOR", "OBS_STORE_SELF_URL", "OBS_TENANT_REQUIRED", "OBS_TENANT_OVERRIDES_FILE", "OBS_MAX_ACTIVE_SERIES_PER_TENANT", "OBS_MAX_ACTIVE_STREAMS_PER_TENANT", "OBS_MAX_METRICS_PUSH_BYTES", "OBS_MAX_LOGS_PUSH_BYTES"} {
					t.Setenv(k, "")
				}
				for k, v := range o.Data {
					if k == "OBS_TENANT_OVERRIDES_FILE" {
						v = file
					}
					t.Setenv(k, v)
				}
				// A split ingester also gets its own URL from its pod name.
				if o.Data["OBS_TARGET"] == "ingester" {
					t.Setenv("OBS_INGESTER_SELF_URL", strings.Split(o.Data["OBS_INGESTER_URL"], ",")[0])
				}
				if o.Data["OBS_TARGET"] == "store" {
					t.Setenv("OBS_STORE_SELF_URL", strings.Split(o.Data["OBS_STORE_URL"]+"http://x:8080", ",")[0])
				}
				cfg, err := config.Load()
				if err != nil {
					t.Fatalf("config.Load: %v", err)
				}
				if cfg.TenantOverridesFile != "" && cfg.TenantOverrides["team-b"].MaxActiveSeries == nil {
					t.Errorf("team-b override did not load: %+v", cfg.TenantOverrides)
				}
			})
		}
	}
}

// CARRIED: without the gateway's RF and ingester list an ingester divides each
// limit by nothing and enforces all of it, so the cluster admits N x the limit.
func TestSplitIngesterCarriesTheGatewaysRing(t *testing.T) {
	for _, sets := range [][]string{
		nil,
		{"split.ingester.replicas=4", "split.ingester.writeReplicas=3", "split.replicationFactor=2", "split.querier.replicationFactor=2"},
	} {
		objs := renderTenant(t, append([]string{"topology=split"}, sets...)...)
		gw := configMapOf(objs, "observability-backend-config")
		ing := configMapOf(objs, "observability-ingester-config")
		if gw == nil || ing == nil {
			t.Fatal("gateway or ingester ConfigMap missing")
		}
		for _, k := range []string{"OBS_REPLICATION_FACTOR", "OBS_INGESTER_URL"} {
			if gw[k] == "" {
				t.Fatalf("gateway %s is empty; the pin would compare nothing", k)
			}
			if ing[k] != gw[k] {
				t.Errorf("sets %v: ingester %s = %q, gateway = %q; they must be equal", sets, k, ing[k], gw[k])
			}
		}
	}
}

func TestTenantSchemaRefusesBadValues(t *testing.T) {
	helmAvailable(t)
	for _, set := range []string{
		"tenants.maxActiveSeriesPerTenant=-1",
		"tenants.maxActiveStreamsPerTenant=-1",
		"tenants.maxMetricsPushBytes=0",
		"tenants.maxLogsPushBytes=16777217",
		"tenants.bogus=1",
		"tenants.overrides.team-b.max_active_series=-5",
		"tenants.overrides.team-b.bogus=1",
		"tenants.required=maybe",
	} {
		out, err := exec.Command("helm", "template", "obs", backendChart, "--set", set).CombinedOutput()
		if err == nil {
			t.Errorf("--set %s rendered; the schema must refuse it", set)
			continue
		}
		if !strings.Contains(string(out), "tenants") {
			t.Errorf("--set %s: the refusal does not name tenants: %s", set, out)
		}
	}
	for _, set := range []string{"tenants.maxMetricsPushBytes=16777216", "tenants.maxActiveSeriesPerTenant=0"} {
		if out, err := exec.Command("helm", "template", "obs", backendChart, "--set", set).CombinedOutput(); err != nil {
			t.Errorf("--set %s must render: %v\n%s", set, err, out)
		}
	}
}

// The chart renders these from tenants.*; under the raw config map they would
// be emitted twice in one ConfigMap, or land on a target that refuses them.
func TestTenantKeysAreRefusedUnderRawConfig(t *testing.T) {
	helmAvailable(t)
	for _, key := range []string{"OBS_TENANT_REQUIRED", "OBS_TENANT_OVERRIDES_FILE", "OBS_MAX_ACTIVE_SERIES_PER_TENANT", "OBS_MAX_ACTIVE_STREAMS_PER_TENANT", "OBS_MAX_METRICS_PUSH_BYTES", "OBS_MAX_LOGS_PUSH_BYTES"} {
		for topology, base := range tenantTopologies {
			args := []string{"template", "obs", backendChart, "--set-string", "config." + key + "=1"}
			for _, b := range base {
				args = append(args, "--set", b)
			}
			out, err := exec.Command("helm", args...).CombinedOutput()
			if err == nil {
				t.Errorf("%s: helm template accepted config.%s", topology, key)
				continue
			}
			if !strings.Contains(string(out), key) || !strings.Contains(string(out), "tenants") {
				t.Errorf("%s: the refusal of %s must name the key and tenants.*: %s", topology, key, out)
			}
		}
	}
}

// The overrides file is mounted with subPath, which the kubelet never refreshes,
// so the pod template carries a checksum that rolls the pod when it changes.
func TestTenantOverridesChecksumRollsMountingPodsOnly(t *testing.T) {
	helmAvailable(t)
	annotated := func(sets ...string) map[string]string {
		args := []string{"template", "obs", backendChart, "--set", "topology=split"}
		for _, set := range sets {
			args = append(args, "--set", set)
		}
		out, err := exec.Command("helm", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("%v\n%s", err, out)
		}
		got := map[string]string{}
		dec := yaml.NewDecoder(strings.NewReader(string(out)))
		for {
			var o struct {
				Kind     string `yaml:"kind"`
				Metadata struct {
					Name string `yaml:"name"`
				} `yaml:"metadata"`
				Spec struct {
					Template struct {
						Metadata struct {
							Annotations map[string]string `yaml:"annotations"`
						} `yaml:"metadata"`
					} `yaml:"template"`
				} `yaml:"spec"`
			}
			if err := dec.Decode(&o); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				t.Fatal(err)
			}
			if o.Kind == "Deployment" || o.Kind == "StatefulSet" {
				got[splitNames[o.Metadata.Name]] = o.Spec.Template.Metadata.Annotations["checksum/tenant-overrides"]
			}
		}
		return got
	}
	if none := annotated(); none["gateway"] != "" || none["ingester"] != "" {
		t.Errorf("checksum present without overrides: %v", none)
	}
	a := annotated("tenants.overrides.team-b.max_active_series=5")
	b := annotated("tenants.overrides.team-b.max_active_series=6")
	for _, c := range []string{"gateway", "ingester", "compactor"} {
		if a[c] == "" {
			t.Errorf("%s: no checksum/tenant-overrides although it mounts the file", c)
		}
		if a[c] == b[c] {
			t.Errorf("%s: checksum did not change when an override value changed", c)
		}
	}
	for _, c := range []string{"querier", "store"} {
		if a[c] != "" {
			t.Errorf("%s: has checksum/tenant-overrides but does not mount the file", c)
		}
	}
}
