package e2e_test

// Phase 6.5 packaging: both Compose stacks mount the demo tenant-overrides
// file read-only and name it on exactly the targets that accept it, the split
// ingesters share the gateway's ring settings (their per-tenant limit share is
// ceil(limit x RF / N)), and Grafana ships datasources that read as team-b.
// Checked-in files only -- no Docker.

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/masonwheeler/observability-platform/internal/config"
	yaml "go.yaml.in/yaml/v3"
)

const (
	composeAllInOnePath = "../../deployments/docker/docker-compose.yml"
	tenantOverridesPath = "../../deployments/docker/tenant-overrides.yaml"
	teamBDatasourcePath = "../../observability/grafana/datasources/team-b.yml"
	overridesMountPath  = "/etc/obs/tenant-overrides.yaml"
)

type composeTenantService struct {
	Environment map[string]string `yaml:"environment"`
	Volumes     []string          `yaml:"volumes"`
}

func loadComposeTenant(t *testing.T, path string) map[string]composeTenantService {
	t.Helper()
	b, err := os.ReadFile(filepath.FromSlash(path))
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Services map[string]composeTenantService `yaml:"services"`
	}
	if err := yaml.Unmarshal(b, &f); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return f.Services
}

func mountsOverrides(s composeTenantService) bool {
	for _, v := range s.Volumes {
		if strings.HasSuffix(v, ":"+overridesMountPath+":ro") && strings.Contains(v, "tenant-overrides.yaml") {
			return true
		}
	}
	return false
}

func TestComposeTenantOverridesOnExactlyTheAcceptingTargets(t *testing.T) {
	cases := []struct {
		path string
		with []string
	}{
		{composeAllInOnePath, []string{"backend"}},
		{composeSplitPath, []string{"gateway", "ingester-1", "ingester-2", "ingester-3", "compactor"}},
	}
	for _, c := range cases {
		svcs := loadComposeTenant(t, c.path)
		for name, s := range svcs {
			got := s.Environment["OBS_TENANT_OVERRIDES_FILE"]
			if slices.Contains(c.with, name) {
				if got != overridesMountPath {
					t.Errorf("%s %s OBS_TENANT_OVERRIDES_FILE = %q, want %s", c.path, name, got, overridesMountPath)
				}
				if !mountsOverrides(s) {
					t.Errorf("%s %s must mount tenant-overrides.yaml read-only at %s, volumes = %v", c.path, name, overridesMountPath, s.Volumes)
				}
			} else if got != "" || mountsOverrides(s) {
				t.Errorf("%s %s must not take the overrides file (its target refuses it)", c.path, name)
			}
		}
	}
}

// The demo leaves the tenant optional: the default tenant keeps working.
func TestComposeTenantNotRequiredInTheDemo(t *testing.T) {
	for _, p := range []string{composeAllInOnePath, composeSplitPath} {
		for name, s := range loadComposeTenant(t, p) {
			if v := s.Environment["OBS_TENANT_REQUIRED"]; v != "" && v != "false" {
				t.Errorf("%s %s sets OBS_TENANT_REQUIRED=%q; the demo's default tenant must keep working", p, name, v)
			}
		}
	}
}

// Each ingester enforces ceil(limit x RF / N): without the gateway's RF and
// member list it would enforce the full limit.
func TestComposeSplitIngestersKnowTheRFAndTheRing(t *testing.T) {
	svcs := loadComposeTenant(t, composeSplitPath)
	gw := svcs["gateway"].Environment
	for _, svc := range composeIngesters {
		env := svcs[svc].Environment
		if env["OBS_INGESTER_URL"] == "" || env["OBS_INGESTER_URL"] != gw["OBS_INGESTER_URL"] {
			t.Errorf("%s OBS_INGESTER_URL = %q, want the gateway's %q", svc, env["OBS_INGESTER_URL"], gw["OBS_INGESTER_URL"])
		}
		if env["OBS_REPLICATION_FACTOR"] == "" || env["OBS_REPLICATION_FACTOR"] != gw["OBS_REPLICATION_FACTOR"] {
			t.Errorf("%s OBS_REPLICATION_FACTOR = %q, want the gateway's %q", svc, env["OBS_REPLICATION_FACTOR"], gw["OBS_REPLICATION_FACTOR"])
		}
	}
}

// Every service environment still loads, with the overrides file the demo
// ships actually parsed (config.Load reads it).
func TestComposeTenantEnvironmentsLoad(t *testing.T) {
	abs, err := filepath.Abs(filepath.FromSlash(tenantOverridesPath))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{composeAllInOnePath, composeSplitPath} {
		for name, s := range loadComposeTenant(t, p) {
			if s.Environment["OBS_TENANT_OVERRIDES_FILE"] == "" || (p == composeSplitPath && composeRoleOf(name) == "") {
				continue
			}
			t.Run(filepath.Base(p)+"/"+name, func(t *testing.T) {
				for _, k := range []string{"OBS_TARGET", "OBS_INGESTER_URL", "OBS_STORE_URL", "OBS_QUERIER_URL", "OBS_INGESTER_SELF_URL", "OBS_REPLICATION_FACTOR", "OBS_STORE_REPLICATION_FACTOR", "OBS_LOGS_FLUSH_THRESHOLD_BYTES", "OBS_LOG_LEVEL", "OBS_INGESTER_TIMEOUT"} {
					t.Setenv(k, "")
				}
				t.Setenv("OBS_DATA_DIR", "data")
				for k, v := range s.Environment {
					t.Setenv(k, v)
				}
				t.Setenv("OBS_TENANT_OVERRIDES_FILE", abs)
				cfg, err := config.Load()
				if err != nil {
					t.Fatalf("config.Load: %v", err)
				}
				o, ok := cfg.TenantOverrides["team-b"]
				if !ok {
					t.Fatalf("overrides = %v, want a team-b entry", cfg.TenantOverrides)
				}
				if o.MaxActiveSeries == nil || *o.MaxActiveSeries != 200 {
					t.Errorf("team-b max_active_series = %v, want 200", o.MaxActiveSeries)
				}
			})
		}
	}
}

func TestTeamBDatasourcesProvisioning(t *testing.T) {
	b, err := os.ReadFile(filepath.FromSlash(teamBDatasourcePath))
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Datasources []struct {
			Name           string         `yaml:"name"`
			Type           string         `yaml:"type"`
			UID            string         `yaml:"uid"`
			URL            string         `yaml:"url"`
			IsDefault      bool           `yaml:"isDefault"`
			JSONData       map[string]any `yaml:"jsonData"`
			SecureJSONData map[string]any `yaml:"secureJsonData"`
		} `yaml:"datasources"`
	}
	if err := yaml.Unmarshal(b, &f); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"obs-prometheus-team-b": "prometheus", "obs-loki-team-b": "loki"}
	if len(f.Datasources) != len(want) {
		t.Fatalf("%d datasources, want %d", len(f.Datasources), len(want))
	}
	for _, ds := range f.Datasources {
		if want[ds.UID] != ds.Type {
			t.Errorf("%s: uid %q type %q unexpected", ds.Name, ds.UID, ds.Type)
		}
		if ds.URL != "http://backend:8080" {
			t.Errorf("%s url = %q, want http://backend:8080 (the gateway answers as backend in the split stack)", ds.Name, ds.URL)
		}
		if ds.IsDefault {
			t.Errorf("%s must not be the default datasource", ds.Name)
		}
		if ds.JSONData["httpHeaderName1"] != "X-Scope-OrgID" || ds.SecureJSONData["httpHeaderValue1"] != "team-b" {
			t.Errorf("%s header = %v / %v, want X-Scope-OrgID: team-b", ds.Name, ds.JSONData["httpHeaderName1"], ds.SecureJSONData["httpHeaderValue1"])
		}
	}
}
