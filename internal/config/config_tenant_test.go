package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var tenantEnvKeys = []string{"OBS_TENANT_REQUIRED", "OBS_TENANT_OVERRIDES_FILE", "OBS_MAX_ACTIVE_SERIES_PER_TENANT", "OBS_MAX_ACTIVE_STREAMS_PER_TENANT", "OBS_MAX_METRICS_PUSH_BYTES", "OBS_MAX_LOGS_PUSH_BYTES"}

func clearTenantEnv(t *testing.T) {
	t.Helper()
	clearStoreEnv(t)
	for _, k := range tenantEnvKeys {
		t.Setenv(k, "")
	}
}

func writeOverrides(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "overrides.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// targetEnv returns the peer settings a target needs to load.
func targetEnv(target string) map[string]string {
	env := map[string]string{"OBS_TARGET": target}
	switch target {
	case "gateway":
		env["OBS_INGESTER_URL"] = "http://i:1"
		env["OBS_QUERIER_URL"] = "http://q:1"
	case "ingester", "compactor":
		env["OBS_STORE_URL"] = "http://s:1"
	case "querier":
		env["OBS_INGESTER_URL"] = "http://i:1"
		env["OBS_STORE_URL"] = "http://s:1"
	}
	return env
}

func TestTenantDefaults(t *testing.T) {
	clearTenantEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TenantRequired || cfg.MaxMetricsPushBytes != 1<<20 || cfg.MaxLogsPushBytes != 4<<20 ||
		cfg.MaxActiveSeriesPerTenant != 0 || cfg.MaxActiveStreamsPerTenant != 0 || cfg.TenantOverridesFile != "" {
		t.Fatalf("defaults = %+v", cfg)
	}
}

func TestTenantSettingsOnTheirTargets(t *testing.T) {
	all := []string{"all-in-one", "gateway", "ingester", "querier", "store", "compactor"}
	file := writeOverrides(t, "overrides: {}\n")
	for _, tc := range []struct {
		key, val string
		loads    []string
	}{
		{"OBS_TENANT_REQUIRED", "true", []string{"all-in-one", "gateway", "querier"}},
		{"OBS_TENANT_OVERRIDES_FILE", file, []string{"all-in-one", "gateway", "ingester", "compactor"}},
		{"OBS_MAX_ACTIVE_SERIES_PER_TENANT", "5", []string{"all-in-one", "ingester"}},
		{"OBS_MAX_ACTIVE_STREAMS_PER_TENANT", "5", []string{"all-in-one", "ingester"}},
		{"OBS_MAX_METRICS_PUSH_BYTES", "1024", []string{"all-in-one", "gateway"}},
		{"OBS_MAX_LOGS_PUSH_BYTES", "1024", []string{"all-in-one", "gateway"}},
	} {
		for _, target := range all {
			t.Run(tc.key+"/"+target, func(t *testing.T) {
				clearTenantEnv(t)
				for k, v := range targetEnv(target) {
					t.Setenv(k, v)
				}
				t.Setenv(tc.key, tc.val)
				_, err := Load()
				want := false
				for _, l := range tc.loads {
					want = want || l == target
				}
				if want && err != nil {
					t.Fatalf("want load, got %v", err)
				}
				if !want && (err == nil || !strings.Contains(err.Error(), tc.key+" is set but target "+target+" does not use it")) {
					t.Fatalf("err = %v, want a refusal", err)
				}
			})
		}
	}
}

func TestTenantSettingsBounds(t *testing.T) {
	for _, tc := range []struct{ key, val string }{
		{"OBS_MAX_ACTIVE_SERIES_PER_TENANT", "-1"},
		{"OBS_MAX_ACTIVE_STREAMS_PER_TENANT", "-1"},
		{"OBS_MAX_ACTIVE_SERIES_PER_TENANT", "bogus"},
		{"OBS_MAX_METRICS_PUSH_BYTES", "0"},
		{"OBS_MAX_METRICS_PUSH_BYTES", "16777217"},
		{"OBS_MAX_LOGS_PUSH_BYTES", "0"},
		{"OBS_MAX_LOGS_PUSH_BYTES", "16777217"},
		{"OBS_MAX_LOGS_PUSH_BYTES", "bogus"},
		{"OBS_TENANT_REQUIRED", "bogus"},
	} {
		t.Run(tc.key+"="+tc.val, func(t *testing.T) {
			clearTenantEnv(t)
			t.Setenv(tc.key, tc.val)
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("err = %v, want it to name %s", err, tc.key)
			}
		})
	}
}

func TestTenantSettingsFromYAML(t *testing.T) {
	clearTenantEnv(t)
	t.Setenv("OBS_TARGET", "ingester")
	t.Setenv("OBS_STORE_URL", "http://s:1")
	if _, err := loadWithYAML(t, "tenant_required: true\n"); err == nil || !strings.Contains(err.Error(), "OBS_TENANT_REQUIRED") {
		t.Fatalf("err = %v, want a refusal", err)
	}
	cfg, err := loadWithYAML(t, "max_active_series_per_tenant: 3\n")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxActiveSeriesPerTenant != 3 {
		t.Fatalf("MaxActiveSeriesPerTenant = %d", cfg.MaxActiveSeriesPerTenant)
	}
}

func TestIngesterTakesReplicationFactorAndRing(t *testing.T) {
	clearTenantEnv(t)
	t.Setenv("OBS_TARGET", "ingester")
	t.Setenv("OBS_STORE_URL", "http://s:1")
	t.Setenv("OBS_REPLICATION_FACTOR", "3")
	t.Setenv("OBS_INGESTER_URL", "http://i1:1,http://i2:1,http://i3:1")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ReplicationFactor != 3 || len(cfg.IngesterURLs) != 3 {
		t.Fatalf("RF = %d, IngesterURLs = %v", cfg.ReplicationFactor, cfg.IngesterURLs)
	}
	t.Setenv("OBS_INGESTER_URL", "")
	cfg, err = Load()
	if err != nil {
		t.Fatalf("without OBS_INGESTER_URL: %v", err)
	}
	if len(cfg.IngesterURLs) != 0 {
		t.Fatalf("IngesterURLs = %v", cfg.IngesterURLs)
	}
}

func TestTenantOverridesFileIsParsedAtLoad(t *testing.T) {
	clearTenantEnv(t)
	t.Setenv("OBS_TENANT_OVERRIDES_FILE", writeOverrides(t, "overrides:\n  team-b:\n    max_active_series: 2\n"))
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.TenantLimits().MaxActiveSeries("team-b"); got != 2 {
		t.Fatalf("team-b MaxActiveSeries = %d", got)
	}
	t.Setenv("OBS_TENANT_OVERRIDES_FILE", writeOverrides(t, "overrides: [oops\n"))
	if _, err := Load(); err == nil {
		t.Fatal("a malformed overrides file must fail Load")
	}
}
