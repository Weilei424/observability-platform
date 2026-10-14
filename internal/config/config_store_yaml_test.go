package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// loadWithYAML runs Load in a directory whose config.yaml holds body.
func loadWithYAML(t *testing.T, body string) (*Config, error) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	return Load()
}

// A store setting given in config.yaml is as explicit as one given in the
// environment: it is bounded and refused on a target that does not use it.
func TestStoreSettingsFromYAMLAreValidated(t *testing.T) {
	for _, tc := range []struct {
		name, target, yaml, wantErr string
	}{
		{"zero store RF", "ingester", "store_url: http://s1:1\nstore_replication_factor: 0\n", "OBS_STORE_REPLICATION_FACTOR must be >= 1"},
		{"negative store RF", "ingester", "store_url: http://s1:1\nstore_replication_factor: -1\n", "OBS_STORE_REPLICATION_FACTOR must be >= 1"},
		{"retry bytes on a querier", "querier", "ingester_url: http://i:1\nstore_url: http://s1:1\nstore_retry_max_bytes: 1024\n", "OBS_STORE_RETRY_MAX_BYTES is set but target querier does not use it"},
		{"retry age on a store", "store", "store_retry_max_age: 1m\n", "OBS_STORE_RETRY_MAX_AGE is set but target store does not use it"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearStoreEnv(t)
			t.Setenv("OBS_TARGET", tc.target)
			_, err := loadWithYAML(t, tc.yaml)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

// The same settings in config.yaml load on the ingester that uses them.
func TestStoreSettingsFromYAMLLoadOnTheIngester(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("OBS_TARGET", "ingester")
	cfg, err := loadWithYAML(t, "store_url: http://s1:1,http://s2:1\nstore_replication_factor: 2\nstore_retry_max_bytes: 1024\nstore_retry_max_age: 1m\n")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.StoreReplicationFactor != 2 || cfg.StoreRetryMaxBytes != 1024 || cfg.StoreRetryMaxAge.String() != "1m0s" {
		t.Fatalf("cfg = %+v", cfg)
	}
}

// A malformed retry bound is refused, from the environment or config.yaml,
// rather than read as 0, which would turn the straggler queue off.
func TestMalformedStoreRetryBoundsAreRefused(t *testing.T) {
	for _, tc := range []struct {
		name, env, yaml, wantErr string
	}{
		{"bytes in env", "bogus", "", "OBS_STORE_RETRY_MAX_BYTES must be a whole number"},
		{"bytes in YAML", "", "store_retry_max_bytes: bogus\n", "OBS_STORE_RETRY_MAX_BYTES must be a whole number"},
		{"fractional bytes in YAML", "", "store_retry_max_bytes: 1.5\n", "OBS_STORE_RETRY_MAX_BYTES must be a whole number"},
		{"age in YAML", "", "store_retry_max_age: soon\n", "OBS_STORE_RETRY_MAX_AGE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearStoreEnv(t)
			t.Setenv("OBS_TARGET", "ingester")
			t.Setenv("OBS_STORE_URL", "http://s1:1")
			if tc.env != "" {
				t.Setenv("OBS_STORE_RETRY_MAX_BYTES", tc.env)
			}
			_, err := loadWithYAML(t, tc.yaml)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

// An explicit 0 still loads: it is the documented way to turn the queue off.
func TestZeroStoreRetryMaxBytesLoads(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("OBS_TARGET", "ingester")
	t.Setenv("OBS_STORE_URL", "http://s1:1")
	t.Setenv("OBS_STORE_RETRY_MAX_BYTES", "0")
	cfg, err := loadWithYAML(t, "")
	if err != nil || cfg.StoreRetryMaxBytes != 0 {
		t.Fatalf("cfg = %+v, err = %v", cfg, err)
	}
}
