package config

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func clearStoreEnv(t *testing.T) {
	t.Helper()
	clearReplicationEnv(t)
	for _, k := range []string{"OBS_STORE_REPLICATION_FACTOR", "OBS_STORE_SELF_URL", "OBS_STORE_RETRY_MAX_BYTES", "OBS_STORE_RETRY_MAX_AGE"} {
		t.Setenv(k, "")
	}
}

func TestStoreListIsNormalized(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("OBS_TARGET", "querier")
	t.Setenv("OBS_INGESTER_URL", "http://i:1")
	t.Setenv("OBS_STORE_URL", "http://S1:8080/, http://s2:8080")
	t.Setenv("OBS_STORE_REPLICATION_FACTOR", "2")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cfg.StoreURLs, []string{"http://s1:8080", "http://s2:8080"}) || cfg.StoreReplicationFactor != 2 {
		t.Fatalf("StoreURLs = %v, R_s = %d", cfg.StoreURLs, cfg.StoreReplicationFactor)
	}
	if cfg.StoreURL != "http://s1:8080,http://s2:8080" {
		t.Errorf("StoreURL = %q", cfg.StoreURL)
	}
}

func TestStoreDefaults(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("OBS_TARGET", "ingester")
	t.Setenv("OBS_STORE_URL", "http://s:1")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.StoreReplicationFactor != 1 || cfg.StoreRetryMaxBytes != 64<<20 || cfg.StoreRetryMaxAge != 15*time.Minute || len(cfg.StoreURLs) != 1 {
		t.Fatalf("defaults = %+v", cfg)
	}
}

func TestStoreSettingsRejected(t *testing.T) {
	for _, tc := range []struct {
		env     map[string]string
		wantErr string
	}{
		{map[string]string{"OBS_TARGET": "ingester", "OBS_STORE_URL": "http://s1:1,,http://s2:1"}, "OBS_STORE_URL"},
		{map[string]string{"OBS_TARGET": "ingester", "OBS_STORE_URL": "http://s1:1,http://S1:1"}, "OBS_STORE_URL"},
		{map[string]string{"OBS_TARGET": "ingester", "OBS_STORE_URL": "http://s1:1", "OBS_STORE_REPLICATION_FACTOR": "2"},
			"OBS_STORE_REPLICATION_FACTOR 2 is larger than the 1 stores in OBS_STORE_URL"},
		{map[string]string{"OBS_TARGET": "ingester", "OBS_STORE_URL": "http://s1:1", "OBS_STORE_REPLICATION_FACTOR": "0"},
			"OBS_STORE_REPLICATION_FACTOR must be >= 1"},
		{map[string]string{"OBS_TARGET": "compactor", "OBS_STORE_URL": "http://s1:1,http://s2:1", "OBS_STORE_REPLICATION_FACTOR": "2"},
			"OBS_STORE_REPLICATION_FACTOR is set but target compactor does not use it"},
		{map[string]string{"OBS_TARGET": "ingester", "OBS_STORE_URL": "http://s:1", "OBS_STORE_SELF_URL": "http://s:1"},
			"OBS_STORE_SELF_URL is set but target ingester does not use it"},
		{map[string]string{"OBS_TARGET": "store", "OBS_STORE_SELF_URL": "ftp://s:1"}, "must be an http or https URL"},
		{map[string]string{"OBS_TARGET": "ingester", "OBS_STORE_URL": "http://s:1", "OBS_STORE_RETRY_MAX_BYTES": "-1"},
			"OBS_STORE_RETRY_MAX_BYTES must be >= 0"},
		{map[string]string{"OBS_TARGET": "querier", "OBS_INGESTER_URL": "http://i:1", "OBS_STORE_URL": "http://s:1", "OBS_STORE_RETRY_MAX_BYTES": "10"},
			"OBS_STORE_RETRY_MAX_BYTES is set but target querier does not use it"},
		{map[string]string{"OBS_TARGET": "compactor", "OBS_STORE_URL": "http://s:1", "OBS_STORE_RETRY_MAX_AGE": "1m"},
			"OBS_STORE_RETRY_MAX_AGE is set but target compactor does not use it"},
		{map[string]string{"OBS_TARGET": "ingester", "OBS_STORE_URL": "http://s:1", "OBS_STORE_RETRY_MAX_AGE": "0s"},
			"OBS_STORE_RETRY_MAX_AGE must be > 0"},
	} {
		clearStoreEnv(t)
		for k, v := range tc.env {
			t.Setenv(k, v)
		}
		_, err := Load()
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("%v: err = %v, want %q", tc.env, err, tc.wantErr)
		}
	}
}

func TestStoreSelfURLOnStore(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("OBS_TARGET", "store")
	t.Setenv("OBS_STORE_SELF_URL", "HTTP://Store-2.:8080/")
	cfg, err := Load()
	if err != nil || cfg.StoreSelfURL != "http://store-2:8080" {
		t.Fatalf("StoreSelfURL = %q, %v", cfg.StoreSelfURL, err)
	}
}

func TestStoreRetrySettingsOnIngester(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("OBS_TARGET", "ingester")
	t.Setenv("OBS_STORE_URL", "http://s:1")
	t.Setenv("OBS_STORE_RETRY_MAX_BYTES", "1024")
	t.Setenv("OBS_STORE_RETRY_MAX_AGE", "1m")
	cfg, err := Load()
	if err != nil || cfg.StoreRetryMaxBytes != 1024 || cfg.StoreRetryMaxAge != time.Minute {
		t.Fatalf("cfg = %+v, err = %v", cfg, err)
	}
}
