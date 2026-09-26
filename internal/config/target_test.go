package config

import (
	"strings"
	"testing"
)

// clearTopologyEnv isolates a test from the developer's shell. Viper treats an
// empty variable as unset, so an empty value restores the default.
func clearTopologyEnv(t *testing.T) {
	t.Helper()
	t.Setenv("OBS_DATA_DIR", "data")
	for _, k := range []string{"OBS_TARGET", "OBS_INGESTER_URL", "OBS_STORE_URL", "OBS_QUERIER_URL"} {
		t.Setenv(k, "")
	}
}

func TestLoad_TargetDefaultsToAllInOne(t *testing.T) {
	clearTopologyEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Target != TargetAllInOne {
		t.Errorf("Target = %q, want %q", cfg.Target, TargetAllInOne)
	}
}

func TestLoad_TargetPeers(t *testing.T) {
	for _, tc := range []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{"store needs no peers", map[string]string{"OBS_TARGET": "store"}, ""},
		{"gateway", map[string]string{"OBS_TARGET": "gateway", "OBS_INGESTER_URL": "http://ingester:8080", "OBS_QUERIER_URL": "http://querier:8080"}, ""},
		{"ingester", map[string]string{"OBS_TARGET": "ingester", "OBS_STORE_URL": "http://store:8080"}, ""},
		{"querier", map[string]string{"OBS_TARGET": "querier", "OBS_INGESTER_URL": "http://ingester:8080", "OBS_STORE_URL": "http://store:8080"}, ""},
		{"compactor", map[string]string{"OBS_TARGET": "compactor", "OBS_STORE_URL": "http://store:8080"}, ""},
		{"https with a trailing slash", map[string]string{"OBS_TARGET": "compactor", "OBS_STORE_URL": "https://store:8443/"}, ""},
		{"unknown target", map[string]string{"OBS_TARGET": "distributor"}, `unknown OBS_TARGET "distributor"`},
		{"missing peer", map[string]string{"OBS_TARGET": "querier", "OBS_INGESTER_URL": "http://ingester:8080"}, "requires OBS_STORE_URL"},
		{"unused peer", map[string]string{"OBS_TARGET": "ingester", "OBS_STORE_URL": "http://store:8080", "OBS_QUERIER_URL": "http://querier:8080"}, "OBS_QUERIER_URL is set but target ingester does not use it"},
		{"all-in-one takes no peers", map[string]string{"OBS_STORE_URL": "http://store:8080"}, "OBS_STORE_URL is set but target all-in-one does not use it"},
		{"no scheme", map[string]string{"OBS_TARGET": "compactor", "OBS_STORE_URL": "store:8080"}, "must be an http or https URL"},
		{"no host", map[string]string{"OBS_TARGET": "compactor", "OBS_STORE_URL": "http://"}, "has no host"},
		{"a path", map[string]string{"OBS_TARGET": "compactor", "OBS_STORE_URL": "http://store:8080/api"}, "must be a base URL"},
		{"a query", map[string]string{"OBS_TARGET": "compactor", "OBS_STORE_URL": "http://store:8080?x=1"}, "must be a base URL"},
		{"bare fragment", map[string]string{"OBS_TARGET": "compactor", "OBS_STORE_URL": "http://store:8080#"}, "must be a base URL"},
		{"bare query", map[string]string{"OBS_TARGET": "compactor", "OBS_STORE_URL": "http://store:8080?"}, "must be a base URL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearTopologyEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			cfg, err := Load()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Load: %v", err)
				}
				if string(cfg.Target) != tc.env["OBS_TARGET"] && tc.env["OBS_TARGET"] != "" {
					t.Errorf("Target = %q, want %q", cfg.Target, tc.env["OBS_TARGET"])
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Load error = %v, want one containing %q", err, tc.wantErr)
			}
			// Every topology error must name the target it is about — an
			// operator running several components needs to see which one a
			// startup error refers to, not just which env var.
			if target := tc.env["OBS_TARGET"]; target != "" && !strings.Contains(err.Error(), target) {
				t.Errorf("error = %v, want it to name the target %q", err, target)
			}
		})
	}
}

// TestLoad_PeerURLRejectsUserinfo pins the fix for a peer URL that carries
// embedded credentials (http://user:pass@host): every other check in
// validatePeerURL passes it — valid scheme, a host, and an empty path — so
// without an explicit rejection it would load silently. The credentials must
// also never appear in the resulting error: printing them into a message a
// caller then logs would defeat the point of rejecting them.
func TestLoad_PeerURLRejectsUserinfo(t *testing.T) {
	clearTopologyEnv(t)
	t.Setenv("OBS_TARGET", "compactor")
	t.Setenv("OBS_STORE_URL", "http://user:hunter2@store:8080")

	_, err := Load()
	if err == nil {
		t.Fatal("Load: want an error for a peer URL with userinfo, got nil")
	}
	if !strings.Contains(err.Error(), "OBS_STORE_URL") {
		t.Errorf("error = %v, want it to name OBS_STORE_URL", err)
	}
	if !strings.Contains(err.Error(), "compactor") {
		t.Errorf("error = %v, want it to name the target compactor", err)
	}
	if !strings.Contains(err.Error(), "credentials") {
		t.Errorf("error = %v, want it to say the URL must not contain credentials", err)
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Errorf("error = %v, must not echo the password", err)
	}
}

// TestLoad_PeerURLTrailingSlashTrimmed pins the normalization every peer URL
// gets: a trailing "/" is trimmed at load time, so later callers can join
// "/internal/v1/..." onto the stored value without doubling the slash.
func TestLoad_PeerURLTrailingSlashTrimmed(t *testing.T) {
	clearTopologyEnv(t)
	t.Setenv("OBS_TARGET", "compactor")
	t.Setenv("OBS_STORE_URL", "https://store:8443/")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.StoreURL != "https://store:8443" {
		t.Errorf("StoreURL = %q, want %q", cfg.StoreURL, "https://store:8443")
	}
}
