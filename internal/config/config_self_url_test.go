package config

import (
	"strings"
	"testing"
)

func TestIngesterSelfURLIsNormalized(t *testing.T) {
	clearReplicationEnv(t)
	t.Setenv("OBS_TARGET", "ingester")
	t.Setenv("OBS_STORE_URL", "http://s:1")
	t.Setenv("OBS_INGESTER_SELF_URL", "HTTP://Ingester-2.:8080/")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.IngesterSelfURL != "http://ingester-2:8080" {
		t.Errorf("IngesterSelfURL = %q, want the ring's spelling http://ingester-2:8080", cfg.IngesterSelfURL)
	}
}

func TestIngesterSelfURLIsOptional(t *testing.T) {
	clearReplicationEnv(t)
	t.Setenv("OBS_TARGET", "ingester")
	t.Setenv("OBS_STORE_URL", "http://s:1")
	cfg, err := Load()
	if err != nil || cfg.IngesterSelfURL != "" {
		t.Fatalf("unset: %q, %v; want empty and no error", cfg.IngesterSelfURL, err)
	}
}

func TestIngesterSelfURLRejected(t *testing.T) {
	for _, tc := range []struct {
		env     map[string]string
		wantErr string
	}{
		{map[string]string{"OBS_TARGET": "ingester", "OBS_STORE_URL": "http://s:1", "OBS_INGESTER_SELF_URL": "ftp://a:1"},
			"OBS_INGESTER_SELF_URL \"ftp://a:1\" must be an http or https URL"},
		{map[string]string{"OBS_TARGET": "ingester", "OBS_STORE_URL": "http://s:1", "OBS_INGESTER_SELF_URL": "http://a:1/x"},
			"must be a base URL"},
		{map[string]string{"OBS_TARGET": "gateway", "OBS_INGESTER_URL": "http://a:1", "OBS_QUERIER_URL": "http://q:1",
			"OBS_INGESTER_SELF_URL": "http://a:1"}, "OBS_INGESTER_SELF_URL is set but target gateway does not use it"},
		{map[string]string{"OBS_INGESTER_SELF_URL": "http://a:1"}, "OBS_INGESTER_SELF_URL is set but target all-in-one does not use it"},
	} {
		clearReplicationEnv(t)
		t.Setenv("OBS_INGESTER_SELF_URL", "")
		for k, v := range tc.env {
			t.Setenv(k, v)
		}
		_, err := Load()
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("%v: err = %v, want %q", tc.env, err, tc.wantErr)
		}
	}
}
