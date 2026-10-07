package config

import (
	"strings"
	"testing"
	"time"
)

func clearReplicationEnv(t *testing.T) {
	t.Helper()
	clearTopologyEnv(t)
	t.Setenv("OBS_REPLICATION_FACTOR", "")
	t.Setenv("OBS_INGESTER_TIMEOUT", "")
}

func TestReplicationDefaults(t *testing.T) {
	clearReplicationEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ReplicationFactor != 1 || cfg.IngesterTimeout != DefaultIngesterTimeout {
		t.Fatalf("defaults = RF %d, timeout %v; want 1, %v", cfg.ReplicationFactor, cfg.IngesterTimeout, DefaultIngesterTimeout)
	}
}

func TestReplicationBounds(t *testing.T) {
	for _, tc := range []struct {
		env     map[string]string
		wantErr string
	}{
		{map[string]string{"OBS_REPLICATION_FACTOR": "0"}, "OBS_REPLICATION_FACTOR must be >= 1"},
		{map[string]string{"OBS_INGESTER_TIMEOUT": "50ms"}, "OBS_INGESTER_TIMEOUT must be >= 100ms"},
		{map[string]string{"OBS_TARGET": "gateway", "OBS_INGESTER_URL": "http://a:1,http://b:1", "OBS_QUERIER_URL": "http://q:1",
			"OBS_REPLICATION_FACTOR": "3"}, "OBS_REPLICATION_FACTOR 3 is larger than the 2 ingesters in OBS_INGESTER_URL"},
		{map[string]string{"OBS_TARGET": "querier", "OBS_INGESTER_URL": "http://a:1", "OBS_STORE_URL": "http://s:1",
			"OBS_REPLICATION_FACTOR": "2"}, "OBS_REPLICATION_FACTOR 2 is larger than the 1 ingesters in OBS_INGESTER_URL"},
	} {
		clearReplicationEnv(t)
		for k, v := range tc.env {
			t.Setenv(k, v)
		}
		_, err := Load()
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("%v: err = %v, want %q", tc.env, err, tc.wantErr)
		}
	}
}

func TestReplicationAcceptedOnGateway(t *testing.T) {
	clearReplicationEnv(t)
	t.Setenv("OBS_TARGET", "gateway")
	t.Setenv("OBS_INGESTER_URL", "http://a:1,http://b:1,http://c:1")
	t.Setenv("OBS_QUERIER_URL", "http://q:1")
	t.Setenv("OBS_REPLICATION_FACTOR", "3")
	t.Setenv("OBS_INGESTER_TIMEOUT", "2s")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ReplicationFactor != 3 || cfg.IngesterTimeout != 2*time.Second {
		t.Fatalf("RF %d timeout %v", cfg.ReplicationFactor, cfg.IngesterTimeout)
	}
}

func TestQuorum(t *testing.T) {
	for rf, want := range map[int]int{1: 1, 2: 2, 3: 2, 4: 3, 5: 3} {
		if got := Quorum(rf); got != want {
			t.Errorf("Quorum(%d) = %d, want %d", rf, got, want)
		}
	}
}
