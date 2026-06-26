package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoad_EnvVarOverride(t *testing.T) {
	t.Setenv("OBS_HTTP_ADDR", ":9090")
	t.Setenv("OBS_DATA_DIR", "/tmp/obs-test-data")
	t.Setenv("OBS_LOG_LEVEL", "debug")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.HTTPAddr != ":9090" {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, ":9090")
	}
	if cfg.DataDir != "/tmp/obs-test-data" {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, "/tmp/obs-test-data")
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, "debug")
	}
}

func TestLoad_EmptyDataDirErrors(t *testing.T) {
	t.Setenv("OBS_DATA_DIR", "")

	_, err := Load()
	if err == nil {
		t.Error("Load() with empty DataDir should return error, got nil")
	}
	if !strings.Contains(err.Error(), "OBS_DATA_DIR") {
		t.Errorf("error message = %q, want it to mention OBS_DATA_DIR", err.Error())
	}
}

func TestLoad_Defaults(t *testing.T) {
	t.Setenv("OBS_DATA_DIR", "data") // must be non-empty for Load() to succeed

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, ":8080")
	}
	if cfg.DataDir != "data" {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, "data")
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, "info")
	}
}

func TestLoad_WALDefaults(t *testing.T) {
	t.Setenv("OBS_DATA_DIR", "data")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.WALSegmentMaxBytes != 128<<20 {
		t.Errorf("WALSegmentMaxBytes = %d, want %d", cfg.WALSegmentMaxBytes, 128<<20)
	}
	if cfg.WALSyncEveryN != 1 {
		t.Errorf("WALSyncEveryN = %d, want 1", cfg.WALSyncEveryN)
	}
}

func TestLoad_WALEnvOverride(t *testing.T) {
	t.Setenv("OBS_DATA_DIR", "data")
	t.Setenv("OBS_WAL_SEGMENT_MAX_BYTES", "67108864") // 64 MiB
	t.Setenv("OBS_WAL_SYNC_EVERY_N", "10")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.WALSegmentMaxBytes != 64<<20 {
		t.Errorf("WALSegmentMaxBytes = %d, want %d", cfg.WALSegmentMaxBytes, 64<<20)
	}
	if cfg.WALSyncEveryN != 10 {
		t.Errorf("WALSyncEveryN = %d, want 10", cfg.WALSyncEveryN)
	}
}

func TestLoad_MaintenanceDefaults(t *testing.T) {
	t.Setenv("OBS_DATA_DIR", "data")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MaintenanceInterval != 30*time.Second {
		t.Errorf("MaintenanceInterval = %v, want 30s", cfg.MaintenanceInterval)
	}
	if cfg.FlushInterval != 2*time.Minute {
		t.Errorf("FlushInterval = %v, want 2m", cfg.FlushInterval)
	}
	if cfg.FlushSealedChunks != 1000 {
		t.Errorf("FlushSealedChunks = %d, want 1000", cfg.FlushSealedChunks)
	}
	if cfg.FlushWALBytes != 64<<20 {
		t.Errorf("FlushWALBytes = %d, want %d", cfg.FlushWALBytes, 64<<20)
	}
	if cfg.CompactionBaseRange != 2*time.Hour {
		t.Errorf("CompactionBaseRange = %v, want 2h", cfg.CompactionBaseRange)
	}
	if cfg.CompactionMultiplier != 4 || cfg.CompactionLevels != 3 {
		t.Errorf("multiplier/levels = %d/%d, want 4/3", cfg.CompactionMultiplier, cfg.CompactionLevels)
	}
	if cfg.Retention != 0 {
		t.Errorf("Retention = %v, want 0", cfg.Retention)
	}
}

func TestLoad_RetentionEnvOverride(t *testing.T) {
	t.Setenv("OBS_DATA_DIR", "data")
	t.Setenv("OBS_RETENTION", "15m")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Retention != 15*time.Minute {
		t.Errorf("Retention = %v, want 15m", cfg.Retention)
	}
}

func TestLoad_InvalidMultiplierErrors(t *testing.T) {
	t.Setenv("OBS_DATA_DIR", "data")
	t.Setenv("OBS_COMPACTION_MULTIPLIER", "1")
	if _, err := Load(); err == nil {
		t.Fatal("multiplier=1 should error")
	}
}

func TestLoad_InvalidDurationErrors(t *testing.T) {
	t.Setenv("OBS_DATA_DIR", "data")
	t.Setenv("OBS_FLUSH_INTERVAL", "notaduration")
	if _, err := Load(); err == nil {
		t.Fatal("invalid flush_interval should error")
	}
}
