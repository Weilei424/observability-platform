package config

import (
	"strings"
	"testing"
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
