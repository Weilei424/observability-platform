package config

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	HTTPAddr           string
	DataDir            string
	LogLevel           string
	WALSegmentMaxBytes int64
	WALSyncEveryN      int

	MaintenanceInterval  time.Duration
	FlushInterval        time.Duration
	FlushSealedChunks    int
	FlushWALBytes        int64
	CompactionBaseRange  time.Duration
	CompactionMultiplier int
	CompactionLevels     int
	Retention            time.Duration
}

func Load() (*Config, error) {
	v := viper.New()

	v.SetDefault("http_addr", ":8080")
	v.SetDefault("data_dir", "data")
	v.SetDefault("log_level", "info")
	v.SetDefault("wal_segment_max_bytes", int64(128<<20))
	v.SetDefault("wal_sync_every_n", 1)
	v.SetDefault("maintenance_interval", "30s")
	v.SetDefault("flush_interval", "2m")
	v.SetDefault("flush_sealed_chunks", 1000)
	v.SetDefault("flush_wal_bytes", int64(64<<20))
	v.SetDefault("compaction_base_range", "2h")
	v.SetDefault("compaction_multiplier", 4)
	v.SetDefault("compaction_levels", 3)
	v.SetDefault("retention", "0s")

	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("config: reading config file: %w", err)
		}
	}

	v.SetEnvPrefix("OBS")
	v.AutomaticEnv()

	// Resolve DataDir: prefer the explicit env var value (including empty string)
	// over viper's default, since viper does not distinguish "env set to empty"
	// from "env unset". Only data_dir needs this workaround because it is the
	// only field validated for non-emptiness — other fields fall back to their
	// defaults harmlessly when the env var is set to empty.
	dataDir := v.GetString("data_dir")
	if envVal, ok := os.LookupEnv("OBS_DATA_DIR"); ok {
		dataDir = envVal
	}

	maintenanceInterval, err := parseDuration(v.GetString("maintenance_interval"), "OBS_MAINTENANCE_INTERVAL")
	if err != nil {
		return nil, err
	}
	flushInterval, err := parseDuration(v.GetString("flush_interval"), "OBS_FLUSH_INTERVAL")
	if err != nil {
		return nil, err
	}
	baseRange, err := parseDuration(v.GetString("compaction_base_range"), "OBS_COMPACTION_BASE_RANGE")
	if err != nil {
		return nil, err
	}
	retention, err := parseDuration(v.GetString("retention"), "OBS_RETENTION")
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		HTTPAddr:             v.GetString("http_addr"),
		DataDir:              dataDir,
		LogLevel:             v.GetString("log_level"),
		WALSegmentMaxBytes:   v.GetInt64("wal_segment_max_bytes"),
		WALSyncEveryN:        v.GetInt("wal_sync_every_n"),
		MaintenanceInterval:  maintenanceInterval,
		FlushInterval:        flushInterval,
		FlushSealedChunks:    v.GetInt("flush_sealed_chunks"),
		FlushWALBytes:        v.GetInt64("flush_wal_bytes"),
		CompactionBaseRange:  baseRange,
		CompactionMultiplier: v.GetInt("compaction_multiplier"),
		CompactionLevels:     v.GetInt("compaction_levels"),
		Retention:            retention,
	}

	if cfg.DataDir == "" {
		return nil, fmt.Errorf("config: OBS_DATA_DIR must not be empty")
	}
	if cfg.MaintenanceInterval <= 0 {
		return nil, fmt.Errorf("config: maintenance_interval must be > 0")
	}
	if cfg.FlushInterval <= 0 {
		return nil, fmt.Errorf("config: flush_interval must be > 0")
	}
	if cfg.CompactionBaseRange <= 0 {
		return nil, fmt.Errorf("config: compaction_base_range must be > 0")
	}
	if cfg.CompactionMultiplier < 2 {
		return nil, fmt.Errorf("config: compaction_multiplier must be >= 2")
	}
	if cfg.CompactionLevels < 1 {
		return nil, fmt.Errorf("config: compaction_levels must be >= 1")
	}
	if cfg.FlushSealedChunks < 0 || cfg.FlushWALBytes < 0 || cfg.Retention < 0 {
		return nil, fmt.Errorf("config: thresholds and retention must be >= 0")
	}

	return cfg, nil
}

func parseDuration(s, name string) (time.Duration, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("config: invalid %s %q: %w", name, s, err)
	}
	return d, nil
}
