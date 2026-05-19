package config

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
)

type Config struct {
	HTTPAddr           string
	DataDir            string
	LogLevel           string
	WALSegmentMaxBytes int64
	WALSyncEveryN      int
}

func Load() (*Config, error) {
	v := viper.New()

	v.SetDefault("http_addr", ":8080")
	v.SetDefault("data_dir", "data")
	v.SetDefault("log_level", "info")
	v.SetDefault("wal_segment_max_bytes", int64(128<<20))
	v.SetDefault("wal_sync_every_n", 1)

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

	cfg := &Config{
		HTTPAddr:           v.GetString("http_addr"),
		DataDir:            dataDir,
		LogLevel:           v.GetString("log_level"),
		WALSegmentMaxBytes: v.GetInt64("wal_segment_max_bytes"),
		WALSyncEveryN:      v.GetInt("wal_sync_every_n"),
	}

	if cfg.DataDir == "" {
		return nil, fmt.Errorf("config: OBS_DATA_DIR must not be empty")
	}

	return cfg, nil
}
