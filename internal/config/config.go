package config

import (
	"fmt"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Target is the component a process runs. One binary serves all of them.
type Target string

const (
	TargetAllInOne  Target = "all-in-one"
	TargetGateway   Target = "gateway"
	TargetIngester  Target = "ingester"
	TargetQuerier   Target = "querier"
	TargetStore     Target = "store"
	TargetCompactor Target = "compactor"
)

// Targets lists every valid target.
var Targets = []Target{TargetAllInOne, TargetGateway, TargetIngester, TargetQuerier, TargetStore, TargetCompactor}

// peerKey is one peer URL setting: its viper key and its environment variable.
type peerKey struct{ key, env string }

var (
	peerIngester = peerKey{"ingester_url", "OBS_INGESTER_URL"}
	peerStore    = peerKey{"store_url", "OBS_STORE_URL"}
	peerQuerier  = peerKey{"querier_url", "OBS_QUERIER_URL"}
)

// requiredPeers lists the peer URLs each target needs. Any other peer URL set
// for a target is an error: a setting that is silently ignored leaves an
// operator watching behavior they believe they changed.
var requiredPeers = map[Target][]peerKey{
	TargetAllInOne:  nil,
	TargetStore:     nil,
	TargetGateway:   {peerIngester, peerQuerier},
	TargetIngester:  {peerStore},
	TargetCompactor: {peerStore},
	TargetQuerier:   {peerIngester, peerStore},
}

type Config struct {
	HTTPAddr                string
	DataDir                 string
	LogLevel                string
	WALSegmentMaxBytes      int64
	WALSyncEveryN           int
	LogsFlushThresholdBytes int64

	Target      Target
	IngesterURL string
	StoreURL    string
	QuerierURL  string

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
	v.SetDefault("logs_flush_threshold_bytes", int64(8<<20))
	v.SetDefault("maintenance_interval", "30s")
	v.SetDefault("flush_interval", "2m")
	v.SetDefault("flush_sealed_chunks", 1000)
	v.SetDefault("flush_wal_bytes", int64(64<<20))
	v.SetDefault("compaction_base_range", "2h")
	v.SetDefault("compaction_multiplier", 4)
	v.SetDefault("compaction_levels", 3)
	v.SetDefault("retention", "0s")
	v.SetDefault("target", string(TargetAllInOne))
	v.SetDefault("ingester_url", "")
	v.SetDefault("store_url", "")
	v.SetDefault("querier_url", "")

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
		HTTPAddr:                v.GetString("http_addr"),
		DataDir:                 dataDir,
		LogLevel:                v.GetString("log_level"),
		WALSegmentMaxBytes:      v.GetInt64("wal_segment_max_bytes"),
		WALSyncEveryN:           v.GetInt("wal_sync_every_n"),
		LogsFlushThresholdBytes: v.GetInt64("logs_flush_threshold_bytes"),
		Target:                  Target(v.GetString("target")),
		IngesterURL:             v.GetString("ingester_url"),
		StoreURL:                v.GetString("store_url"),
		QuerierURL:              v.GetString("querier_url"),
		MaintenanceInterval:     maintenanceInterval,
		FlushInterval:           flushInterval,
		FlushSealedChunks:       v.GetInt("flush_sealed_chunks"),
		FlushWALBytes:           v.GetInt64("flush_wal_bytes"),
		CompactionBaseRange:     baseRange,
		CompactionMultiplier:    v.GetInt("compaction_multiplier"),
		CompactionLevels:        v.GetInt("compaction_levels"),
		Retention:               retention,
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
	if cfg.WALSegmentMaxBytes <= 0 {
		return nil, fmt.Errorf("config: wal_segment_max_bytes must be > 0")
	}
	if cfg.WALSyncEveryN < 1 {
		// < 1 would disable automatic fsync entirely, silently dropping the WAL's
		// durability guarantee.
		return nil, fmt.Errorf("config: wal_sync_every_n must be >= 1")
	}
	if cfg.LogsFlushThresholdBytes <= 0 {
		return nil, fmt.Errorf("config: logs_flush_threshold_bytes must be > 0")
	}

	if err := cfg.validateTopology(); err != nil {
		return nil, err
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

// validateTopology checks the target and that exactly the peer URLs it needs
// are set, each a base http(s) URL.
func (c *Config) validateTopology() error {
	required, ok := requiredPeers[c.Target]
	if !ok {
		names := make([]string, len(Targets))
		for i, t := range Targets {
			names[i] = string(t)
		}
		return fmt.Errorf("config: unknown OBS_TARGET %q (want one of %s)", c.Target, strings.Join(names, ", "))
	}
	values := map[peerKey]string{peerIngester: c.IngesterURL, peerStore: c.StoreURL, peerQuerier: c.QuerierURL}
	for _, p := range []peerKey{peerIngester, peerStore, peerQuerier} {
		needed := slices.Contains(required, p)
		v := values[p]
		switch {
		case needed && v == "":
			return fmt.Errorf("config: target %s requires %s", c.Target, p.env)
		case !needed && v != "":
			return fmt.Errorf("config: %s is set but target %s does not use it; unset it", p.env, c.Target)
		case v != "":
			if err := validatePeerURL(p.env, v); err != nil {
				return err
			}
		}
	}
	return nil
}

// validatePeerURL requires an absolute http or https URL with a host and
// nothing after it but an optional "/": clients append their own paths.
func validatePeerURL(env, raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("config: %s %q is not a URL: %w", env, raw, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("config: %s %q must be an http or https URL", env, raw)
	}
	if u.Host == "" {
		return fmt.Errorf("config: %s %q has no host", env, raw)
	}
	if (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("config: %s %q must be a base URL with no path, query, or fragment", env, raw)
	}
	return nil
}
