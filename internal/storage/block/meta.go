package block

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Meta holds block metadata persisted to meta.json inside each block directory.
type Meta struct {
	BlockID    string    `json:"block_id"`
	MinTime    int64     `json:"min_time"`
	MaxTime    int64     `json:"max_time"`
	NumSeries  int       `json:"num_series"`
	NumSamples int       `json:"num_samples"`
	CreatedAt  time.Time `json:"created_at"`
}

func writeMeta(dir string, m Meta) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("block: marshal meta: %w", err)
	}
	return os.WriteFile(filepath.Join(dir, "meta.json"), data, 0o644)
}

func readMeta(dir string) (Meta, error) {
	data, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		return Meta{}, fmt.Errorf("block: read meta.json: %w", err)
	}
	var m Meta
	if err := json.Unmarshal(data, &m); err != nil {
		return Meta{}, fmt.Errorf("block: unmarshal meta.json: %w", err)
	}
	return m, nil
}
