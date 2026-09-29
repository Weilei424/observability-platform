package integration_test

// Split rows name their component: `ingester:data/...` or `store:data/...`,
// so the all-in-one test's data/ rows and these never match each other.

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/config"
)

var splitLayoutRe = regexp.MustCompile("(?m)^\\|\\s*`(ingester|store):(data/[^`]+)`\\s*\\|")

func TestSplitStorageLayoutDocMatchesDisk(t *testing.T) {
	doc, err := os.ReadFile(storageLayoutDoc)
	if err != nil {
		t.Fatal(err)
	}
	documented := map[string]map[string]*regexp.Regexp{"ingester": {}, "store": {}}
	for _, m := range splitLayoutRe.FindAllStringSubmatch(string(doc), -1) {
		documented[m[1]][m[2]] = layoutPattern(t, m[2])
	}
	if len(documented["ingester"]) == 0 || len(documented["store"]) == 0 {
		t.Fatalf("%s documents no ingester: or store: rows", storageLayoutDoc)
	}

	ingDir, storeDir := produceSplitTrees(t)
	for component, dir := range map[string]string{"ingester": ingDir, "store": storeDir} {
		produced := walkTree(t, dir)
		matched := map[string]bool{}
		for entry := range produced {
			ok := false
			for d, re := range documented[component] {
				if re.MatchString(entry) {
					ok, matched[d] = true, true
				}
			}
			if !ok {
				t.Errorf("the %s wrote %q, which %s does not document as %s:%s", component, entry, storageLayoutDoc, component, entry)
			}
		}
		for d := range documented[component] {
			if !matched[d] {
				t.Errorf("%s documents %s:%s, which the %s never produced", storageLayoutDoc, component, d, component)
			}
		}
	}
}

func walkTree(t *testing.T, dataDir string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	err := filepath.WalkDir(dataDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dataDir, path)
		if rel == "." {
			return nil
		}
		entry := "data/" + filepath.ToSlash(rel)
		if d.IsDir() {
			entry += "/"
		}
		out[entry] = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// produceSplitTrees runs a real store and ingester, writes both signals,
// flushes through the internal API by stopping the ingester gracefully, and
// returns their data directories.
func produceSplitTrees(t *testing.T) (ingesterDir, storeDir string) {
	t.Helper()
	storeAddr, ingAddr := freeAddr(t), freeAddr(t)
	base := func(target config.Target, addr string) *config.Config {
		return &config.Config{
			Target: target, HTTPAddr: addr, DataDir: t.TempDir(), LogLevel: "info",
			WALSegmentMaxBytes: 1 << 20, WALSyncEveryN: 1, LogsFlushThresholdBytes: 1 << 20,
			MaintenanceInterval: time.Hour, FlushInterval: time.Hour,
			CompactionBaseRange: 2 * time.Hour, CompactionMultiplier: 4, CompactionLevels: 3,
		}
	}
	store := &process{t: t, cfg: base(config.TargetStore, storeAddr)}
	ingCfg := base(config.TargetIngester, ingAddr)
	ingCfg.StoreURL = "http://" + storeAddr
	ingester := &process{t: t, cfg: ingCfg}
	store.start()
	ingester.start()

	var metrics []string
	for i := range 120 { // one sealed chunk
		metrics = append(metrics, fmt.Sprintf(`{"name":"layout_split","labels":{},"timestamp_ms":%d,"value":%d}`, 1_000_000+i*1000, i))
	}
	resp, err := http.Post("http://"+ingAddr+"/api/v1/ingest/metrics", "application/json",
		strings.NewReader(`{"metrics":[`+strings.Join(metrics, ",")+`]}`))
	if err != nil || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("ingest: %v %v", err, resp)
	}
	resp.Body.Close()
	resp, err = http.Post("http://"+ingAddr+"/loki/api/v1/push", "application/json",
		strings.NewReader(`{"streams":[{"stream":{"service":"layout-split"},"values":[["1000000000","line"]]}]}`))
	if err != nil || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("push: %v %v", err, resp)
	}
	resp.Body.Close()

	ingester.stop() // the final flush sends the sealed chunk and the log head to the store
	store.stop()
	return ingCfg.DataDir, store.cfg.DataDir
}
