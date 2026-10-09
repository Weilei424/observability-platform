package integration_test

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/ring"
)

// setRF restarts p (the gateway or querier) with replication factor rf and
// the given ingester list.
func setRF(p *process, rf int, urls []string) {
	p.t.Helper()
	p.stop()
	p.cfg.ReplicationFactor = rf
	p.cfg.IngesterURL, p.cfg.IngesterURLs = strings.Join(urls, ","), slices.Clone(urls)
	p.start()
	httpClient.CloseIdleConnections()
}

// Raising RF from 1 to 3 is safe for reads only once every write acknowledged
// at RF=1 is off its single ingester. The maintenance flush does not do that:
// it takes sealed chunks only, and the logs head flushes at a size threshold,
// so a quiet series' open chunk or a small log line stays on its one ingester
// indefinitely. A querier at RF=3 that then skips that ingester answers an
// incomplete 200. The barrier is an acknowledged drain of every ingester, each
// restarted after its 200 (a drained ingester refuses writes until then).
func TestRaisingRFNeedsADrainOfEveryIngester(t *testing.T) {
	c := startClusterRF(t, 3, 1)
	r, err := ring.New(c.ingesterURLs)
	if err != nil {
		t.Fatal(err)
	}
	// A metric and a stream with the same RF=1 owner.
	const metric = "mig_metric"
	owner := r.Owner(seriesKey(t, metric))
	svc := ""
	for i := 0; svc == ""; i++ {
		candidate := fmt.Sprintf("mig-%d", i)
		l, _ := logs.NewStreamLabels(map[string]string{"service": candidate})
		if r.Owner(l.Hash()) == owner {
			svc = candidate
		}
	}
	base := time.Now().UnixMilli()
	c.ingest(t, metric, base, 7)         // one sample: an open chunk, never sealed
	c.pushLog(t, svc, base*1e6, "quiet") // well under the logs flush threshold

	// The querier reads every ingester but the owner, which it finds down.
	dead := "http://" + freeAddr(t)
	withOwnerDown := slices.Clone(c.ingesterURLs)
	withOwnerDown[slices.Index(withOwnerDown, owner)] = dead
	readBoth := func() (string, int, int) {
		v, code, _ := c.instant(t, metric, base)
		_, entries, _ := c.lokiQueryRange(t, fmt.Sprintf(`{service=%q}`, svc), base*1e6-1, base*1e6+1)
		return v, code, len(entries)
	}

	// The gateway goes first, as staged. Waiting is not a barrier: the
	// maintenance loop runs every 50ms here and still leaves both writes on
	// their owner alone.
	setRF(c.gateway, 3, c.ingesterURLs)
	time.Sleep(300 * time.Millisecond)
	setRF(c.querier, 3, withOwnerDown)
	if v, code, lines := readBoth(); code != http.StatusOK || v != "" || lines != 0 {
		t.Fatalf("without the drain barrier: read = %q (%d), %d lines; want the hazard this test guards: an incomplete 200", v, code, lines)
	}

	// The barrier: drain each ingester, wait for its 200, restart it.
	setRF(c.querier, 1, c.ingesterURLs)
	for i, p := range c.ingesters {
		if code, body := postDrain(t, c.ingesterURLs[i]); code != http.StatusOK {
			t.Fatalf("drain %s = %d: %s", c.ingesterURLs[i], code, body)
		}
		p.stop()
		p.start()
	}
	httpClient.CloseIdleConnections()
	setRF(c.querier, 3, withOwnerDown)
	if v, code, lines := readBoth(); code != http.StatusOK || v != "7" || lines != 1 {
		t.Fatalf("after the drain barrier: read = %q (%d), %d lines; want 7 and the one line with the owner down", v, code, lines)
	}
}
