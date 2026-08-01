// Command sample-app emits Loki-style log streams to the observability-platform
// backend so the Grafana logs demo has live data. It is the logs counterpart to
// examples/load-generator (metrics) and deliberately emits no metrics of its own:
// two processes writing independent counters to one series would make rate()
// meaningless. Application metrics move here in Phase 5.1.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

// envLabel is carried by every stream. It is constant on purpose: it gives
// /loki/api/v1/labels a third label name so Grafana's label browser has
// something to show beyond the two labels the demo queries use.
const envLabel = "local"

func defaultAddr() string {
	if v := os.Getenv("OBS_BACKEND_ADDR"); v != "" {
		return v
	}
	return "http://localhost:8080"
}

// entry is one generated log line plus the stream it belongs to.
type entry struct {
	service string
	level   string
	tsNs    int64
	line    string
}

var (
	methods    = []string{"GET", "POST"}
	paths      = []string{"/api/v1/query", "/api/v1/query_range", "/api/v1/ingest/metrics", "/loki/api/v1/push"}
	jobs       = []string{"compaction", "retention", "flush"}
	serverErrs = []int{500, 503}
)

func requestID(r *rand.Rand) string {
	return fmt.Sprintf("%06x", r.Intn(1<<24))
}

// apiEntry generates one api-service line: 80% info, 12% warn, 8% error.
// Lines are plain text, never JSON or logfmt — `| json` and `| logfmt` return
// explicit errors in this backend, so structured lines would invite an
// unsupported pipeline on a viewer's first click.
func apiEntry(r *rand.Rand, tsNs int64) entry {
	method := methods[r.Intn(len(methods))]
	path := paths[r.Intn(len(paths))]
	id := requestID(r)
	switch roll := r.Float64(); {
	case roll < 0.80:
		return entry{"api", "info", tsNs,
			fmt.Sprintf("%s %s 200 in %dms request_id=%s", method, path, 1+r.Intn(200), id)}
	case roll < 0.92:
		return entry{"api", "warn", tsNs,
			fmt.Sprintf("slow request: %s %s 200 in %dms request_id=%s exceeded 500ms budget", method, path, 500+r.Intn(1000), id)}
	default:
		return entry{"api", "error", tsNs,
			fmt.Sprintf("%s %s %d in %dms request_id=%s upstream timeout after 30s", method, path, serverErrs[r.Intn(len(serverErrs))], 1+r.Intn(60), id)}
	}
}

// workerEntry generates one worker-service line: 90% info, 10% error.
func workerEntry(r *rand.Rand, tsNs int64) entry {
	job := jobs[r.Intn(len(jobs))]
	id := fmt.Sprintf("block-%04d", r.Intn(10000))
	if r.Float64() < 0.90 {
		return entry{"worker", "info", tsNs,
			fmt.Sprintf("job %s=%s completed in %dms", job, id, 100+r.Intn(3000))}
	}
	return entry{"worker", "error", tsNs,
		fmt.Sprintf("job %s=%s failed: deadline exceeded", job, id)}
}

// buildBatch generates one tick: three api lines and one worker line.
func buildBatch(r *rand.Rand, tsNs int64) []entry {
	out := make([]entry, 0, 4)
	for i := 0; i < 3; i++ {
		out = append(out, apiEntry(r, tsNs))
	}
	return append(out, workerEntry(r, tsNs))
}

// encodePush groups entries by stream and marshals the Loki push payload:
// {"streams":[{"stream":{...},"values":[["<unix_nano>","<line>"]]}]}
func encodePush(entries []entry) ([]byte, error) {
	type stream struct {
		Stream map[string]string `json:"stream"`
		Values [][2]string       `json:"values"`
	}
	byKey := make(map[string]*stream)
	var order []string
	for _, e := range entries {
		key := e.service + "/" + e.level
		s, ok := byKey[key]
		if !ok {
			s = &stream{Stream: map[string]string{"service": e.service, "level": e.level, "env": envLabel}}
			byKey[key] = s
			order = append(order, key)
		}
		s.Values = append(s.Values, [2]string{strconv.FormatInt(e.tsNs, 10), e.line})
	}
	streams := make([]*stream, 0, len(order))
	for _, k := range order {
		streams = append(streams, byKey[k])
	}
	return json.Marshal(map[string]any{"streams": streams})
}

// postBatch sends one push and returns the status plus a short body snippet.
func postBatch(client *http.Client, addr string, body []byte) (int, string, error) {
	resp, err := client.Post(addr+"/loki/api/v1/push", "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, string(snippet), nil
}

// tickerInterval converts a batches-per-second rate into a ticker interval,
// rejecting every input that time.NewTicker would panic on.
//
// A bare `rate <= 0` guard is not enough. NaN fails every comparison, so it
// slips through and divides to NaN, and a large enough rate divides to a
// sub-nanosecond interval that truncates to zero — both reach NewTicker as a
// non-positive duration and panic with a stack trace instead of the clean
// message the flag guard is there to print.
func tickerInterval(rate float64) (time.Duration, error) {
	if math.IsNaN(rate) || math.IsInf(rate, 0) {
		return 0, fmt.Errorf("--rate must be a finite number, got %v", rate)
	}
	if rate <= 0 {
		return 0, fmt.Errorf("--rate must be greater than 0")
	}
	// Bound the quotient BEFORE converting. Converting a float64 outside int64's
	// range is implementation-defined — on amd64 it wraps negative — so a rate so
	// small that the interval overflows would otherwise fall into the "too large"
	// branch below and report the exact opposite of what went wrong.
	//
	// The comparison is >= because float64(math.MaxInt64) rounds up to 2^63, one
	// past the largest representable Duration; the same off-by-one the Prometheus
	// time-parameter bounds already guard against.
	ivl := float64(time.Second) / rate
	if ivl >= float64(math.MaxInt64) {
		return 0, fmt.Errorf("--rate %v is too small; it yields an interval longer than the maximum %v", rate, time.Duration(math.MaxInt64))
	}
	d := time.Duration(ivl)
	if d <= 0 {
		return 0, fmt.Errorf("--rate %v is too large; it yields a sub-nanosecond interval", rate)
	}
	return d, nil
}

func main() {
	addr := flag.String("addr", defaultAddr(), "backend base URL; OBS_BACKEND_ADDR env var takes precedence if set")
	rate := flag.Float64("rate", 2, "log batches per second (must be > 0)")
	duration := flag.Int("duration", 0, "run duration in seconds; 0 = run until interrupted")
	flag.Parse()

	// env var takes precedence over -addr flag
	if v := os.Getenv("OBS_BACKEND_ADDR"); v != "" {
		*addr = v
	}

	interval, err := tickerInterval(*rate)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error: "+err.Error())
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if *duration > 0 {
		var dcancel context.CancelFunc
		ctx, dcancel = context.WithTimeout(ctx, time.Duration(*duration)*time.Second)
		defer dcancel()
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	client := &http.Client{Timeout: 5 * time.Second}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var batches, lines, errs int

	log.Printf("sample-app: addr=%s rate=%.1f/s duration=%ds", *addr, *rate, *duration)

	for {
		select {
		case <-ctx.Done():
			log.Printf("sample-app stopped: batches=%d lines=%d errors=%d", batches, lines, errs)
			return
		case <-ticker.C:
			batch := buildBatch(r, time.Now().UnixNano())
			body, err := encodePush(batch)
			if err != nil {
				log.Printf("marshal error: %v", err)
				errs++
				continue
			}
			// A demo generator must survive a backend restart, so every failure is
			// logged and counted rather than fatal.
			status, snippet, err := postBatch(client, *addr, body)
			if err != nil {
				log.Printf("POST error: %v", err)
				errs++
				continue
			}
			if status < 200 || status >= 300 {
				log.Printf("unexpected status %d: %s", status, snippet)
				errs++
				continue
			}
			batches++
			lines += len(batch)
		}
	}
}
