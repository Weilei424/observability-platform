// Command sample-app emits Loki-style log streams and its own metrics to the
// observability-platform backend, so both Grafana demos have live data.
//
// Its metric names are disjoint from examples/load-generator's http_* series.
// The separation is by metric NAME rather than by a service label because the
// provisioned metrics dashboard aggregates http_requests_total with no service
// filter — a second writer under that name would silently fold into panels it
// has nothing to do with. See metrics.go.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func defaultAddr() string {
	if v := os.Getenv("OBS_BACKEND_ADDR"); v != "" {
		return v
	}
	return "http://localhost:8080"
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

// startupLine renders the one-line startup banner. It is a separate function so
// the formatting can be pinned by a test.
//
// The rates use %g rather than %.1f: one decimal place flattens 0.01 — and every
// smaller accepted rate — to "0.0", which is the one value tickerInterval refuses
// to start on, so the banner would report a rate the program would have rejected.
// The resolved intervals are printed beside them because that is what the process
// will actually do; a rate near either representable bound makes that obvious.
func startupLine(addr string, rate float64, interval time.Duration, metricsRate float64, metricsInterval time.Duration, duration int) string {
	return fmt.Sprintf("sample-app: addr=%s rate=%g/s interval=%v metrics_rate=%g/s metrics_interval=%v duration=%ds",
		addr, rate, interval, metricsRate, metricsInterval, duration)
}

func main() {
	addr := flag.String("addr", defaultAddr(), "backend base URL; OBS_BACKEND_ADDR env var takes precedence if set")
	rate := flag.Float64("rate", 2, "log batches per second (must be > 0)")
	metricsRate := flag.Float64("metrics-rate", 1, "metric pushes per second (must be > 0)")
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

	metricsInterval, err := tickerInterval(*metricsRate)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error: --metrics-rate: "+err.Error())
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

	metricsTicker := time.NewTicker(metricsInterval)
	defer metricsTicker.Stop()

	// OBS_INSTANCE is set from the pod name by the producers chart; unset
	// everywhere else. See newWorkload.
	wl := newWorkload(os.Getenv("OBS_INSTANCE"))

	client := &http.Client{Timeout: 5 * time.Second}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var batches, lines, pushes, errs int

	log.Print(startupLine(*addr, *rate, interval, *metricsRate, metricsInterval, *duration))

	for {
		select {
		case <-ctx.Done():
			log.Printf("sample-app stopped: batches=%d lines=%d metric_pushes=%d errors=%d", batches, lines, pushes, errs)
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
			if err := postBatch(client, *addr, body); err != nil {
				log.Printf("push failed: %v", err)
				errs++
				continue
			}
			batches++
			lines += len(batch)
		case <-metricsTicker.C:
			// Same goroutine as the log case above on purpose: the workload
			// counters and the *rand.Rand both stay single-owner, and rand.Rand
			// is not safe for concurrent use.
			wl.tick(r)
			body, err := encodeMetrics(wl.samples(time.Now().UnixMilli()))
			if err != nil {
				log.Printf("metrics marshal error: %v", err)
				errs++
				continue
			}
			if err := postMetrics(client, *addr, body); err != nil {
				log.Printf("metrics push failed: %v", err)
				errs++
				continue
			}
			pushes++
		}
	}
}
