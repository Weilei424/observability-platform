// Command sample-app emits Loki-style log streams to the observability-platform
// backend so the Grafana logs demo has live data. It is the logs counterpart to
// examples/load-generator (metrics) and deliberately emits no metrics of its own:
// two processes writing independent counters to one series would make rate()
// meaningless. Application metrics move here in Phase 5.1.
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
// The rate uses %g rather than %.1f: one decimal place flattens 0.01 — and every
// smaller accepted rate — to "0.0", which is the one value tickerInterval refuses
// to start on, so the banner would report a rate the program would have rejected.
// The resolved interval is printed beside it because that is what the process
// will actually do; a rate near either representable bound makes that obvious.
func startupLine(addr string, rate float64, interval time.Duration, duration int) string {
	return fmt.Sprintf("sample-app: addr=%s rate=%g/s interval=%v duration=%ds", addr, rate, interval, duration)
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

	log.Print(startupLine(*addr, *rate, interval, *duration))

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
			if err := postBatch(client, *addr, body); err != nil {
				log.Printf("push failed: %v", err)
				errs++
				continue
			}
			batches++
			lines += len(batch)
		}
	}
}
