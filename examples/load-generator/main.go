package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
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

// withInstance returns base plus an `instance` label when one is configured,
// and base unchanged when it is not.
//
// The label is what makes replicas > 1 safe. The counters below live in process
// memory, so two replicas emitting identical label sets are two independent
// counters sharing one series identity: their samples interleave at the same
// timestamps and every switch between them looks to rate() like a counter
// reset, which silently understates the metrics dashboard's Request Rate and
// Error Rate panels. The Deployment sets OBS_INSTANCE from the pod name via the
// downward API, giving each replica its own series; the dashboard's sum-by
// panels aggregate them back together correctly.
//
// Unset — Docker Compose, `go run` — the label is omitted entirely, so
// single-writer setups keep exactly the series they had.
func withInstance(instance string, base map[string]string) map[string]string {
	if instance == "" {
		return base
	}
	base["instance"] = instance
	return base
}

func main() {
	addr := flag.String("addr", defaultAddr(), "backend base URL; OBS_BACKEND_ADDR env var takes precedence if set")
	rate := flag.Float64("rate", 5, "requests per second (must be > 0)")
	duration := flag.Int("duration", 0, "run duration in seconds; 0 = run until interrupted")
	flag.Parse()

	// env var takes precedence over -addr flag
	if v := os.Getenv("OBS_BACKEND_ADDR"); v != "" {
		*addr = v
	}

	if *rate <= 0 {
		fmt.Fprintln(os.Stderr, "error: --rate must be greater than 0")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if *duration > 0 {
		var dcancel context.CancelFunc
		ctx, dcancel = context.WithTimeout(ctx, time.Duration(*duration)*time.Second)
		defer dcancel()
	}

	interval := time.Duration(float64(time.Second) / *rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	client := &http.Client{Timeout: 5 * time.Second}
	// OBS_INSTANCE is set from the pod name by the producers chart; unset
	// everywhere else. See withInstance.
	instance := os.Getenv("OBS_INSTANCE")
	lbl := func(m map[string]string) map[string]string { return withInstance(instance, m) }
	var sent, errs int
	var getTotal, postTotal int64
	var getErrors, postErrors int64
	activeConns := float64(10)

	log.Printf("load-generator: addr=%s rate=%.1f/s duration=%ds", *addr, *rate, *duration)

	for {
		select {
		case <-ctx.Done():
			log.Printf("load-generator stopped: sent=%d errors=%d", sent, errs)
			return
		case <-ticker.C:
			// simulate one GET and one POST request this tick regardless of batch outcome
			getTotal++
			postTotal++
			tsMs := time.Now().UnixMilli()

			// random walk for active connections, bounded [1, 50]
			step := rand.Intn(3) + 1 // magnitude 1–3
			if rand.Intn(2) == 0 {
				step = -step
			}
			activeConns += float64(step)
			if activeConns < 1 {
				activeConns = 1
			} else if activeConns > 50 {
				activeConns = 50
			}

			// GET latency [1ms, 200ms], POST latency [5ms, 500ms]
			getLatency := 0.001 + rand.Float64()*0.199
			postLatency := 0.005 + rand.Float64()*0.495

			metrics := []any{
				map[string]any{
					"name":         "http_requests_total",
					"labels":       lbl(map[string]string{"service": "api", "method": "GET", "status": "200"}),
					"timestamp_ms": tsMs,
					"value":        float64(getTotal),
				},
				map[string]any{
					"name":         "http_requests_total",
					"labels":       lbl(map[string]string{"service": "api", "method": "POST", "status": "201"}),
					"timestamp_ms": tsMs,
					"value":        float64(postTotal),
				},
				map[string]any{
					"name":         "http_request_duration_seconds",
					"labels":       lbl(map[string]string{"service": "api", "method": "GET"}),
					"timestamp_ms": tsMs,
					"value":        getLatency,
				},
				map[string]any{
					"name":         "http_request_duration_seconds",
					"labels":       lbl(map[string]string{"service": "api", "method": "POST"}),
					"timestamp_ms": tsMs,
					"value":        postLatency,
				},
				map[string]any{
					"name":         "active_connections",
					"labels":       lbl(map[string]string{"service": "api"}),
					"timestamp_ms": tsMs,
					"value":        activeConns,
				},
			}

			// ~5% error rate per method
			if rand.Float64() < 0.05 {
				getErrors++
				metrics = append(metrics, map[string]any{
					"name":         "http_errors_total",
					"labels":       lbl(map[string]string{"service": "api", "method": "GET", "status": "500"}),
					"timestamp_ms": tsMs,
					"value":        float64(getErrors),
				})
			}
			if rand.Float64() < 0.05 {
				postErrors++
				metrics = append(metrics, map[string]any{
					"name":         "http_errors_total",
					"labels":       lbl(map[string]string{"service": "api", "method": "POST", "status": "503"}),
					"timestamp_ms": tsMs,
					"value":        float64(postErrors),
				})
			}

			body, err := json.Marshal(map[string]any{"metrics": metrics})
			if err != nil {
				log.Printf("marshal error: %v", err)
				errs++
				continue
			}

			resp, err := client.Post(*addr+"/api/v1/ingest/metrics", "application/json", bytes.NewReader(body))
			if err != nil {
				log.Printf("POST error: %v", err)
				errs++
				continue
			}
			func() {
				defer resp.Body.Close()
				io.Copy(io.Discard, resp.Body)
			}()

			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				log.Printf("unexpected status %d", resp.StatusCode)
				errs++
			} else {
				sent++
			}
		}
	}
}
