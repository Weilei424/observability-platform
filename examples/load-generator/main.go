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

func main() {
	addr := flag.String("addr", "http://localhost:8080", "backend base URL")
	rate := flag.Float64("rate", 5, "requests per second (must be > 0)")
	duration := flag.Int("duration", 0, "run duration in seconds; 0 = run until interrupted")
	flag.Parse()

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
	var sent, errs int
	var counter1, counter2 int64

	log.Printf("load-generator: addr=%s rate=%.1f/s duration=%ds", *addr, *rate, *duration)

	for {
		select {
		case <-ctx.Done():
			log.Printf("load-generator stopped: sent=%d errors=%d", sent, errs)
			return
		case <-ticker.C:
			counter1++
			counter2++
			tsMs := time.Now().UnixMilli()
			latency := 0.001 + rand.Float64()*0.499 // uniform [0.001, 0.500)

			payload := map[string]any{
				"metrics": []any{
					map[string]any{
						"name":         "http_requests_total",
						"labels":       map[string]string{"service": "api", "method": "GET", "status": "200"},
						"timestamp_ms": tsMs,
						"value":        float64(counter1),
					},
					map[string]any{
						"name":         "http_requests_total",
						"labels":       map[string]string{"service": "api", "method": "POST", "status": "201"},
						"timestamp_ms": tsMs,
						"value":        float64(counter2),
					},
					map[string]any{
						"name":         "http_request_duration_seconds",
						"labels":       map[string]string{"service": "api"},
						"timestamp_ms": tsMs,
						"value":        latency,
					},
				},
			}

			body, err := json.Marshal(payload)
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
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()

			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				log.Printf("unexpected status %d", resp.StatusCode)
				errs++
			} else {
				sent++
			}
		}
	}
}
