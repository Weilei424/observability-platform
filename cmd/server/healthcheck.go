// Probe mode. `/server -healthcheck` performs one readiness probe against a
// server already running in this container and exits 0 or 1.
//
// It exists because the runtime image is distroless: there is no shell, no
// curl, and no wget for a Compose or Kubernetes healthcheck to exec, and the
// only executable present is this binary. Phase 5.2 reuses it as a Kubernetes
// exec probe.

package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
)

// healthcheckRequested reports whether argv asks for probe mode. Both spellings
// are accepted because Compose healthcheck lines are written by hand.
func healthcheckRequested(args []string) bool {
	for _, a := range args {
		if a == "-healthcheck" || a == "--healthcheck" {
			return true
		}
	}
	return false
}

// probeURL derives the /readyz URL to probe from an HTTP listen address.
//
// Wildcard hosts are rewritten to loopback: 0.0.0.0 and :: are valid to bind
// and not reliably valid to dial, and the probe runs inside the very container
// that did the binding, so loopback always reaches it.
func probeURL(httpAddr string) (string, error) {
	host, port, err := net.SplitHostPort(httpAddr)
	if err != nil {
		return "", fmt.Errorf("invalid OBS_HTTP_ADDR %q: %w", httpAddr, err)
	}
	if port == "0" {
		return "", fmt.Errorf(
			"cannot probe OBS_HTTP_ADDR %q: port 0 binds an ephemeral port whose number only the running process knows",
			httpAddr)
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	// JoinHostPort brackets IPv6 literals, which a bare concatenation would not.
	return "http://" + net.JoinHostPort(host, port) + "/readyz", nil
}

// runHealthcheck performs the probe and returns the process exit code: 0 when
// the backend answers 200, 1 for every other outcome.
//
// /readyz is the right target rather than /healthz: it creates and removes a
// temp file in the data directory, so a passing probe means the process is
// serving AND its storage is writable.
func runHealthcheck(url string, client *http.Client, stderr io.Writer) int {
	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintf(stderr, "healthcheck: GET %s: %v\n", url, err)
		return 1
	}
	defer resp.Body.Close()
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(stderr, "healthcheck: GET %s returned %d, want 200: %s\n",
			url, resp.StatusCode, strings.TrimSpace(string(snippet)))
		return 1
	}
	return 0
}
