package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestHealthcheckRequested(t *testing.T) {
	cases := []struct {
		args []string
		want bool
	}{
		{nil, false},
		{[]string{}, false},
		{[]string{"-healthcheck"}, true},
		{[]string{"--healthcheck"}, true},
		{[]string{"-other", "-healthcheck"}, true},
		{[]string{"-healthcheckery"}, false},
		{[]string{"healthcheck"}, false},
		{[]string{"-serve"}, false},
	}
	for _, tc := range cases {
		if got := healthcheckRequested(tc.args); got != tc.want {
			t.Errorf("healthcheckRequested(%v) = %v, want %v", tc.args, got, tc.want)
		}
	}
}

// TestProbeURL covers every address form the container and the local runtime
// can produce. Wildcard hosts are the important ones: 0.0.0.0 and :: are
// bindable but not dialable, so probing them verbatim fails on some stacks.
func TestProbeURL(t *testing.T) {
	cases := []struct {
		addr string
		want string
	}{
		{":8080", "http://127.0.0.1:8080/readyz"},
		{"0.0.0.0:8080", "http://127.0.0.1:8080/readyz"},
		{"[::]:8080", "http://127.0.0.1:8080/readyz"},
		{"127.0.0.1:9090", "http://127.0.0.1:9090/readyz"},
		{"10.0.0.5:8080", "http://10.0.0.5:8080/readyz"},
		{"[::1]:8080", "http://[::1]:8080/readyz"},
		{"backend:8080", "http://backend:8080/readyz"},
	}
	for _, tc := range cases {
		got, err := probeURL(tc.addr)
		if err != nil {
			t.Errorf("probeURL(%q) returned error %v", tc.addr, err)
			continue
		}
		if got != tc.want {
			t.Errorf("probeURL(%q) = %q, want %q", tc.addr, got, tc.want)
		}
	}
}

// TestProbeURL_Errors pins the two inputs that cannot produce a usable URL.
// Port 0 matters: the bench runner starts the server that way, and the actual
// port is known only to the running process (it publishes it via OBS_ADDR_FILE).
func TestProbeURL_Errors(t *testing.T) {
	cases := []struct{ addr, wantSubstring string }{
		{":0", "port 0"},
		{"0.0.0.0:0", "port 0"},
		{"not-an-address", "OBS_HTTP_ADDR"},
		{"", "OBS_HTTP_ADDR"},
	}
	for _, tc := range cases {
		got, err := probeURL(tc.addr)
		if err == nil {
			t.Errorf("probeURL(%q) = %q, want an error", tc.addr, got)
			continue
		}
		if !strings.Contains(err.Error(), tc.wantSubstring) {
			t.Errorf("probeURL(%q) error = %q, want it to mention %q", tc.addr, err, tc.wantSubstring)
		}
	}
}

func TestRunHealthcheck_ExitCodes(t *testing.T) {
	cases := []struct {
		name     string
		status   int
		wantCode int
	}{
		{"ready", http.StatusOK, 0},
		{"unavailable", http.StatusServiceUnavailable, 1},
		{"not found", http.StatusNotFound, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotPath string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.Path
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(`{"status":"unavailable","reason":"disk"}`))
			}))
			defer srv.Close()

			var stderr bytes.Buffer
			code := runHealthcheck(srv.URL+"/readyz", srv.Client(), &stderr)
			if code != tc.wantCode {
				t.Errorf("exit code = %d, want %d (stderr: %s)", code, tc.wantCode, stderr.String())
			}
			if gotPath != "/readyz" {
				t.Errorf("probed %q, want /readyz", gotPath)
			}
			if tc.wantCode == 1 && !strings.Contains(stderr.String(), strconv.Itoa(tc.status)) {
				t.Errorf("stderr = %q, want the %d status named so the container log says why", stderr.String(), tc.status)
			}
			if tc.wantCode == 0 && stderr.Len() != 0 {
				t.Errorf("stderr = %q, want silence on success", stderr.String())
			}
		})
	}
}

// TestRunHealthcheck_TransportError is the state Compose spends the first
// seconds of every `up` in: nothing is listening yet.
func TestRunHealthcheck_TransportError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL + "/readyz"
	client := srv.Client()
	srv.Close() // nothing is listening now

	var stderr bytes.Buffer
	if code := runHealthcheck(url, client, &stderr); code != 1 {
		t.Errorf("exit code = %d, want 1 when the connection is refused", code)
	}
	if stderr.Len() == 0 {
		t.Error("stderr is empty; a failed probe must say why")
	}
}

// TestRunHealthcheck_HonorsClientTimeout keeps the probe bounded: Compose
// bounds it too, but a probe that hangs past its own timeout would leave the
// container marked healthy for one extra interval.
func TestRunHealthcheck_HonorsClientTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := srv.Client()
	client.Timeout = 50 * time.Millisecond

	var stderr bytes.Buffer
	if code := runHealthcheck(srv.URL+"/readyz", client, &stderr); code != 1 {
		t.Errorf("exit code = %d, want 1 when the probe times out", code)
	}
}
