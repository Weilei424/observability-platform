package e2e_test

// Fixture tests for the response-assertion helpers in kind_smoke.sh.
//
// The script itself needs a kind cluster, so nothing in CI runs it on every
// commit — which is how it shipped with an assertion that returned 0 for every
// valid backend response and turned a working deployment into a 60-second
// timeout and a failure. The helpers are pure functions of a response body, so
// they are testable without a cluster: this sources the script with
// OBS_KIND_SMOKE_LIB_ONLY=1 (which returns before the first kubectl call) and
// feeds each helper the response shapes the real run will hand it.

import (
	"os/exec"
	"strings"
	"testing"
)

const kindSmokeScript = "kind_smoke.sh"

// callHelper sources kind_smoke.sh in lib-only mode and invokes one helper with
// body as its single argument, returning the helper's stdout.
func callHelper(t *testing.T, fn, body string) string {
	t.Helper()
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq not installed; the smoke-script helpers are jq wrappers")
	}
	// The body goes through the environment, not the command line, so a fixture
	// containing quotes or backslashes cannot be mangled by shell quoting and
	// silently turn into a different test than the one written here.
	cmd := exec.Command("bash", "-c",
		`OBS_KIND_SMOKE_LIB_ONLY=1 . ./`+kindSmokeScript+` && `+fn+` "$BODY"`)
	cmd.Env = append(cmd.Environ(), "BODY="+body)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v\noutput: %s", fn, err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestPromSampleCount(t *testing.T) {
	tests := map[string]struct {
		body string
		want string
	}{
		// The shape internal/api/query.go actually emits: the value is
		// [<number>, "<string>"], which is why the Grafana-dataframe helper
		// (numeric_columns) counts 0 here and must not be used on it.
		"valid vector, one sample": {
			`{"status":"success","data":{"resultType":"vector","result":[` +
				`{"metric":{"__name__":"http_requests_total","method":"GET"},"value":[1756600000,"42.5"]}]}}`,
			"1",
		},
		"valid vector, two series": {
			`{"status":"success","data":{"resultType":"vector","result":[` +
				`{"metric":{"method":"GET"},"value":[1756600000,"42.5"]},` +
				`{"metric":{"method":"POST"},"value":[1756600000,"7"]}]}}`,
			"2",
		},
		"negative and exponent values still count": {
			`{"status":"success","data":{"resultType":"vector","result":[` +
				`{"metric":{},"value":[1756600000,"-0.5"]},` +
				`{"metric":{},"value":[1756600000,"1.5e+07"]}]}}`,
			"2",
		},
		// A series that exists with no samples in range: the query succeeded,
		// but there is nothing to prove a producer is writing.
		"empty result": {
			`{"status":"success","data":{"resultType":"vector","result":[]}}`,
			"0",
		},
		"error envelope": {
			`{"status":"error","errorType":"bad_data","error":"invalid query"}`,
			"0",
		},
		// Prometheus renders these as strings like any other value; neither is
		// evidence that a real sample arrived.
		"NaN and Inf are not samples": {
			`{"status":"success","data":{"resultType":"vector","result":[` +
				`{"metric":{},"value":[1756600000,"NaN"]},` +
				`{"metric":{},"value":[1756600000,"+Inf"]}]}}`,
			"0",
		},
		"malformed json":     {`{"status":"success","data":{`, "0"},
		"not json at all":    {`<html><body>502 Bad Gateway</body></html>`, "0"},
		"empty body":         {``, "0"},
		"json but not a map": {`[1,2,3]`, "0"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := callHelper(t, "prom_sample_count", tc.body); got != tc.want {
				t.Errorf("prom_sample_count = %q, want %q\nbody: %s", got, tc.want, tc.body)
			}
		})
	}
}

func TestLokiEntryCount(t *testing.T) {
	tests := map[string]struct {
		body string
		want string
	}{
		"two entries in one stream": {
			`{"status":"success","data":{"resultType":"streams","result":[` +
				`{"stream":{"service":"worker","level":"info"},"values":[` +
				`["1756600000000000000","job flush=block-0001 completed in 120ms"],` +
				`["1756600001000000000","job compaction=block-0002 completed in 900ms"]]}]}}`,
			"2",
		},
		"entries across two streams": {
			`{"status":"success","data":{"resultType":"streams","result":[` +
				`{"stream":{"service":"worker","level":"info"},"values":[["1756600000000000000","a"]]},` +
				`{"stream":{"service":"worker","level":"error"},"values":[["1756600001000000000","b"]]}]}}`,
			"2",
		},
		"stream present but empty": {
			`{"status":"success","data":{"resultType":"streams","result":[` +
				`{"stream":{"service":"worker"},"values":[]}]}}`,
			"0",
		},
		"no streams matched": {
			`{"status":"success","data":{"resultType":"streams","result":[]}}`,
			"0",
		},
		"error envelope":  {`{"status":"error","error":"parse error"}`, "0"},
		"malformed json":  {`{"status":"success","data":`, "0"},
		"not json at all": {`upstream connect error`, "0"},
		"empty body":      {``, "0"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := callHelper(t, "loki_entry_count", tc.body); got != tc.want {
				t.Errorf("loki_entry_count = %q, want %q\nbody: %s", got, tc.want, tc.body)
			}
		})
	}
}

// numeric_columns is correct for Grafana's /api/ds/query dataframes, where both
// columns are JSON numbers, and wrong for the backend's Prometheus API. Pinning
// both halves keeps the two helpers from being swapped back by a later edit.
func TestNumericColumnsIsForGrafanaDataframesOnly(t *testing.T) {
	dataframe := `{"results":{"A":{"frames":[{"schema":{"fields":[{"name":"Time"},{"name":"Value"}]},` +
		`"data":{"values":[[1756600000000,1756600005000],[1,2]]}}]}}}`
	if got := callHelper(t, "numeric_columns", dataframe); got != "2" {
		t.Errorf("numeric_columns(dataframe) = %q, want %q", got, "2")
	}

	promVector := `{"status":"success","data":{"resultType":"vector","result":[` +
		`{"metric":{},"value":[1756600000,"42.5"]}]}}`
	if got := callHelper(t, "numeric_columns", promVector); got != "0" {
		t.Errorf("numeric_columns(prometheus vector) = %q, want %q — if this now returns a "+
			"positive count the helper changed; the smoke script still must not use it there", got, "0")
	}
}
