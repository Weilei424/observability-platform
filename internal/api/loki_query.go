package api

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/masonwheeler/observability-platform/internal/logs"
	"github.com/masonwheeler/observability-platform/internal/metrics"
)

// maxEpochSeconds is the largest whole-second epoch whose nanosecond count still
// fits in an int64 (~year 2262). It only guards the float→int64 conversion below;
// the authoritative range check is nanosOf.
const maxEpochSeconds = math.MaxInt64 / int64(time.Second)

// Go can represent Unix nanoseconds only between 1677-09-21T00:12:43.145224192Z
// and 2262-04-11T23:47:16.854775807Z — the int64 extremes exactly. (Go's own
// UnixNano doc rounds the lower end up to "before the year 1678"; the real bound
// is three months earlier, so error text quotes 1677.) Time.UnixNano is
// documented as undefined outside that window — it wraps rather than failing —
// so every path that converts a time.Time to nanoseconds has to be bounded
// first, or an out-of-range request silently becomes a garbage query bound
// instead of a 400.
var (
	minNanoTime = time.Unix(0, math.MinInt64)
	maxNanoTime = time.Unix(0, math.MaxInt64)
)

func nanosOf(t time.Time, raw string) (int64, error) {
	if t.Before(minNanoTime) || t.After(maxNanoTime) {
		return 0, fmt.Errorf("timestamp %q is outside the representable nanosecond range (1677-2262)", raw)
	}
	return t.UnixNano(), nil
}

// parseLokiTime parses a Loki timestamp, mirroring Loki's own parseTimestamp
// (pkg/loghttp/params.go) so the same string means the same instant here:
//
//   - contains '.'        → float seconds       ("1700000000.5")
//   - integer, ≤10 digits → whole seconds       ("1700000000")
//   - integer, >10 digits → nanoseconds         ("1700000000000000000")
//   - otherwise           → RFC3339 / RFC3339Nano
//
// The digit-length rule is surprising, but it is the upstream contract and it is
// what a hand-written `curl ...&start=1700000000` depends on: without it a
// second-granularity epoch would be read as a few nanoseconds after 1970 and
// quietly return nothing.
func parseLokiTime(s string) (int64, error) {
	if strings.Contains(s, ".") {
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			sec, frac := math.Modf(f)
			// Bound before the int64 conversion, which is undefined for a float
			// outside int64's range. nanosOf then does the exact check.
			if sec > float64(maxEpochSeconds) || sec < float64(-maxEpochSeconds) {
				return 0, fmt.Errorf("timestamp %q is outside the representable nanosecond range (1677-2262)", s)
			}
			// Loki rounds the fraction to milliseconds, so float noise in the
			// low digits does not leak into the nanosecond value. Sub-millisecond
			// precision in a float timestamp is dropped, upstream and here alike.
			frac = math.Round(frac*1000) / 1000
			return nanosOf(time.Unix(int64(sec), int64(frac*float64(time.Second))), s)
		}
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		// Loki measures the raw string, sign included: "-1234567890" is 11
		// characters and so reads as nanoseconds, not seconds. Mirroring the raw
		// length keeps the parity claim literally true.
		if len(s) > 10 {
			return n, nil // already nanoseconds
		}
		return nanosOf(time.Unix(n, 0), s)
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return nanosOf(t, s)
		}
	}
	return 0, fmt.Errorf("invalid timestamp %q: want a Unix epoch (seconds, float seconds, or nanoseconds) or RFC3339", s)
}

// sinceStart resolves Loki's `since` parameter — a duration measured back from
// anchorNs — into an absolute start timestamp.
//
// The grammar is Prometheus's, not Go's: upstream parses `since` with
// prometheus/common's model.ParseDuration (pkg/loghttp/params.go), so `1d` and
// `1w` are valid while `150ns` and `1.5h` are not, and a bare `0` is the one
// value allowed without a unit. metrics.ParsePromDurationNanos is that same
// grammar at the same resolution. The grammar has no sign, so a negative
// duration is unrepresentable.
func sinceStart(s string, anchorNs int64) (int64, error) {
	ns, err := metrics.ParsePromDurationNanos(s)
	if err != nil {
		return 0, fmt.Errorf("%q is not a duration: want a Prometheus duration such as 5m, 1h30m, 1d, or 0", s)
	}
	// The subtraction wraps rather than fails, and a wrapped bound is worse than
	// an error: it lands somewhere plausible instead of returning a 400.
	if anchorNs < math.MinInt64+ns {
		return 0, fmt.Errorf("duration %q reaches outside the representable nanosecond range (1677-2262)", s)
	}
	return anchorNs - ns, nil
}

// parseLokiStep parses `step` the way upstream's parseSecondsOrDuration does:
// float seconds first, then the Prometheus duration grammar. It returns
// **nanoseconds**, not milliseconds, because that is the resolution Loki keeps —
// a sub-millisecond step is legal there and decides whether the points limit
// trips. Rounding to milliseconds would reject a valid 100µs step and accept an
// invalid 500µs one.
func parseLokiStep(s string) (int64, error) {
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		ns := f * float64(time.Second)
		// float64(math.MaxInt64) rounds *up* to 2^63, which is one past the last
		// representable int64, so the upper bound has to exclude it. The lower
		// bound is exactly -2^63 and is representable, so it stays inclusive.
		if math.IsNaN(ns) || ns >= float64(math.MaxInt64) || ns < float64(math.MinInt64) {
			return 0, fmt.Errorf("step %q is out of range", s)
		}
		// Truncate rather than round: upstream converts with time.Duration(ts).
		return int64(ns), nil
	}
	return metrics.ParsePromDurationNanos(s)
}

func parseLokiLimit(s string) (int, error) {
	if s == "" {
		return 100, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid 'limit' parameter %q: want a positive integer", s)
	}
	return n, nil
}

// parseLokiDirection is case-insensitive because upstream is: Loki upper-cases
// the value before matching it against its protobuf enum names, so "BACKWARD"
// and "Forward" are as valid as the lowercase spellings Grafana happens to send.
func parseLokiDirection(s string) (logs.Direction, error) {
	switch strings.ToLower(s) {
	case "", "backward":
		return logs.Backward, nil
	case "forward":
		return logs.Forward, nil
	default:
		return logs.Backward, fmt.Errorf("invalid 'direction' parameter %q: want 'forward' or 'backward'", s)
	}
}

// requireLogQuery guards handlers when no logs query engine is configured (e.g.
// metrics-only test wiring). Production always configures it.
func (s *Server) requireLogQuery(w http.ResponseWriter, r *http.Request) bool {
	if s.logQuery == nil {
		s.log.Error("logs query engine not configured", "component", "logs_query",
			"request_id", chimiddleware.GetReqID(r.Context()))
		writeLokiError(w, http.StatusInternalServerError, "logs query engine not configured")
		return false
	}
	return true
}

// parseLokiQueryParams performs the parameter parsing shared by the query_range
// and instant query handlers: the log-query-engine guard, form parsing, the
// required 'query' parameter, 'limit', and 'direction'. It deliberately does not
// parse the query expression — the two endpoints accept different expression
// kinds (see handleLokiQuery). On any failure it writes the plain-text error
// response itself and returns ok=false; callers must return immediately when ok
// is false. On success, r.Form is populated so callers can read their own
// remaining parameters (start/end/time) from it directly.
func (s *Server) parseLokiQueryParams(w http.ResponseWriter, r *http.Request) (queryStr string, limit int, dir logs.Direction, ok bool) {
	if !s.requireLogQuery(w, r) {
		return "", 0, 0, false
	}
	if err := r.ParseForm(); err != nil {
		writeLokiError(w, http.StatusBadRequest, "invalid request: "+err.Error())
		return "", 0, 0, false
	}
	q := r.Form
	queryStr = q.Get("query")
	if queryStr == "" {
		writeLokiError(w, http.StatusBadRequest, "missing required parameter 'query'")
		return "", 0, 0, false
	}
	limit, err := parseLokiLimit(q.Get("limit"))
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, err.Error())
		return "", 0, 0, false
	}
	dir, err = parseLokiDirection(q.Get("direction"))
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, err.Error())
		return "", 0, 0, false
	}
	return queryStr, limit, dir, true
}

// parseLokiLogSelector parses queryStr as a log query, writing the plain-text
// 400 itself on failure.
func parseLokiLogSelector(w http.ResponseWriter, queryStr string) (logs.LogSelector, bool) {
	sel, err := logs.ParseLogQL(queryStr)
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, err.Error())
		return logs.LogSelector{}, false
	}
	return sel, true
}

// respondLokiStreams runs fetch and writes its result as a Loki streams
// envelope, or logs the error and writes a 500 on failure. logMsg distinguishes
// the query_range vs instant query call site in the log line.
func (s *Server) respondLokiStreams(w http.ResponseWriter, r *http.Request, logMsg string, fetch func() ([]logs.StreamResult, error)) {
	results, err := fetch()
	if err != nil {
		s.log.Error(logMsg, "component", "logs_query",
			"request_id", chimiddleware.GetReqID(r.Context()), "err", err)
		writeLokiError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeLokiStreams(w, toLokiStreamResults(results))
}

func (s *Server) handleLokiQueryRange(w http.ResponseWriter, r *http.Request) {
	queryStr, limit, dir, ok := s.parseLokiQueryParams(w, r)
	if !ok {
		return
	}

	// Dispatch on the expression kind before reading any time parameter, so a
	// malformed query reports the query error rather than a time error. An
	// expression opening with '{' is a log query; anything else is a metric query
	// or a constant expression, which belongs on the instant endpoint.
	var (
		sel      logs.LogSelector
		mq       logs.MetricQuery
		isMetric bool
	)
	if trimmed := strings.TrimSpace(queryStr); trimmed != "" && trimmed[0] != '{' {
		parsed, err := logs.ParseMetricQuery(trimmed)
		switch {
		case errors.Is(err, logs.ErrNotMetricQuery):
			writeLokiError(w, http.StatusBadRequest,
				"constant expressions such as vector(1) are only supported on the instant query endpoint /loki/api/v1/query")
			return
		case err != nil:
			writeLokiError(w, http.StatusBadRequest, err.Error())
			return
		}
		mq, isMetric = parsed, true
	} else if sel, ok = parseLokiLogSelector(w, queryStr); !ok {
		return
	}

	q := r.Form

	// `interval` thins a stream response, returning the entry at start and then
	// the next one at or after each interval boundary. Ignoring it would hand back
	// *more* entries than asked for and look like a working filter, so it is
	// rejected outright per the project guardrail that unsupported query features
	// error explicitly.
	if q.Get("interval") != "" {
		writeLokiError(w, http.StatusBadRequest,
			"unsupported parameter 'interval': log entry sampling is not implemented; omit it to receive every matching entry (metric queries use 'step' instead)")
		return
	}

	nowNs := time.Now().UnixNano()
	endNs := nowNs
	var err error
	if raw := q.Get("end"); raw != "" {
		endNs, err = parseLokiTime(raw)
		if err != nil {
			writeLokiError(w, http.StatusBadRequest, "invalid 'end' parameter: "+err.Error())
			return
		}
	}

	// Loki anchors a *relative* start to min(end, now): a caller asking for the
	// last five minutes with `end` in the future wants the last five minutes of
	// data, not an all-future window. Both the one-hour default and `since` hang
	// off this anchor.
	anchorNs := endNs
	if anchorNs > nowNs {
		anchorNs = nowNs
	}

	// Precedence, per the upstream range-query contract: an explicit `start`
	// supersedes `since`, which supersedes the one-hour default.
	startNs := anchorNs - int64(time.Hour)
	if raw := q.Get("since"); raw != "" {
		startNs, err = sinceStart(raw, anchorNs)
		if err != nil {
			writeLokiError(w, http.StatusBadRequest, "invalid 'since' parameter: "+err.Error())
			return
		}
	}
	if raw := q.Get("start"); raw != "" {
		startNs, err = parseLokiTime(raw)
		if err != nil {
			writeLokiError(w, http.StatusBadRequest, "invalid 'start' parameter: "+err.Error())
			return
		}
	}
	if endNs < startNs {
		writeLokiError(w, http.StatusBadRequest, "invalid time range: 'end' must be >= 'start'")
		return
	}

	stepNs, ok := resolveLokiStep(w, q.Get("step"), startNs, endNs)
	if !ok {
		return
	}

	if isMetric {
		series, err := s.logQuery.EvalMetricRange(r.Context(), mq, startNs, endNs, stepNs)
		if err != nil {
			s.log.Error("loki metric query_range failed", "component", "logs_query",
				"request_id", chimiddleware.GetReqID(r.Context()), "err", err)
			writeLokiError(w, http.StatusInternalServerError, "internal error")
			return
		}
		writeLokiMatrix(w, toLokiMatrixSeries(series))
		return
	}

	s.respondLokiStreams(w, r, "loki query_range failed", func() ([]logs.StreamResult, error) {
		return s.logQuery.QueryRange(r.Context(), sel, startNs, endNs, limit, dir)
	})
}

// maxLokiPoints is Loki's safety limit on points per timeseries — enough for 60s
// resolution over a week, or 1h resolution over a year.
const maxLokiPoints = 11000

// resolveLokiStep returns the step in nanoseconds, writing the plain-text 400
// itself on failure. `step` does not shape a stream response — the log path calls
// this for validation and discards the result — but it is the tick spacing of a
// metric query.
//
// An absent step is derived exactly as upstream's ParseRangeQuery does:
// max(floor(rangeSeconds/250), 1) seconds, which is positive by construction and
// yields at most 250 points, so it needs no further checking.
func resolveLokiStep(w http.ResponseWriter, raw string, startNs, endNs int64) (int64, bool) {
	// endNs >= startNs is already established, so a negative difference can only
	// mean the true span exceeded int64 nanoseconds (~292 years) and wrapped.
	// Saturate instead of rejecting: upstream computes the span with
	// time.Time.Sub, which clamps to the maximum Duration.
	rangeNs := endNs - startNs
	if rangeNs < 0 {
		rangeNs = math.MaxInt64
	}

	if raw == "" {
		seconds := rangeNs / int64(time.Second) / 250
		if seconds < 1 {
			seconds = 1
		}
		return seconds * int64(time.Second), true
	}

	stepNs, err := parseLokiStep(raw)
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, "invalid 'step' parameter: "+raw)
		return 0, false
	}
	if stepNs <= 0 {
		writeLokiError(w, http.StatusBadRequest, "invalid 'step' parameter: must be greater than 0")
		return 0, false
	}
	if rangeNs/stepNs > maxLokiPoints {
		writeLokiError(w, http.StatusBadRequest,
			fmt.Sprintf("exceeded maximum resolution of %d points per timeseries; try decreasing the query resolution ('step')", maxLokiPoints))
		return 0, false
	}
	return stepNs, true
}

// handleLokiQuery serves the instant query endpoint. It accepts two expression
// kinds:
//
//   - a stream selector, evaluated as a log query over [0, time]. Upstream Loki
//     rejects log queries here with a 400 and directs them to query_range; we
//     accept them as a deliberate superset. Grafana never sends a log query to
//     this endpoint, so nothing depends on the stricter behavior.
//   - a constant metric expression such as vector(1)+vector(1), returning a
//     "vector" envelope. This is the Grafana Loki datasource health check.
func (s *Server) handleLokiQuery(w http.ResponseWriter, r *http.Request) {
	queryStr, limit, dir, ok := s.parseLokiQueryParams(w, r)
	if !ok {
		return
	}
	q := r.Form

	timeNs := time.Now().UnixNano()
	if raw := q.Get("time"); raw != "" {
		var err error
		timeNs, err = parseLokiTime(raw)
		if err != nil {
			writeLokiError(w, http.StatusBadRequest, "invalid 'time' parameter: "+err.Error())
			return
		}
	}

	// A query that does not open with a stream selector is a metric query or a
	// constant expression. ParseMetricQuery owns the former and reports
	// ErrNotMetricQuery for the latter, which falls through to the constant
	// subset — the path Grafana's datasource health check takes.
	if trimmed := strings.TrimSpace(queryStr); trimmed != "" && trimmed[0] != '{' {
		mq, err := logs.ParseMetricQuery(trimmed)
		switch {
		case err == nil:
			samples, evalErr := s.logQuery.EvalMetricInstant(r.Context(), mq, timeNs)
			if evalErr != nil {
				s.log.Error("loki metric query failed", "component", "logs_query",
					"request_id", chimiddleware.GetReqID(r.Context()), "err", evalErr)
				writeLokiError(w, http.StatusInternalServerError, "internal error")
				return
			}
			writeLokiVectorSamples(w, toLokiVectorSamples(samples))
			return
		case !errors.Is(err, logs.ErrNotMetricQuery):
			writeLokiError(w, http.StatusBadRequest, err.Error())
			return
		}

		res, err := logs.ParseScalarQuery(trimmed)
		if err != nil {
			writeLokiError(w, http.StatusBadRequest, err.Error())
			return
		}
		if res.HasVector {
			writeLokiVector(w, timeNs, res.Value)
		} else {
			writeLokiScalar(w, timeNs, res.Value)
		}
		return
	}

	sel, ok := parseLokiLogSelector(w, queryStr)
	if !ok {
		return
	}
	s.respondLokiStreams(w, r, "loki query failed", func() ([]logs.StreamResult, error) {
		return s.logQuery.QueryInstant(r.Context(), sel, timeNs, limit, dir)
	})
}

// handleLokiLabels returns all stream label names. start/end/query narrowing is
// accepted but ignored this phase (documented limitation).
func (s *Server) handleLokiLabels(w http.ResponseWriter, r *http.Request) {
	if !s.requireLogQuery(w, r) {
		return
	}
	writeLokiLabels(w, s.logQuery.LabelNames())
}

// handleLokiLabelValues returns all values for the {name} label.
func (s *Server) handleLokiLabelValues(w http.ResponseWriter, r *http.Request) {
	if !s.requireLogQuery(w, r) {
		return
	}
	name := chi.URLParam(r, "name")
	writeLokiLabels(w, s.logQuery.LabelValues(name))
}

func toLokiStreamResults(results []logs.StreamResult) []lokiStreamResult {
	out := make([]lokiStreamResult, 0, len(results))
	for _, rs := range results {
		values := make([][2]string, 0, len(rs.Entries))
		for _, e := range rs.Entries {
			values = append(values, [2]string{strconv.FormatInt(e.TimestampNs, 10), e.Line})
		}
		out = append(out, lokiStreamResult{Stream: rs.Labels, Values: values})
	}
	return out
}

func toLokiMatrixSeries(series []logs.MetricSeries) []lokiMatrixSeries {
	out := make([]lokiMatrixSeries, 0, len(series))
	for _, s := range series {
		values := make([][2]any, 0, len(s.Points))
		for _, p := range s.Points {
			values = append(values, lokiSampleValue(p.TimestampNs, p.Value))
		}
		out = append(out, lokiMatrixSeries{Metric: s.Labels, Values: values})
	}
	return out
}

func toLokiVectorSamples(samples []logs.MetricSample) []lokiVectorSample {
	out := make([]lokiVectorSample, 0, len(samples))
	for _, s := range samples {
		out = append(out, lokiVectorSample{Metric: s.Labels, Value: lokiSampleValue(s.TimestampNs, s.Value)})
	}
	return out
}
