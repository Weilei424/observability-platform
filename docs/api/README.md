# API Reference

The backend serves three API surfaces:

| Surface | Purpose |
|---|---|
| [Metrics](metrics.md) | A project-internal ingest endpoint plus the Prometheus-compatible query API |
| [Logs](logs.md) | The Loki-compatible push and query API |
| [Limitations](limitations.md) | Exactly which query forms are supported, and what the platform does not do |

Prometheus and Loki compatibility is a deliberate subset, not an attempt at
completeness. Anything outside the subset fails with an explicit `400` rather
than being silently ignored — a query that quietly returns the wrong answer is
worse than one that refuses. The authoritative list is
[limitations.md](limitations.md), which is executed against the parsers by the
test suite.

## Response envelopes

The Prometheus-compatible endpoints answer with the Prometheus HTTP API envelope.

Success:

```json
{"status":"success","data":{"resultType":"vector","result":[]},"warnings":[]}
```

Error:

```json
{"status":"error","errorType":"bad_data","error":"invalid query: unsupported function \"avg\""}
```

`warnings` is **always present on success** (as `[]` when empty) and **always
omitted on error**. Two `errorType` values are in use:

| `errorType` | Status | Meaning |
|---|---|---|
| `bad_data` | 400 | The request is malformed, or asks for something outside the supported subset |
| `execution` | 500 | The query parsed but failed while running |

The ingest endpoint and the Loki-compatible endpoints do **not** use this
envelope; each surface documents its own.

## Time parameters

Timestamps and durations are not interchangeable, and the two surfaces differ.

| Parameter | Accepts |
|---|---|
| `time`, `start`, `end` (Prometheus) | Unix seconds as an integer or float, or RFC3339 / RFC3339Nano. **Not a duration** |
| `step` (Prometheus) | Float seconds, or a Prometheus duration |
| `time`, `start`, `end` (Loki) | Unix nanoseconds, Unix seconds, float seconds, or RFC3339 / RFC3339Nano |
| `since` (Loki) | A Prometheus duration, measured back from the anchor |
| `step` (Loki) | Float seconds or a Prometheus duration |

Prometheus duration units are `ms`, `s`, `m`, `h`, `d`, `w`, `y`; Go's forms
(`1.5h`, `150ns`) are not accepted for `since`.

On the Loki endpoints a bare integer is read by its **string length**, as
upstream Loki does: 10 characters or fewer is seconds, longer is nanoseconds. A
float (one containing `.`) is always seconds, and its precision is rounded to
milliseconds.

## Methods

The Prometheus-compatible endpoints accept **both GET and POST** with identical
semantics. POST exists because Grafana sends long queries as a form body rather
than a query string.

The Loki-compatible endpoints are **GET-only, except `push`**, which is POST.

## Operational endpoints

```http
GET /healthz
```

Liveness. Returns `200` with `{"status":"ok"}` whenever the process is running.
Deliberately not coupled to disk: a full or unwritable volume must not turn into
a restart loop.

```http
GET /readyz
```

Readiness. Creates and removes a temporary file in the data directory, so a
`200` with `{"status":"ok"}` is evidence that storage is actually writable. On
failure it returns `503` with `{"status":"unavailable","reason":"..."}`. This is
the probe that proves storage works, and the one Kubernetes gates traffic on.

```http
GET /metrics
```

Prometheus exposition of the platform's own internals — ingestion rate, query
latency, WAL size, block and chunk counts, compaction progress, error counts.
Registered only when the server is built with a metrics registry. It is scraped
by a separate Prometheus instance, not by this backend; see
[../runbooks/self-observability.md](../runbooks/self-observability.md).
