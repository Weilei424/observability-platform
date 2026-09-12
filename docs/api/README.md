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

`time`, `start`, and `end` on the Prometheus-compatible endpoints accept a unix
timestamp in seconds (fractional allowed) or a Prometheus duration. `step`
accepts a duration. Accepted units: `ms`, `s`, `m`, `h`, `d`, `w`, `y`.

The Loki-compatible endpoints take nanosecond unix timestamps or RFC3339, and
additionally accept `since` as a relative window.

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
