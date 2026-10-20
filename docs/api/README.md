# API Reference

The backend serves three API surfaces:

| Surface | Purpose |
|---|---|
| [Metrics](metrics.md) | A project-internal ingest endpoint plus the Prometheus-compatible query API |
| [Logs](logs.md) | The Loki-compatible push and query API |
| [Internal](internal.md) | The API the split components use among themselves; never exposed by the gateway |
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
omitted on error**. Five `errorType` values are in use:

| `errorType` | Status | Meaning |
|---|---|---|
| `bad_data` | 400 | The request is malformed, or asks for something outside the supported subset |
| `unauthorized` | 401 | `OBS_TENANT_REQUIRED` is on and the request has no `X-Scope-OrgID` header |
| `canceled` | 499 | The client abandoned the request before it finished |
| `execution` | 500 | The query parsed but failed while running |
| `unavailable` | 503 | A component the query needs could not be reached (split topology only) |

The ingest endpoint and the Loki-compatible endpoints do **not** use this
envelope; each surface documents its own.

## Tenants

Every public route reads the tenant from the `X-Scope-OrgID` header, as Mimir
and Loki do. Two tenants writing to one deployment never see each other's
metrics, logs, label names, label values, or series.

| Header | Result |
|---|---|
| absent or empty | The **default tenant**, `anonymous` (`OBS_TENANT_REQUIRED=false`, the default). With `OBS_TENANT_REQUIRED=true`: `401` |
| `X-Scope-OrgID: anonymous` | The default tenant, explicitly |
| `X-Scope-OrgID: team-b` | Tenant `team-b` |
| invalid ID | `400 invalid tenant ID ...` |
| `a\|b` | `400 multi-tenant queries are not supported` |

A valid ID is 1 to 150 bytes of `[A-Za-z0-9!_.*'()-]`, and not `.` or `..`.
Every existing client (Grafana, the sample app, the load generator) sends no
header and is the default tenant, and data written before tenancy existed is
the default tenant's, with no migration.

The tenant errors take each route family's own shape: the Prometheus envelope
on the query routes (`errorType` `bad_data` for `400`, `unauthorized` for
`401`), plain text on the Loki query routes, and `{"error":"..."}` on both
write routes. `/healthz`, `/readyz`, `/metrics` and `/internal/v1` take no
tenant on the public side.

The write routes add three tenant-specific answers, each limit per tenant and
set by environment defaults plus an optional overrides file
(`OBS_TENANT_OVERRIDES_FILE`):

| Status | Body | When |
|---|---|---|
| 413 | `{"error":"push body exceeds <n> bytes for tenant <t>"}` | The body is over the tenant's `max_metrics_push_bytes` (default 1 MiB) or `max_logs_push_bytes` (default 4 MiB) |
| 429 | `{"error":"tenant <t> would exceed its active series limit (<n>)"}` | The push would take the tenant past its `max_active_series` (logs: `max_active_streams`, and the message says "streams"). Nothing of that push is written on the refusing node; samples for series already in the head are not refused |

The label `__tenant__` is reserved: a client cannot send it (it is refused like
any `__`-prefixed name except `__name__`), no response ever contains it, and a
selector naming it matches nothing. Mind that Grafana is not a tenant boundary:
it sends the one header its datasource is provisioned with, and the demo
provisions a second datasource pair for `team-b`
([../runbooks/local-demo.md](../runbooks/local-demo.md)).

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
