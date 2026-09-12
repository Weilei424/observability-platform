# Metrics API

Two different things live here: a **project-internal ingest endpoint**, which is
this project's own JSON API and not Prometheus-compatible, and the
**Prometheus-compatible query API**, which Grafana talks to unmodified.

Conventions — envelopes, time formats, methods — are in [README.md](README.md).
Supported query forms are in [limitations.md](limitations.md).

---

## Ingest

```http
POST /api/v1/ingest/metrics
```

Appends samples. This is the endpoint the sample app and load generator use.
Prometheus `remote_write` is not implemented; see [limitations.md](limitations.md).

Each sample is written to the WAL before it is acknowledged, so a `204` means the
data survives a crash.

| Field | Required | Notes |
|---|---|---|
| `name` | yes | Metric name |
| `labels` | yes | Label set; may be empty |
| `timestamp_ms` | yes | Unix milliseconds |
| `value` | yes | Float |

The request body is capped at 1 MiB and must be exactly one JSON object —
anything after it is rejected rather than ignored.

**Example**

```bash
curl -sf -X POST 'http://localhost:8080/api/v1/ingest/metrics' \
  -H 'Content-Type: application/json' \
  -d '{"metrics":[
        {"name":"http_requests_total","labels":{"service":"api","method":"GET","status":"200"},"timestamp_ms":1710000000000,"value":10},
        {"name":"active_connections","labels":{"service":"api"},"timestamp_ms":1710000000000,"value":14}
      ]}'
```

**Responses**

| Status | Body | When |
|---|---|---|
| 204 | empty | Every sample accepted |
| 400 | `{"error":"..."}` | Invalid JSON, trailing data after the object, or an empty/missing `metrics` array |
| 400 | `{"errors":[{"index":0,"field":"name","message":"..."}]}` | One or more entries failed validation; `index` is the position in the array |
| 500 | `{"error":"internal error"}` | The append failed |

These are **not** the Prometheus envelope. This endpoint is not
Prometheus-compatible, and pretending otherwise would suggest a compatibility
that does not exist.

---

## Instant query

```http
GET /api/v1/query
POST /api/v1/query
```

Evaluates an expression at a single point in time.

| Parameter | Required | Default | Notes |
|---|---|---|---|
| `query` | yes | — | PromQL subset; see [limitations.md](limitations.md) |
| `time` | no | now | Unix seconds or a Prometheus duration |

**Example**

```bash
curl -sG 'http://localhost:8080/api/v1/query' \
  --data-urlencode 'query=sum(rate(http_requests_total[1m]))'
```

**Responses**

| Status | `errorType` | When |
|---|---|---|
| 200 | — | `data.resultType` is `vector`, or `scalar` for a numeric expression such as `1+1` |
| 400 | `bad_data` | `query` missing, unparseable, or outside the supported subset; malformed `time` |
| 500 | `execution` | The query failed while running |

---

## Range query

```http
GET /api/v1/query_range
POST /api/v1/query_range
```

Evaluates an expression at every `step` across a window. This is what Grafana
time-series panels call.

| Parameter | Required | Default | Notes |
|---|---|---|---|
| `query` | yes | — | PromQL subset |
| `start` | yes | — | No default — an absent `start` is an error, not "the beginning of time" |
| `end` | yes | — | Must be >= `start` |
| `step` | yes | — | Duration; must be greater than zero |

**Example**

```bash
curl -sG 'http://localhost:8080/api/v1/query_range' \
  --data-urlencode 'query=sum by (method)(rate(http_requests_total[1m]))' \
  --data-urlencode 'start=1710000000' \
  --data-urlencode 'end=1710003600' \
  --data-urlencode 'step=30'
```

**Responses**

| Status | `errorType` | When |
|---|---|---|
| 200 | — | `data.resultType` is `matrix` |
| 400 | `bad_data` | Missing or malformed `query`, `start`, `end`, or `step`; `step` <= 0; `end` < `start` |
| 500 | `execution` | The query failed while running |

---

## Label names

```http
GET /api/v1/labels
POST /api/v1/labels
```

Returns every label name, optionally restricted to series matching a selector.
Grafana calls this to populate label dropdowns.

| Parameter | Required | Default | Notes |
|---|---|---|---|
| `match[]` | no | all series | Repeatable series selector |
| `start` | no | all time | Restricts to series with samples in the window |
| `end` | no | all time | |

**Example**

```bash
curl -s 'http://localhost:8080/api/v1/labels'
```

`data` is an array of strings.

---

## Label values

```http
GET /api/v1/label/{name}/values
POST /api/v1/label/{name}/values
```

Returns every value observed for one label. `{name}` is the label name, in the
path.

| Parameter | Required | Default | Notes |
|---|---|---|---|
| `match[]` | no | all series | Repeatable series selector |
| `start` | no | all time | |
| `end` | no | all time | |

**Example**

```bash
curl -s 'http://localhost:8080/api/v1/label/method/values'
```

`data` is an array of strings.

---

## Series

```http
GET /api/v1/series
POST /api/v1/series
```

Returns the label sets of matching series, without any samples.

| Parameter | Required | Default | Notes |
|---|---|---|---|
| `match[]` | **yes** | — | At least one is required; an unfiltered series dump is not offered |
| `start` | no | all time | |
| `end` | no | all time | |

**Example**

```bash
curl -sG 'http://localhost:8080/api/v1/series' \
  --data-urlencode 'match[]=http_requests_total'
```

**Responses**

| Status | `errorType` | When |
|---|---|---|
| 200 | — | `data` is an array of label-set objects |
| 400 | `bad_data` | No `match[]` given, or a selector is unparseable |
| 500 | `execution` | The lookup failed |
