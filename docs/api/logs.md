# Logs API

A Loki-compatible subset: Grafana's Loki datasource talks to these endpoints
unmodified. Conventions are in [README.md](README.md); supported query forms are
in [limitations.md](limitations.md).

Errors on this surface are **plain text** (`text/plain; charset=utf-8`), not the
Prometheus JSON envelope — that is what upstream Loki does, and Grafana surfaces
the message directly.

---

## Push

```http
POST /loki/api/v1/push
```

Appends log lines to streams. A stream is identified by its label set, exactly as
a metric series is.

Body:

```json
{"streams":[{"stream":{"service":"api","level":"info"},"values":[["1710000000000000000","request completed"]]}]}
```

Each entry is `["<unix_nano>", "<line>"]`, with the timestamp as a **string**.

`Content-Type`, if present, must parse to exactly `application/json`; look-alikes
such as `application/jsonp` and protobuf bodies are rejected rather than guessed
at.

**Validation is all-or-nothing.** Every entry is checked before anything is
buffered, so a push containing one bad entry stores none of them. A
partially-applied push can never be mistaken for a complete one.

A canonical Loki structured-metadata entry (`["<ts>","<line>",{...}]`) decodes
successfully and is then explicitly rejected — structured metadata is not
supported, and silently dropping the third element would lose data the sender
believed was stored.

**Example**

```bash
curl -sf -X POST 'http://localhost:8080/loki/api/v1/push' \
  -H 'Content-Type: application/json' \
  -d "{\"streams\":[{\"stream\":{\"service\":\"api\",\"level\":\"info\"},\"values\":[[\"$(date +%s)000000000\",\"request completed\"]]}]}"
```

**Responses**

| Status | Body | When |
|---|---|---|
| 204 | empty | Every entry accepted and written to the WAL |
| 400 | error list | Any entry invalid, unsupported content type, or malformed JSON |

---

## Instant query

```http
GET /loki/api/v1/query
```

Evaluates a query at a single point in time.

| Parameter | Required | Default | Notes |
|---|---|---|---|
| `query` | yes | — | LogQL subset; see [limitations.md](limitations.md) |
| `time` | no | now | Unix nanoseconds or RFC3339 |
| `limit` | no | server default | Maximum entries returned |
| `direction` | no | `backward` | `forward` or `backward` |

This endpoint accepts log selectors, metric queries, and constant expressions
such as `vector(1)`. Constant expressions are **only** available here — the range
endpoint rejects them with a message saying so.

**Example**

```bash
curl -sG 'http://localhost:8080/loki/api/v1/query' \
  --data-urlencode 'query={service="api"} |= "timeout"'
```

`data.resultType` is `streams` for log queries and `vector` for metric queries.

---

## Range query

```http
GET /loki/api/v1/query_range
```

Evaluates a query across a window. This is what Grafana's Explore and log panels
call.

| Parameter | Required | Default | Notes |
|---|---|---|---|
| `query` | yes | — | LogQL subset |
| `end` | no | now | Unix nanoseconds or RFC3339 |
| `start` | no | one hour before the anchor | The anchor is `min(end, now)` |
| `since` | no | — | Relative window from the anchor; an explicit `start` wins over it |
| `step` | no | derived | Metric queries only |
| `limit` | no | server default | |
| `direction` | no | `backward` | |

A relative start is anchored to `min(end, now)`: asking for "the last five
minutes" with `end` in the future means the last five minutes of data, not an
all-future window.

**`interval` is rejected with `400`.** Entry sampling is not implemented, and
ignoring the parameter would hand back *more* entries than asked for while
looking like a working filter. Metric queries use `step` instead.

**Example**

```bash
curl -sG 'http://localhost:8080/loki/api/v1/query_range' \
  --data-urlencode 'query=sum by (level) (count_over_time({service="api"}[5m]))' \
  --data-urlencode 'start=1710000000000000000' \
  --data-urlencode 'end=1710003600000000000'
```

**Responses**

| Status | When |
|---|---|
| 200 | `data.resultType` is `streams` for log queries, `matrix` for metric queries |
| 400 | Missing or unparseable `query`, a form outside the subset, `interval` supplied, a constant expression, malformed times, or `end` < `start` |
| 500 | The query failed while running |

---

## Label names

```http
GET /loki/api/v1/labels
```

Returns every stream label name. Grafana calls this to populate the label
browser.

**Example**

```bash
curl -s 'http://localhost:8080/loki/api/v1/labels'
```

---

## Label values

```http
GET /loki/api/v1/label/{name}/values
```

Returns every value observed for one stream label.

**Example**

```bash
curl -s 'http://localhost:8080/loki/api/v1/label/service/values'
```
