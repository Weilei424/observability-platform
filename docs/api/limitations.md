# Limitations

This is the authoritative list of what the platform supports and what it does
not. Other documents summarise it or link to it; none of them restate it, because
two lists drift.

Every `Example` below is a literal expression, and every row is executed against
the real parser by `TestDocumentedQueryFormsMatchTheParser`. A row that disagrees
with the code fails the build.

Unsupported forms return `400` with an explicit message rather than being ignored
or approximated. A query that quietly returns the wrong answer is worse than one
that refuses.

## PromQL subset

| Form | Example | Status |
|---|---|---|
| Bare metric name | `http_requests_total` | Supported |
| Label selector | `http_requests_total{job="api"}` | Supported |
| `rate` over a range | `rate(http_requests_total[5m])` | Supported |
| `sum` | `sum(http_requests_total)` | Supported |
| `sum by` | `sum by (job)(http_requests_total)` | Supported |
| Numeric scalar arithmetic | `1+1` | Supported (returns `scalar`) |
| Any other function | `avg(http_requests_total)` | Returns 400 |
| Histogram functions | `histogram_quantile(0.9, http_request_duration_seconds)` | Returns 400 |
| Metric arithmetic | `http_requests_total + http_errors_total` | Returns 400 |
| Subqueries | `rate(http_requests_total[5m])[10m:1m]` | Returns 400 |

Duration units: `ms`, `s`, `m`, `h`, `d`, `w`, `y`.

Joins, recording rules, and alerting rules are absent entirely — there is no
rule evaluator and no Alertmanager integration, so they are not parsed and then
rejected; the concept does not exist in this backend.

## LogQL subset

| Form | Example | Status |
|---|---|---|
| Stream selector | `{service="api"}` | Supported |
| Multiple label matchers | `{service="api", level="error"}` | Supported |
| Chained line filters | `{service="api"} \|= "timeout" != "healthz"` | Supported |
| Regex line filter | `{service="api"} \|~ "5\\d\\d"` | Supported |
| Negative regex line filter | `{service="api"} !~ "^debug"` | Supported |
| `count_over_time` | `count_over_time({service="api"}[5m])` | Supported |
| `rate` | `rate({service="api"}[5m])` | Supported |
| `bytes_over_time` | `bytes_over_time({service="api"}[5m])` | Supported |
| `bytes_rate` | `bytes_rate({service="api"}[5m])` | Supported |
| `sum by` over a metric query | `sum by (level) (count_over_time({service="api"}[5m]))` | Supported |
| `\| drop` in final position | `{service="api"} \| drop __error__` | Supported (last stage only) |
| Regex label matcher | `{service=~"api\|web"}` | Returns 400 |
| Non-equality label matcher | `{service!="api"}` | Returns 400 |
| JSON parsing pipeline | `{service="api"} \| json` | Returns 400 |
| `unwrap` and its aggregations | `avg_over_time({service="api"} \| unwrap duration [5m])` | Returns 400 |
| Vector aggregations other than `sum` | `topk(5, count_over_time({service="api"}[5m]))` | Returns 400 |
| Binary operations | `sum(count_over_time({service="api"}[5m])) / 2` | Returns 400 |

Label matchers inside `{...}` are **equality-only**; regex applies to log *lines*,
where all four operators (`|=`, `!=`, `|~`, `!~`) work and chain.

`| drop <labels>` is supported only as the final pipeline stage, because Grafana
appends it to every log-volume query. No other pipeline stage or line formatter
is implemented.

Metric queries answer `resultType: "matrix"` on `query_range` and `"vector"` on
the instant endpoint. Range durations accept both the Prometheus grammar (`5m`,
`1d`, `1w`) and Go's (`1.5h`, `150ns`), as upstream LogQL does.

The `offset` modifier and the `interval` parameter are not supported;
`interval` is rejected explicitly rather than ignored, because ignoring it would
return more entries than the caller asked for while looking like a working
filter.

## Platform limits

These are properties of the whole system, not of the query languages.

- **Single node.** There is no ring, no replication, no query fanout, and no
  multi-tenancy. One process owns all storage. Distributed mode is Phase 6 in
  [`../planning/IMPLEMENTATION_PLAN.md`](../planning/IMPLEMENTATION_PLAN.md) and
  is not built.
- **No authentication or authorization.** Every endpoint is open to anyone who
  can reach the port; `internal/api/middleware/` contains request logging and
  metrics and nothing else. Exposing this beyond localhost or a trusted cluster
  network requires a proxy that terminates auth in front of it.
- **No Prometheus `remote_write`.** Ingestion is this project's own JSON API;
  see [metrics.md](metrics.md). A Prometheus server cannot forward to this
  backend without a translator.
- **No recording rules, alerting rules, or Alertmanager.**
- **No downsampling.** Compaction merges blocks and rebuilds their index; it
  never reduces resolution. Storage grows with raw sample count until retention
  deletes whole blocks.
- **Retention is off by default.** `retention` defaults to `0s`, which means keep
  everything forever. Set `OBS_RETENTION` to enable deletion.
- **Grafana credentials come from the environment.** No password is committed
  anywhere; the Helm chart fails to render if you supply neither `admin.password`
  nor `admin.existingSecret`.
- **Log structured metadata is rejected, not dropped.** A Loki push carrying a
  third element per entry fails rather than silently discarding it.

## See also

- [README.md](README.md) — envelopes, time formats, methods
- [metrics.md](metrics.md) — Prometheus-compatible endpoints
- [logs.md](logs.md) — Loki-compatible endpoints
