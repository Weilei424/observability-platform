# Grafana Logs Demo

The logs counterpart to [`grafana-demo.md`](grafana-demo.md). Both demos run from the
same backend process on port 8080 — one Go service speaking the Prometheus subset and
the Loki subset at once.

## Prerequisites

- Docker and Docker Compose installed
- Ports 8080 and 3000 free

## Start the stack

```bash
make local-up
```

This starts four services:
- **backend** on port 8080 — the observability backend
- **grafana** on port 3000 — Grafana with provisioned datasources and dashboards
- **load-generator** — continuously posts metrics
- **sample-app** — continuously pushes log streams

## Wait for data

Allow ~15 seconds. The sample app pushes two batches per second across five streams:
`service` is `api` or `worker`, `level` is `info`, `warn`, or `error`, and every stream
carries `env=local`.

Confirm the sample app is actually producing before moving on:

```bash
curl -s localhost:8080/loki/api/v1/label/service/values
```

Expected: the response contains both `api` and `worker`. This matters because the
smoke test below pushes its own `service="smoke-test"` data and passes regardless —
so a green smoke test does not prove the `sample-app` container started. If `api` and
`worker` are missing, the dashboard will be empty even though everything else checks
out.

## Verify the datasource

1. Navigate to `http://localhost:3000` and log in (**admin / admin**)
2. **Connections → Data sources → observability-platform-logs**
3. Scroll down and click **Save & test**
4. Expected: a green success banner

Grafana's Loki health check runs `vector(1)+vector(1)` as an instant query; the backend
answers it from a constant-expression evaluator that reads no stored data.

## Explore workflow

**Explore → observability-platform-logs**, then work down this ladder:

```logql
{service="api"}
{service="api", level="error"}
{service="api"} |= "timeout"
{service="api"} |~ `5\d\d`
{service="api"} != "GET"
{service="worker", level="error"} |= "deadline"
```

Each step should narrow the result. Things to notice:

- Rows are colored by the **`level` stream label** — level is indexed, not parsed out
  of the line, so filtering by it is an index lookup rather than a text scan.
- **Log details** (click a row) shows all three labels including `env`.
- The **label browser** lists `env`, `level`, and `service` from
  `/loki/api/v1/labels`.
- Regex uses backticks — `` `5\d\d` ``. LogQL lexes strings with Go's rules, so a
  double-quoted `"5\d\d"` is a parse error; the double-quoted form must be
  `"5\\d\\d"`.

## Dashboard

**Dashboards → Observability Platform Logs**.

- **Service** and **Level** dropdowns are populated live from
  `/loki/api/v1/label/{name}/values`.
- **Search** is a free-text box feeding `|= "$search"`. Empty means "no filter" —
  `|= ""` matches every line. `$search` is interpolated raw into that string, so it
  only accepts plain substrings — a `"` or a backslash breaks the quoting and
  produces a 400 parse error. For regex or anything with special characters, use
  Explore with backticks (`` |~ `pattern` ``) instead.
- `worker` emits only `info` and `error`, so **Service=worker + Level=warn** is
  legitimately empty. That is a stream that does not exist, not a failure.
- The dropdowns are single-select on purpose. An *All* option would make Grafana emit
  `service=~"api|worker"`, and regex label matchers are unsupported (see below).

## Run the API smoke test

With the stack running:

```bash
make smoke-logs
# or
BACKEND_ADDR=http://localhost:8080 bash tests/e2e/logs_smoke.sh
```

Expected: all checks PASS, exit 0. The script pushes under `service="smoke-test"`, so
that value appears in the **Service** dropdown afterwards — real data, correctly
indexed, not a bug.

## Known limitations

Two different things are going on below, and they produce different HTTP statuses.
Unsupported **query features** — forms the parser recognizes but the engine rejects —
return an explicit `400`, per the project's rule that unsupported PromQL/LogQL always
fails loudly instead of returning a plausible-looking wrong answer. Unimplemented
**endpoints** are simpler: no route is registered for them at all, so the server
answers with a plain `404`. Neither is a stub quietly returning zeros or empty results.

| What you'll see | Why |
|---|---|
| The **log-volume histogram** above Explore's log lines shows an error | Grafana sends `sum by (level) (count_over_time({...}[$__auto]))`. Metric LogQL is not implemented and returns 400 — Phase 4.6. Log lines are unaffected. |
| The **Live** button fails | Live tailing needs a WebSocket at `/loki/api/v1/tail`. No route is registered for it, so the request returns 404. Use dashboard auto-refresh. |
| No **query size estimate** in the query editor | Grafana calls `/loki/api/v1/index/stats`. No route is registered for it, so it returns 404. A stub returning zeros was rejected as a confident lie. |
| **Label browser** narrowing (choosing a label to see its values in context of other selected labels) fails | Grafana's Loki language provider calls `/loki/api/v1/series` to narrow label values. No route is registered for it, so it returns 404. |
| `\| json`, `\| logfmt`, `line_format` return 400 | Log-parsing pipelines are out of the supported subset. |
| `{service=~"api\|web"}` returns 400 | Label matchers are equality-only because they are index-backed. Regex applies to **lines**. |
| Label dropdowns ignore the dashboard time range | The label endpoints accept and ignore `start`/`end`; the stream index is not time-partitioned for label discovery. |

## Stop the stack

```bash
make local-down
```
