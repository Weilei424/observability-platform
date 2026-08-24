# Grafana Logs Demo

The logs counterpart to [`grafana-demo.md`](grafana-demo.md). Both demos run from the
same backend process on port 8080 — one Go service speaking the Prometheus subset and
the Loki subset at once.

## Prerequisites

- Docker and Docker Compose installed
- Ports 8080 and 3000 free
- Go (for `make smoke-logs`; the demo stack itself needs only Docker)
- `jq` (for `make smoke-compose`, which parses Grafana's dataframe responses)

## Start the stack

```bash
make local-up
```

This starts four services:
- **backend** on port 8080 — the observability backend
- **grafana** on port 3000 — Grafana with provisioned datasources and dashboards
- **load-generator** — continuously posts metrics
- **sample-app** — continuously pushes log streams (and its own metrics; see [`grafana-demo.md`](grafana-demo.md))

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

### Metric queries

The histogram above the log lines is Explore's log-volume panel. Grafana builds its
query from your stream selector and sends it automatically, appending `| drop __error__`
(harmless here — no parser stage runs, so that label is never set):

```logql
sum by (level) (count_over_time({service="api"} | drop __error__[5m]))
count_over_time({service="api"} |= "timeout" [5m])
rate({service="api"}[5m])
bytes_over_time({service="api"}[5m])
```

The first returns one series per level; the others return one per stream. `rate` is
`count_over_time` per second, and the `bytes_` pair measures volume rather than line
count. Switching Explore's query type to **Instant** returns a single point per series
rather than a series over time, with the same numbers a range query reports for the
equivalent tick — every window is `(t - range, t]`, so an entry landing exactly on the
tick counts either way.

## Dashboard

**Dashboards → Observability Platform Logs**.

- **Log volume by level**, at the top, draws stacked bars from
  `sum by (level) (count_over_time({service="$service"} |= "$search" [$__interval]))`.
  It shares the **Service** and **Search** variables with the log panels below it, so
  narrowing either narrows both.
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

## Run the smoke test

With the stack running:

```bash
make smoke-logs
```

Expected: exit 0. This runs two independent halves:

| Half | Command | Needs |
|---|---|---|
| Grafana provisioning files parse and are internally consistent | `go test ./tests/e2e/` | Go only — no backend, no Docker |
| Loki API behaves: push, filters, label discovery, Grafana's datasource health check | `bash tests/e2e/logs_smoke.sh` | a running backend (`BACKEND_ADDR` overrides `localhost:8080`) |

The first half is what catches a datasource pointing at the wrong URL or a variable
that would emit an unsupported regex label matcher — failures that leave every API
check green while Grafana itself shows an error. It runs in plain `go test ./...`
too, so `make test` covers it.

The script half pushes under `service="smoke-test"`, so that value appears in the
**Service** dropdown afterwards — real data, correctly indexed, not a bug.

## Test the whole stack through Grafana

`make smoke-logs` never starts Grafana. To check the demo the way a viewer meets
it — Compose up, Grafana provisioned, panels rendering — run:

```bash
make smoke-compose
```

This one owns its stack: it uses its own Compose project name (`obs-compose-e2e`)
and removes its volumes at the end, so it will not touch a stack you started with
`make local-up`. It does need ports 3000 and 8080 free, and fails immediately with
a clear message if either is taken.

Everything it asserts goes through Grafana's HTTP API rather than the backend's:

- the Loki datasource health check — the **Save & test** button
- the dashboard and both variable dropdowns, through the datasource resource proxy
- both panel expressions through `/api/ds/query`, taken from the dashboard Grafana
  actually serves, so the test cannot drift from the panels
- log chunks reaching disk, which is what the demo's 16 KiB flush override is for
- the data surviving a `docker compose restart backend`
- all four containers still running at the end, and fresh sample-app rows arriving
  after the restart — a producer that dies partway through leaves the label values
  and seeded lines behind, so "it ran once" and "it is running" otherwise look the same

Roughly 2–4 minutes on a warm image cache, longer on the first build. Useful env
overrides: `OBS_COMPOSE_KEEP_UP=1` leaves the stack running for poking around,
and `OBS_COMPOSE_PROJECT` changes the project name.

CI runs this on every push to `main` and every pull request.

## Known limitations

Two different things are going on below, and they produce different HTTP statuses.
Unsupported **query features** — forms the parser recognizes but the engine rejects —
return an explicit `400`, per the project's rule that unsupported PromQL/LogQL always
fails loudly instead of returning a plausible-looking wrong answer. Unimplemented
**endpoints** are simpler: no route is registered for them at all, so the server
answers with a plain `404`. Neither is a stub quietly returning zeros or empty results.

| What you'll see | Why |
|---|---|
| The **Live** button fails | Live tailing needs a WebSocket at `/loki/api/v1/tail`. No route is registered for it, so the request returns 404. Use dashboard auto-refresh. |
| No **query size estimate** in the query editor | Grafana calls `/loki/api/v1/index/stats`. No route is registered for it, so it returns 404. A stub returning zeros was rejected as a confident lie. |
| **Label browser** narrowing (choosing a label to see its values in context of other selected labels) fails | Grafana's Loki language provider calls `/loki/api/v1/series` to narrow label values. No route is registered for it, so it returns 404. |
| `\| json`, `\| logfmt`, `line_format`, `\| unwrap` return 400 | Log-parsing pipelines are out of the supported subset. `\| drop <labels>` is the one exception, because Grafana appends it to every log-volume query. |
| `{service=~"api\|web"}` returns 400 | Label matchers are equality-only because they are index-backed. Regex applies to **lines**. |
| Label dropdowns ignore the dashboard time range | The label endpoints accept and ignore `start`/`end`; the stream index is not time-partitioned for label discovery. |
| `avg_over_time`, `quantile_over_time`, `\| unwrap` return 400 | Label-extraction range aggregations are out of the supported subset. Line-based ones — `count_over_time`, `rate`, `bytes_over_time`, `bytes_rate` — are supported. |
| `sum(...) / sum(...)` returns 400 | Binary operations between metric queries are not implemented. |
| `count(...)`, `topk(...)` return 400 | `sum` is the only supported vector aggregation. |

## Stop the stack

```bash
make local-down
```
