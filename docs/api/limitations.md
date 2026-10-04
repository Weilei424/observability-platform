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

- **One store, one compactor, no replication.** The backend runs all-in-one or
  split into five components
  ([../architecture/components.md](../architecture/components.md)). Writes are
  sharded over a ring of ingesters, but there is one store and one compactor,
  no replication, no parallel query fanout, and no multi-tenancy. Those are
  Phases 6.3-6.5 in
  [`../planning/IMPLEMENTATION_PLAN.md`](../planning/IMPLEMENTATION_PLAN.md).
- **Ring membership is static.** The gateway and querier read the ingester list
  from `OBS_INGESTER_URL` at startup; adding or removing an ingester takes a
  restart of both. There are no heartbeats, join or leave states, or hot reload.
- **A gateway and querier list mismatch hides writes.** If the gateway routes
  to an ingester the querier does not read, those writes are never returned. A
  membership change is therefore staged: the querier gains an ingester before
  the gateway, and the gateway loses it before the querier
  ([../runbooks/split-demo.md](../runbooks/split-demo.md)). Helm enforces the
  order with `split.ingester.writeReplicas`; in Compose the two lists are
  edited by hand, in that order. Both log `ring ready` with `ring=<hash>` of
  the sorted member list at startup: once a change is complete, the two hashes
  must be equal.
- **Ingesters are read one after another.** The querier reads every ingester in
  turn, then the store, so read latency grows with the number of ingesters.
  Parallel fanout is Phase 6.4.
- **One ingester down fails every read and some writes.** A read needs every
  member, so any unreachable ingester answers `503`. A write answers `503` only
  when the batch has a key that ingester owns; other batches succeed. A `503`
  can leave some of a batch's groups written; retrying the whole batch is safe.
- **Removing an ingester can leave data unread if its drain fails.** An
  ingester's graceful stop seals and flushes its whole head into the store. If
  the store is unreachable, the drain fails (logged at ERROR) and the unflushed
  data stays in the ingester's WAL, not read until the ingester rejoins the
  ring. Reads answer `503` while a stopped ingester is still on the querier's
  list.
- **Upgrading to 6.2 has an overwrite window.** A pre-6.2 WAL record replays with a
  fresh generation, so on a restart while pre-6.2 segments are still past the
  checkpoint, a pre-upgrade sample can outrank a post-upgrade overwrite at the same
  series and timestamp. After upgrading, let the head flush (the WAL checkpoint passes
  the pre-6.2 segments) before relying on same-timestamp overwrites across a restart.
- **Last-write-wins across ingesters follows the clock.** Write generations are
  `max(previous + 1, now in Unix microseconds)`. Two writes to one series at
  the same timestamp on two ingesters, within their clock skew, resolve by
  clock, not by arrival order. On one ingester generations are strictly
  increasing, so arrival order wins.
- **Ingesters do not check ownership.** An ingester accepts any write sent to
  it, on its public routes or the internal push routes. A write that skips the
  ring is still read, because every read covers every ingester, but it
  breaks the one-owner-per-series placement the ring promises.
- **Queries, selectors, and label names must be valid UTF-8 and at most
  128 KiB.** A `query`, each `match[]`, or a `{name}` in a label-values path
  longer than 131072 bytes or not valid UTF-8 is refused with `400`
  (`bad_data` on the Prometheus routes), in every topology; so is a LogQL
  label matcher whose escapes (`{job="\xff"}`) decode to invalid UTF-8. Split
  mode carries each label matcher to the ingester and the store inside a
  size-limited JSON request, where an invalid byte would arrive as U+FFFD, and
  all-in-one refuses the same inputs so the two answer alike. Stored labels are
  valid UTF-8, so no refused matcher could have matched anything. Line filters
  run in the querier and never cross that request, so a substring filter such
  as `|= "\xff"` keeps its byte.
- **No authentication or authorization.** Every endpoint is open to anyone who
  can reach the port; `internal/api/middleware/` contains request logging and
  metrics and nothing else. Exposing this beyond localhost or a trusted cluster
  network requires a proxy that terminates auth in front of it.
- **The internal API is unauthenticated too.** The split components talk over
  `/internal/v1` ([internal.md](internal.md)). The gateway never proxies it and
  the Compose split file publishes only the gateway's port, but anything on the
  Compose network or inside the cluster can reach it.
- **No backpressure in the split topology.** While the store is down, the
  ingester keeps accepting writes into its WALs and heads without bound;
  `obs_flush_failures_total`, `obs_log_flush_failures_total`, and
  `obs_wal_bytes` show it.
- **A slow or hanging store stalls a logs flush per batch, not once.** The logs
  flush holds the head lock across every batch it sends, so a store that
  accepts connections but never answers can cost up to the 10 s flush timeout
  on *each* batch of one flush, not a single 10 s cap for the whole thing — a
  large head can be split into several batches. The 30 s backoff only limits
  how often a new threshold flush is attempted after a failure; it does not
  bound one already in flight.
- **There is no server-side query timeout.** A querier request runs under the
  inbound HTTP request's own context, and nothing wraps it with a deadline. A
  hung ingester or store therefore holds that request open until the caller
  (Grafana, or curl) disconnects — it never resolves to a `503` on its own.
- **A store that permanently rejects a batch wedges the head.** A tolerant
  flush never discards a batch the store failed to accept — that would lose
  data already acknowledged to the client — so a store-side bug that keeps
  refusing an otherwise-valid batch retries it forever, once per 30 s backoff,
  while the WAL it cannot checkpoint keeps growing.
- **A hung store can delay the compactor's shutdown by up to about 4
  minutes.** `RunOnce` always runs a compaction pass and then a retention pass
  — `applyRetention` runs unconditionally after `compactToStable`, with no
  check of the shutdown context — and each of `CompactOnce` and
  `ApplyRetention` opens its own fixed 2-minute timeout on a context
  independent of the shutdown signal. Against a store that hangs on every
  request, a maintenance pass in flight when shutdown is requested can cost a
  compaction call's timeout, then a retention call's timeout: up to about 4
  minutes before the compactor's process can exit on its own. In practice a
  SIGKILL usually cuts this short first — the Compose split's compactor
  service has no `stop_grace_period` override (Compose's 10 s default), and
  the Kubernetes split chart sets no `terminationGracePeriodSeconds` for the
  compactor Deployment (Kubernetes' 30 s default) — so this window matters
  mainly for an orchestrator configured with a longer grace period.
- **No all-in-one → split migration.** A split deployment starts from empty data
  directories or ones the split topology wrote; moving an all-in-one data
  directory into an ingester and a store is unsupported.
- **Ingester volume loss resets the generation floor.** The ingester persists
  its generation floor on its own volume. If that volume is lost while the store
  survives, the floor restarts at 1, and later overwrites at already-flushed
  timestamps lose to the older samples the store still holds.
- **Split-mode reads move data.** Every in-range sample and log entry of every
  matching series or stream crosses the network to the querier before it
  filters and caps.
- **No Prometheus `remote_write`.** Ingestion is this project's own JSON API;
  see [metrics.md](metrics.md). A Prometheus server cannot forward to this
  backend without a translator.
- **No recording rules, alerting rules, or Alertmanager.**
- **No downsampling.** Compaction merges blocks and rebuilds their index; it
  never reduces resolution. Storage grows with raw sample count until retention
  deletes whole blocks.
- **Retention is off by default.** `retention` defaults to `0s`, which means keep
  everything forever. Set `OBS_RETENTION` to enable deletion.
- **Grafana credentials differ by runtime, and the Compose demo ships a
  throwaway password.** The Kubernetes path commits none: the Helm chart fails to
  render unless you supply `admin.password` or `admin.existingSecret`. The Compose
  demo does the opposite on purpose — `deployments/docker/docker-compose.yml` sets
  `GF_SECURITY_ADMIN_PASSWORD: admin` so the local walkthrough needs no setup.
- **The Compose demo is reachable only from the host.** Its three published
  ports are written `"127.0.0.1:8080:8080"`, `"127.0.0.1:3000:3000"`, and
  `"127.0.0.1:9090:9090"`, so Docker binds them to loopback rather than
  `0.0.0.0`. Nothing in this repository needs off-host access — the in-container
  clients reach the backend as `backend:8080` over the Compose network, which
  host publishing does not affect — and the three things being published are an
  unauthenticated backend, a Prometheus with no access control, and a Grafana
  whose password is `admin`. Dropping the `127.0.0.1:` prefixes puts all three on
  every network the host can route to; if you need that, treat it as a
  deliberate act and set a real Grafana password first.
- **Log structured metadata is rejected, not dropped.** A Loki push carrying a
  third element per entry fails rather than silently discarding it.

## Durability

Both ingest paths — `POST /api/v1/ingest/metrics` and `POST /loki/api/v1/push` —
append to a write-ahead log before answering `204`, and both share one knob:
`OBS_WAL_SYNC_EVERY_N`, which defaults to `1`.

- **At the default, an acknowledgement is durable.** The active WAL segment is
  fsynced before the response is written, so a `204` survives a host crash or a
  power cut, not merely the process being killed.
- **Above the default, the tail is not.** `OBS_WAL_SYNC_EVERY_N=N` fsyncs once
  per `N` records, so up to `N-1` already-acknowledged records live only in the
  OS page cache. `kill -9`, a panic, or a container restart still loses none of
  them — the page cache outlives the process. A kernel panic, a power loss, or a
  yanked VM can take all of them, and the client was told `204`. That is a
  throughput trade, and it is only ever made deliberately.
- **The exposure is bounded by that tail, not by the log.** Segment rotation
  fsyncs the outgoing segment before sealing it, and `0` is rejected at config
  load rather than silently disabling fsync altogether, so the window is at most
  the `N-1` most recent records of the open segment — never anything older, and
  never a sealed one.

Replay on startup reads every record that reached the disk, so anything inside
the fsync guarantee comes back. `docs/runbooks/local-demo.md` has a runnable
proof of the restart half of this.

## See also

- [README.md](README.md) — envelopes, time formats, methods
- [metrics.md](metrics.md) — Prometheus-compatible endpoints
- [logs.md](logs.md) — Loki-compatible endpoints
