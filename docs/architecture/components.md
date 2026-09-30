# Components

The backend runs as one process (`OBS_TARGET=all-in-one`, the default) or as
five (`gateway`, `ingester`, `querier`, `store`, `compactor`). It is one binary
either way; the target decides what a process serves, runs, and owns. This page
is the split topology's reference: what each component does, which data it
owns, how data moves between them, and what happens when one is down.

The all-in-one process is the same components assembled in-process — same
routes, same on-disk layout, same single maintenance loop — so everything in
[README.md](README.md) describes it unchanged.

## Responsibilities and ownership

| Target | Serves | Background work | Owns on disk | Calls |
|---|---|---|---|---|
| `all-in-one` | every public route | flush → compact → retain | the whole data directory | — |
| `gateway` | every public route, proxied | — | — | ingester, querier |
| `ingester` | the two write routes; internal head reads | metrics flush loop; logs threshold flush | `metrics/wal`, `metrics/checkpoint`, `metrics/genfloor`, `logs/wal` | store |
| `querier` | the nine read routes | — | — | ingester, store |
| `store` | internal only: flush-in, reads, block maintenance | — | `metrics/blocks`, `metrics/tmp`, `logs/chunks`, `logs/index` | — |
| `compactor` | — | compact → retain | — | store |

Every durable directory has exactly one owning process, so no volume is shared
between components and no process ever reasons about another mutating a
directory under it. Peers are named by `OBS_INGESTER_URL`, `OBS_STORE_URL`, and
`OBS_QUERIER_URL`; a target refuses to start if one it needs is missing or one
it does not use is set. The internal API is described in
[../api/internal.md](../api/internal.md); it carries no authentication,
matching the public API (see [../api/limitations.md](../api/limitations.md)),
and is reachable in Kubernetes only through ClusterIP Services.

```mermaid
graph LR
  P["producers"] -->|"POST /api/v1/ingest/metrics<br/>POST /loki/api/v1/push"| G["gateway<br/>api.Upstreams"]
  GR["Grafana"] -->|"obs-prometheus · obs-loki"| G
  G -->|"write routes"| I["ingester<br/>metrics.HeadStore · logs.Head"]
  G -->|"read routes"| Q["querier<br/>metrics.Merge · logs.Merge"]
  Q -->|"1. POST /internal/v1/metrics/select<br/>POST /internal/v1/logs/select<br/>GET .../metrics/labels · label-values<br/>GET .../logs/labels · label-values"| I
  Q -->|"2. POST /internal/v1/metrics/select<br/>POST /internal/v1/logs/select<br/>GET .../metrics/labels · label-values<br/>GET .../logs/labels · label-values"| S["store<br/>metrics.BlockStore · logs.ChunkStore"]
  I -->|"POST /internal/v1/metrics/flush<br/>POST /internal/v1/logs/flush"| S
  C["compactor<br/>compactor.Compactor · rpc.BlockManager"] -->|"GET /internal/v1/metrics/blocks<br/>POST /internal/v1/metrics/compact<br/>POST /internal/v1/metrics/retention"| S
```

## The flush

```mermaid
sequenceDiagram
  participant W as WALStore.FlushBlock
  participant H as HeadStore.FlushBlock
  participant S as store: BlockStore.IngestSeriesChunks
  W->>H: flush sealed chunks
  H->>H: write metrics/genfloor (NextGeneration)
  loop each batch of at most 16 MiB of chunk bytes, at most block.MaxChunksPerSeries chunks per series
    H->>S: POST /internal/v1/metrics/flush
    S->>S: write block, validate, register (queryable)
    S-->>H: 200 {"block_id"}
    H->>H: DiscardSealedChunks(batch)
  end
  H-->>W: done
  W->>W: write metrics/checkpoint, delete covered WAL segments
```

The generation floor is written first because last-write-wins between two
samples at one timestamp is decided by write generation, and the blocks that
would otherwise seed the ingester's counter live in another process. With the
floor persisted before anything is sent, no block the ingester ever ships can
outrank a write it accepts after a restart.

A batch also never carries more than `block.MaxChunksPerSeries` chunks for any
one series, split across batches if a series' backlog is larger: `store`'s
block reader (`block.OpenReader`) refuses a block whose index declares more
chunks than that for a series, and it only discovers that after the store has
already written and fsynced it. The store then deletes that rejected block and
answers with an error (`BlockStore.ingestLocked`'s `OpenReader` branch), so no
data is lost — the ingester keeps its chunks and retries — but without the cap
every retry of an over-limit series would fail the same way and that series'
flush would wedge for good. The ingester enforces the cap before sending so
that failure mode never happens.

`BlockStore.IngestSeriesChunks` refuses malformed input — zero series, a
duplicate series ID, a series with no chunks, an empty chunk, or a label set
that does not fingerprint to its claimed ID — with `ErrInvalidSeriesChunks`,
which the store's flush-in handler answers as `400`; any other failure is
`500`.

The logs head (`Head.flushLocked`) flushes similarly — batched requests to the
same sink seam — but differs in three ways:

- it holds its lock from snapshot to reset and drops nothing from memory until
  *every* batch in the flush has been acknowledged, not batch-by-batch like the
  metrics head: `LogWAL.Checkpoint` deletes every segment, so the checkpoint
  (and the head reset) is only correct once the whole flush has landed;
- a failure part-way through therefore leaves the whole head intact, so the
  next attempt resends batches the store already holds — those duplicates are
  absorbed by the `(timestamp, line)` dedup a read applies, the same dedup
  that already covers the crash window between a chunk write and its
  checkpoint;
- it has no generation floor: logs entries never need one, since ordering
  between head and persisted entries is fixed (persisted ahead of head at an
  equal timestamp) rather than resolved by a write generation.

Its batches are sized by estimated wire bytes — the JSON an `IngestStreams`
call would encode, not raw log-line bytes — so a batch of highly-escaped lines
still lands under the store's body limit. In the ingester a failed logs flush
does not fail the push that triggered it — the entry is already in the WAL —
and flushes pause for 30 s.

## Reads, and why a flush is invisible to them

The querier reads through `metrics.Merge(ingester, store)` and
`logs.Merge(ingester, store)`. Each reads its first source to completion, then
its second, never both at once:

1. the store registers a flushed block before it acknowledges the flush;
2. the ingester discards those chunks only after the acknowledgement;
3. the querier finishes reading the ingester before it starts on the store.

```mermaid
sequenceDiagram
  participant GR as Grafana
  participant Q as querier: metrics.QueryEngine · logs.QueryEngine
  participant I as ingester: HeadStore.Select · logs.AsSource(Head)
  participant S as store: BlockStore.Select · logs.AsSource(ChunkStore)
  GR->>Q: GET·POST /api/v1/query_range · GET /loki/api/v1/query_range
  Q->>I: 1. POST /internal/v1/metrics/select (or /logs/select)
  I-->>Q: head series/streams
  Q->>S: 2. POST /internal/v1/metrics/select (or /logs/select)
  S-->>Q: block/chunk series/streams
  Q->>Q: sortAndDedup (metrics) · mergeEntries (logs)
  Q-->>GR: promVectorData/promMatrixData · resultType streams
```

A sample missing from the ingester's answer was therefore discarded before that
read began, so the store had it before its read began. Overlap between the two
answers is resolved per signal, by the same rule the equivalent all-in-one path
already applies between its own head and persisted data — one rule per signal,
not two, shared by all-in-one and split:

- metric samples merge by `(timestamp, highest generation)` through
  `sortAndDedup` — the function `metrics.Merge` calls and `BlockStore` itself
  uses between its head and its blocks;
- log entries dedup by `(timestamp, line)`, persisted ahead of head at an equal
  timestamp, through `mergeEntries` — the function `logs.Merge` calls and the
  one `logs.Store.StreamEntries` uses between its head and its chunk store in
  all-in-one.

## Compaction

The compactor owns the policy — planning with `compactor.Plan`, the cadence,
the retention clock, and the maintenance metrics. The store owns the mechanism:
`rpc.BlockManager.CompactOnce` lists the store's blocks, plans locally, and
posts the chosen groups; the store re-checks that each group's blocks still
exist and runs `BlockStore.CompactOnce` with every integrity check it has
always run. That runs under `flushMu` end to end — the same lock
`IngestSeriesChunks` takes, so a flush and a compaction never race on the block
set — and takes the readers' `bs.mu` only briefly, for the swap that installs
the merged block and retires its sources; every read against `bs.mu` sees
either the pre-compaction or the post-compaction set, never a partial one.

```mermaid
sequenceDiagram
  participant C as compactor.Compactor
  participant B as rpc.BlockManager
  participant S as store: BlockStore.CompactOnce
  C->>B: CompactOnce(compactor.Plan)
  B->>S: GET /internal/v1/metrics/blocks
  S-->>B: block list
  B->>B: compactor.Plan(blocks)
  B->>S: POST /internal/v1/metrics/compact {groups}
  S->>S: re-check each group's blocks still exist
  S->>S: BlockStore.CompactOnce (flushMu end to end; bs.mu only for the swap)
  S-->>B: 200 {"compacted": n}
  C->>B: ApplyRetention(now, retention)
  B->>S: POST /internal/v1/metrics/retention
  S-->>B: 200 {"deleted": n}
```

## When a component is down

| Down | Writes | Reads and metadata | Background work |
|---|---|---|---|
| store | accepted: WAL + head; the head grows | `503` `unavailable` | flushes and compaction passes fail, are counted, and retry |
| ingester | `503` at the gateway | `503` at the querier | — |
| querier | unaffected | `503` at the gateway | — |
| compactor | unaffected | unaffected | no compaction or retention until it returns |

Reads fail closed: a query never answers from one source while the other is
down, because a partial answer would look complete. Readiness reports only a
process's own state — its data directory, for the ingester and store — so one
component's outage never marks its dependents unready.

A peer outage or a deadline (`rpc.ErrUnavailable`, including a wrapped
`context.DeadlineExceeded`) answers `503`; a caller that leaves before a query
finishes answers `499` `canceled` instead — Grafana abandoning a dashboard
refresh or zoom is not a server error, and counting it as one would inflate the
self-observability dashboard's error rate for something that never failed. The
gateway applies the same rule to a client that disconnects mid-proxy: `499`,
not `503`.

## Observability

Each component exports only the metrics for work it does. Every scrape target
carries a `component` label; the internals dashboard counts HTTP only at the
edge (`all-in-one` or `gateway`) and plots `up` by component. Every log line
carries `target=<mode>`, and one `X-Request-Id` follows a request from the
gateway through the querier to the ingester and store.

## Deploying it

- Compose: [../runbooks/split-demo.md](../runbooks/split-demo.md), from
  `deployments/docker/docker-compose.split.yml`. The ingester and store get a
  60 s `stop_grace_period` there, long enough for a graceful stop to finish an
  in-flight flush instead of being killed mid-write.
- Kubernetes: the backend chart's `topology: split`; see
  [../../deployments/helm/README.md](../../deployments/helm/README.md). The
  ingester and store StatefulSets set `terminationGracePeriodSeconds: 60` for
  the same reason.
