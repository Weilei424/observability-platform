# Architecture

Four views of the same system: what talks to what, how a metric gets to disk, how
a log line gets to disk, and how a query gets answered.

Every node below names a real symbol, file, or route. A diagram of generic boxes
cannot be checked against the tree and ages into decoration; one that names
`FlushBlock` and `streams.index` can be followed into the code.

On-disk detail lives in [storage-layout.md](storage-layout.md). The query forms
these paths accept are in [../api/limitations.md](../api/limitations.md).

## System context

```mermaid
graph LR
  SA[sample-app] -->|"POST /api/v1/ingest/metrics"| B
  SA -->|"POST /loki/api/v1/push"| B
  LG[load-generator] -->|"POST /api/v1/ingest/metrics"| B
  B["backend :8080"] --> D[("data/")]
  P["Prometheus :9090"] -->|"scrape GET /metrics"| B
  G["Grafana :3000"] -->|"obs-prometheus"| B
  G -->|"obs-loki"| B
  G -->|"obs-internals"| P
```

One Go process speaks both the Prometheus subset and the Loki subset, over one
port, backed by two separate storage subtrees.

Grafana has three datasources and two of them are Prometheus-shaped, which is
deliberate. `obs-prometheus` reaches the backend's own TSDB, holding what the
producers push. `obs-internals` reaches a **separate Prometheus** that scrapes the
backend's `/metrics`, holding telemetry *about* the backend. Telemetry that shares
fate and storage with the workload it observes goes blind exactly when that
storage breaks — a WAL problem would corrupt the evidence of the WAL problem.

## Metrics write path

```mermaid
graph TD
  A["POST /api/v1/ingest/metrics"] --> B["handleIngestMetrics<br/>validate name · labels · timestamp_ms · value"]
  B --> C["metrics.NewLabels<br/>normalize + fingerprint"]
  C --> D["WALStore.Append"]
  D --> E["wal.WAL.Append<br/>data/metrics/wal/NNNNNN.wal<br/>fsync every wal_sync_every_n"]
  E --> F["BlockStore head chunks<br/>in memory, per series"]
  F -->|"maintenance loop · flush_interval"| G["WALStore.FlushBlock"]
  G --> H[("data/metrics/blocks/&lt;id&gt;/<br/>meta.json · index · chunks · postings")]
  H --> I["checkpoint written<br/>data/metrics/checkpoint"]
  I --> J["WAL segments below the checkpoint deleted"]
  H -->|"compaction_base_range × multiplier"| K["compactor merge"]
  K -->|"retention"| L["block deletion"]
```

**WAL before buffer** is the durability contract. A sample is acknowledged only
once it is in the write-ahead log, so a `204` means a crash loses nothing the
caller was told had been stored. The in-memory head chunks exist for query speed,
never as the system of record.

**The checkpoint is what makes WAL truncation safe.** Segments are deleted only
after the samples they carry are durable in an immutable block. Without it, the
choice would be between an unbounded WAL and a window where data exists in
neither place.

Compaction merges small blocks into larger ones and rebuilds their index. It never
reduces resolution — there is no downsampling, so storage grows with raw sample
count until retention deletes whole blocks.

## Logs write path

```mermaid
graph TD
  A["POST /loki/api/v1/push"] --> B["handleLokiPush<br/>validate every entry before buffering anything"]
  B --> C["logs.StreamLabels<br/>stream lookup + fingerprint"]
  C --> D["logs WAL append<br/>data/logs/wal/NNNNNN.wal"]
  D --> E["in-memory per-stream buffer"]
  E -->|"logs_flush_threshold_bytes"| F["logs.Store.Flush"]
  F --> G[("data/logs/chunks/&lt;chunk&gt;<br/>compressed · CRC over header and payload")]
  F --> H[("data/logs/index/streams.index")]
```

The shape mirrors the metrics path — fingerprint, WAL, buffer, flush, index — with
one deliberate difference: **validation is all-or-nothing**. Every entry in a push
is checked before anything is buffered, so a request containing one bad line
stores none of them. A partially-applied push can never be mistaken for a complete
one, which matters because the sender has no way to ask which lines survived.

Log chunks are compressed and carry a CRC over both header and payload, so a
truncated or corrupted chunk is detected on read rather than served as plausible
log lines.

## Query path

```mermaid
graph TD
  Q1["GET·POST /api/v1/query · query_range"] --> P1["metrics.ParseExpr"]
  P1 -->|"error"| E1["400 bad_data"]
  P1 --> X1["QueryEngine.EvalInstant · EvalRange"]
  X1 --> S1["head chunks + block readers<br/>postings → series → chunk refs"]
  S1 --> R1["promVectorData · promMatrixData<br/>status · data · warnings"]

  Q2["GET /loki/api/v1/query · query_range"] --> D2{"logs.IsLogExpression"}
  D2 -->|"yes"| P2["logs.ParseLogQL"]
  D2 -->|"no"| P3["logs.ParseMetricQuery"]
  P3 -->|"ErrNotMetricQuery"| P4["logs.ParseScalarQuery<br/>instant endpoint only"]
  P2 --> X2["QueryEngine.QueryRange · QueryInstant"]
  P3 --> X3["QueryEngine.EvalMetricRange · EvalMetricInstant"]
  X2 --> R2["resultType: streams"]
  X3 --> R3["resultType: matrix · vector"]
```

**Parsing is the compatibility boundary.** Everything the platform claims to
support is decided in `metrics.ParseExpr` and in the Loki dispatch, before any
storage is touched. Anything outside the subset fails there with an explicit
`400` — which is why the Loki `interval` parameter is rejected rather than
ignored, and why a structured-metadata log entry is rejected rather than
silently stripped.

The Loki side dispatches on the expression before reading any time parameter, so a
malformed query reports the query error rather than a confusing time error. An
expression opening with `{` is a log selector; anything else is a metric query, or
a constant expression such as `vector(1)` which is answerable only on the instant
endpoint.

Reads merge in-memory head chunks with on-disk blocks, so a query spanning a flush
boundary sees one continuous series rather than a gap.
