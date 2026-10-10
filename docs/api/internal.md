# Internal API

The split components talk to one another over HTTP under `/internal/v1`, on each
component's own port. This is not a public API:

- The gateway never proxies it — `/internal/*` is not in its route table.
- The Compose split file publishes no port but the gateway's, so it is reachable
  only on the Compose network; in Kubernetes only through ClusterIP Services.
- It is unauthenticated, like the public API; see [limitations.md](limitations.md).
- It carries no cross-version promise. A split deployment runs one binary
  version, and a protocol break becomes `/internal/v2`.

The ingester serves the six read routes over its in-memory heads, plus the two
push routes; the store serves the six over its blocks and log chunks, plus five
routes of its own. The
querier, ingester, and compactor call them through `internal/rpc`'s clients.

## Encoding

Request and response bodies are JSON. Timestamps and write generations are
integers. **Sample values are strings**, formatted as Go's
`strconv.FormatFloat(v, 'g', -1, 64)` formats them — ingest accepts `NaN`,
`+Inf`, and `-Inf`, which JSON numbers cannot carry. A sample is
`[timestamp_ms, "value", generation]`; a log entry is `[timestamp_ns, "line"]`.
Flushed chunks travel in their persisted encoding — base64 of the chunk's own
bytes — so the store validates them exactly as it validates a chunk read from
disk.

Unknown request fields, and any data left over after the JSON value, are both
refused with `400`: both sides of this API ship in one binary, so either one
is a version mismatch, not something to ignore. A flush body is also refused
with `400` if it is not valid UTF-8, rather than let `encoding/json` silently
replace an invalid byte sequence with `U+FFFD` and store a line that is not
the one sent.

Errors are `{"error": "..."}`:

| Status | Meaning |
|---|---|
| `400` | A malformed or invalid request |
| `413` | A flush body over 64 MiB, or any other body over 1 MiB. The public API caps a selector at 128 KiB, which JSON escaping cannot grow past 768 KiB, so a query never meets this limit |
| `500` | The component failed while serving the request |

A caller treats a refused connection, a timeout, or any `5xx` as the component
being unavailable, which a query answers with `503` `unavailable`. A `4xx`
means the two components disagree about this protocol — a bug — and a query
answers `500`.

## Reads — ingester and store

```http
POST /internal/v1/metrics/select
```

```json
{"matchers":[{"name":"__name__","value":"http_requests_total"}],
 "min_ms":1758600000000,"max_ms":1758603600000,"anchor":true}
```

Every series matching the matchers, with its samples in `[min_ms, max_ms]` (one
per timestamp, the highest generation) and, with `anchor`, its latest sample
before `min_ms`. `series_only` returns labels alone — series with a sample in
the range, or with `any_time` every series the index knows. `any_time` needs
`series_only`; `anchor` excludes it. An inverted range (`max_ms` before
`min_ms`) answers an empty result, not an error.

```json
{"series":[{"labels":{"__name__":"http_requests_total","method":"GET"},
            "anchor":[1758599995000,"41",17],
            "samples":[[1758600000000,"42",18]]}]}
```

```http
GET /internal/v1/metrics/labels
```

`{"names":[...]}` — every label name.

```http
GET /internal/v1/metrics/label-values
```

`?name=<label>`; `{"values":[...]}`. The name travels as a query parameter, not
a path segment, so that any label name arrives intact.

```http
POST /internal/v1/logs/select
```

```json
{"matchers":[{"name":"service","value":"api"}],
 "min_ns":1758600000000000000,"max_ns":1758603600000000000}
```

Every stream matching the matchers that holds an entry in `[min_ns, max_ns]`,
ascending by stream, entries ascending by timestamp and deduplicated by
`(timestamp, line)`:

```json
{"streams":[{"labels":{"service":"api","level":"error"},
             "entries":[[1758600000000000000,"GET /api/v1/query 503 in 9ms"]]}]}
```

```http
GET /internal/v1/logs/labels
```

```http
GET /internal/v1/logs/label-values
```

As their metrics counterparts, over log streams.

## Flush-in — store only

A flush answers only once the data is durable and readable: the ingester drops
what it sent when it receives the `200`, and that order is what keeps a flush
invisible to queries ([../architecture/components.md](../architecture/components.md)).

```http
POST /internal/v1/metrics/flush
```

Sealed chunks in their persisted encoding, base64 in JSON. The store validates
each exactly as it validates a chunk read from disk, then writes, validates, and
registers one block. A series repeated within the same flush, or a chunk the
store's decoder rejects, answers `400`; a storage failure once validation has
passed answers `500`.

```json
{"series":[{"labels":{"__name__":"http_requests_total","method":"GET"},
            "chunks":["<base64 of the chunk's persisted bytes>"]}]}
```

`{"block_id":"8f0c2a91d4e6b357","series":1,"samples":120}`

```http
POST /internal/v1/logs/flush
```

Head streams, entries in the order they were appended. Invalid UTF-8 is refused
rather than silently replaced.

`{"streams":[{"labels":{"service":"api"},"entries":[[1758600000000000000,"line"]]}]}` →
`{"streams":1,"entries":1}`

## Push — ingester only

The gateway validates a write itself, then sends each ingester the part the
ring assigns it ([../architecture/components.md](../architecture/components.md)).
The gateway is the only caller. The bodies are in the internal codec: strict
decoding, label values as written, sample values as strings.

```http
POST /internal/v1/metrics/push
```

`{"series":[{"labels":{"__name__":"http_requests_total"},"samples":[[1758600000000,"1"]]}]}` —
each sample is `[timestamp_ms, "value", generation]`: the write generation the
gateway stamped when it admitted the batch, which every replica stores exactly.
A sample without one, `[timestamp_ms, "value"]`, has the ingester assign its
own; a generation must be positive. The ingester's answer carries
`X-Obs-Push-Generations: 1` when it takes generations. The gateway sends
generations only to an ingester that has said so — it asks each one with an
empty push at start, and again before the first push to one it has not heard
from — so during a rolling upgrade an ingester from before 6.3, which refuses
a three-element sample, is sent the two-element form.

```http
POST /internal/v1/logs/push
```

`{"streams":[{"labels":{"service":"api"},"entries":[[1758600000000000000,"line"]]}]}` —
each entry is `[timestamp_ns, "line"]`.

Both answer `204` with no body once every sample or entry is in the WAL and the
head. The body is capped at 64 MiB, the flush cap: the public bodies are 1 MiB
(metrics) and 4 MiB (logs) and re-encoding can only grow one by a small factor,
so the cap never stops a valid push. A body over it is `413`.

| Status | Meaning |
|---|---|
| `204` | accepted |
| `400` | a malformed body, or labels the ingester refuses on its second validation |
| `500` | the append failed |
| `503` | the ingester is draining and takes no new writes |


The ingester counts what it ingests, and counts only an append failure as a
rejection (reason `append`); the gateway already counted any validation
rejection. It does not check that it owns the keys. A `4xx` is a protocol bug and the
gateway answers `500` for it; a transport error, a deadline, or a `5xx` is the
ingester being unavailable and the gateway answers `503`.

## Drain — ingester only

```http
POST /internal/v1/drain
```

First a write barrier: the ingester stops taking writes, on every write route
— public and internal, metrics and logs — and waits for the ones already
accepted to finish. Then it flushes its whole head into the store — every
metrics chunk, including the open ones a normal flush leaves, and every
buffered log line. All of it, including any wait for a flush already in
progress, happens within 40 seconds. It answers `200` `{"drained":true}` only
when everything reached the store and the head was empty afterwards, and `503`
`{"error":"drain incomplete: …"}` otherwise: the store unreachable or the
deadline passed. A `200` therefore covers every write the ingester ever
acknowledged. Removing an ingester waits for this `200` after the gateway stops
writing to it and before the querier stops reading it
([../runbooks/split-demo.md](../runbooks/split-demo.md)). An ingester also runs
one such drain as the last step of a graceful stop, within the process's
shutdown budget.

The barrier is permanent until the ingester restarts, even when the drain
answers `503`: retry the drain until it answers `200`. Every write after it is
refused with `503` `{"error":"ingester is draining: it takes no new writes"}`,
which the gateway reports as that ingester being unavailable. Draining an
ingester the gateway still writes to therefore fails the writes it owns until
it restarts.

| Status | Meaning |
|---|---|
| `200` | every write this ingester acknowledged is in the store |
| `503` | the drain did not complete; what is left stays in the WAL, and writes stay refused |

## Block maintenance — store only, driven by the compactor

```http
GET /internal/v1/metrics/blocks
```

`{"blocks":[{"id":"8f0c2a91d4e6b357","level":1,"min_time":1758600000000,"max_time":1758607199000,"size_bytes":48213}]}` —
what the compactor plans from.

```http
POST /internal/v1/metrics/compact
```

`{"groups":[["id-a","id-b"]]}` → `{"compacted":1}`. A group naming a block that
no longer exists is skipped. On failure the answer is a `500` that still carries
the count compacted before the error.

```http
POST /internal/v1/metrics/retention
```

`{"now_ms":1758700000000,"retention_ms":86400000}` → `{"deleted":2}`. The clock
and window are the compactor's; the deletion is the store's. A `500` still
carries the count.
