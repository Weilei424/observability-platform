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
push routes; the store serves the six over its blocks and log chunks, plus six
routes of its own, the handoff route among them. The
querier, ingester, and compactor call them through `internal/rpc`'s clients,
and a store calls the other stores' flush-in routes during a handoff.

`OBS_STORE_URL` is a comma-separated list on the ingester, querier, and
compactor, normalized and de-duplicated like `OBS_INGESTER_URL`; one URL is a
one-store ring. The ingester sends each flushed series or stream to the
`OBS_STORE_REPLICATION_FACTOR` (R_s) stores the store ring gives its key, so
one flush is several flush-in requests, one per store, each carrying only the
keys that store owns. The querier reads every store, and the compactor drives
each store's block maintenance in turn
([../architecture/components.md](../architecture/components.md)).

## Tenants

Every request a peer makes for a tenant carries `X-Scope-OrgID`; `rpc.Client`
sets it from the request's context on **every** call. The six read routes and
the two push routes **refuse a request without it**: `400` with
`missing tenant`, so a peer that forgot the tenant fails loudly instead of
reading, or writing, the default tenant. The default tenant is sent as
`anonymous`. A push must also carry only that tenant's label sets: a series or
stream whose `__tenant__` names another tenant (or none, under a named header),
or a mix, is `400` before anything is admitted or appended. The flush-in, flush, blocks, compact, retention, drain and handoff
routes take whole label sets or no data at all, and need no tenant.

A tenant's series and streams carry the reserved label `__tenant__=<id>` in
their label set (the default tenant carries none), so the label travels inside
the push, flush-in and handoff bodies and is part of every hash that places a
key. A read answer never contains it: the serving side filters by the request's
tenant and strips the label before answering
([../architecture/components.md](../architecture/components.md#tenants)).

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
| `421` | An ingester or store was addressed as another member (see below) |
| `500` | The component failed while serving the request |

A caller treats a refused connection, a timeout, any `5xx`, or a `421` as the
component being unavailable, which a query answers with `503` `unavailable`.
Any other `4xx` means the two components disagree about this protocol — a bug —
and a query answers `500`.

Every request carries `X-Obs-Member`: the base URL the caller meant to reach,
as the ring lists it. An ingester given its own URL (`OBS_INGESTER_SELF_URL`),
or a store given its own (`OBS_STORE_SELF_URL`), refuses, with `421` `{"error":
"this member is <self>, not <member>"}`, a request whose header names another
member; one without the header passes, so an operator's `curl` needs none. A
connection can reach a different member than its URL names — when containers
restart, Docker's DNS can briefly answer one member's name with an address
another has just taken, and keep-alive then holds that connection — and
without the check that member's one acknowledgement would count twice toward
a write or flush quorum. On a `421` the caller drops its pooled connections,
so its next request dials and resolves afresh. The Compose split file and the
Helm chart set every ingester's and every store's own URL.

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
what it sent once enough stores have answered `200`, and that order is what
keeps a flush invisible to queries
([../architecture/components.md](../architecture/components.md)). One batch of
a head flush goes to several stores at once, each sent the series or streams
it owns; the batch counts as flushed when every key in it has `200`s from
W_s = R_s/2 + 1 of its R_s stores. A store that failed its part while every
key still reached W_s is sent the same body again later, from the ingester's
in-memory retry queue ([limitations.md](limitations.md)).

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

Both flush routes answer `503` `{"error":"store is handing off: it takes no new
flushes"}` once a handoff has closed the store's flush barrier, until the store
restarts ([Handoff](#handoff-store-only)). The ingester's own drain counts
that `503` like any other store outage.

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
from, within that push's one timeout; if that ask fails, so does the push,
and nothing is sent until an ask succeeds — so during a rolling upgrade an
ingester from before 6.3, which refuses a three-element sample, is sent the
two-element form.

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
| `400` | a malformed body, labels the ingester refuses on its second validation, no `X-Scope-OrgID` (`missing tenant`), or a label set of another tenant than the header's |
| `429` | the push would take a tenant past its share of the active series (metrics) or streams (logs) limit; nothing was written to the WAL. The share is ⌈limit × RF ÷ N⌉ |
| `500` | the append failed |
| `503` | the ingester is draining and takes no new writes |


A `429` is the tenant's limit, not an outage: the gateway counts a replica that
answered it as refusing the key, and answers `429` itself when more replicas
refuse a key than the write quorum tolerates
([limitations.md](limitations.md)). The ingester counts the refusal in
`obs_tenant_limit_rejections_total`.

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
accepted to finish. Then it flushes its whole head to the stores — every
metrics chunk, including the open ones a normal flush leaves, and every
buffered log line. All of it, including any wait for a flush already in
progress, happens within 40 seconds. It answers `200` `{"drained":true}` only
when every key reached its store quorum (W_s of its R_s stores) and the head
was empty afterwards, and `503` `{"error":"drain incomplete: …"}` otherwise:
too many of a key's stores unreachable, or the deadline passed. A `200`
therefore covers every write the ingester ever acknowledged. It does not wait
for the retry queue: a store that missed its copy during the drain gets it
from the queue only while this ingester keeps running. Removing an ingester waits for this `200` after the gateway stops
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
| `200` | every write this ingester acknowledged is on a quorum of its stores |
| `503` | the drain did not complete; what is left stays in the WAL, and writes stay refused |

## Handoff — store only

```http
POST /internal/v1/handoff
```

```json
{"stores":["http://store-b:8080","http://store-c:8080"],"replication_factor":2}
```

A leaving store copies everything it holds to the stores that remain. It
handles one leaving store: remove stores one handoff at a time
([../runbooks/split-demo.md](../runbooks/split-demo.md#add-or-remove-a-store)).
`stores` is the target ring, the stores that stay; `replication_factor` is how
many owners each series and stream has on it. The body is checked first, and a
refused body leaves the store taking flushes. `400` for:

- an empty `stores` list;
- a URL that does not normalize to a base URL;
- the same store listed twice;
- a `replication_factor` below 1 or above the number of stores listed;
- a target equal to the store's own URL, when `OBS_STORE_SELF_URL` is set.

Without a self URL the store cannot spot itself: listing the leaving store
among the targets fails later, with `503` rather than `400`, because its own
barrier refuses the copy. The Compose split file and the Helm chart set the
self URL, so there the mistake answers `400`.

Then the barrier closes, waiting for flushes already admitted to finish, and it
stays closed until the store restarts, even when the handoff fails. Both
flush-in routes answer `503` `{"error":"store is handing off: it takes no new
flushes"}`. The store then sends every metrics series, then every log stream,
to every owner on the target ring through their flush-in routes, in batches.
Every owner must acknowledge every batch. Send the deployment's
`OBS_STORE_REPLICATION_FACTOR` as `replication_factor`: the route accepts any
value from 1 to the target count, but a lower one gives keys fewer new owners
and can leave a key with fewer than W_s copies, so reads may be silently
incomplete if one of its stores is down. The whole handoff is bounded by 10
minutes; each batch request by the store's request timeout.

`{"series":1204,"streams":38}` — the distinct series and the streams
acknowledged.

| Status | Meaning |
|---|---|
| `200` | every owner acknowledged every batch |
| `400` | the body was refused; the barrier is still open |
| `503` | the handoff did not complete; the barrier stays closed |

A `503` is `{"series":n,"streams":m,"error":"..."}`: the counts acknowledged
before the error. Retry until `200`: a retry resends everything, and the
duplicates it leaves on the owners collapse on read and at compaction.

## Block maintenance — store only, driven by the compactor

The compactor calls these on each store in `OBS_STORE_URL` in turn; a store
that fails is logged and skipped, and the others are still maintained.

```http
GET /internal/v1/metrics/blocks
```

`{"blocks":[{"id":"8f0c2a91d4e6b357","level":1,"min_time":1758600000000,"max_time":1758607199000,"size_bytes":48213,"tenant":"team-b"}]}` —
what the compactor plans from. `tenant` is the block's tenant and is omitted
for the default tenant (every block from before tenancy): a block holds one
tenant's series, and the compactor plans each tenant's blocks separately, so
compaction never merges two tenants.

```http
POST /internal/v1/metrics/compact
```

`{"groups":[["id-a","id-b"]]}` → `{"compacted":1}`. A group naming a block that
no longer exists is skipped. On failure the answer is a `500` that still carries
the count compacted before the error.

```http
POST /internal/v1/metrics/retention
```

`{"now_ms":1758700000000,"retention_ms":86400000,"overrides_ms":{"team-b":3600000}}` →
`{"deleted":2}`. The clock and windows are the compactor's; the deletion is the
store's. `retention_ms` is the default window and `overrides_ms` the per-tenant
ones, in milliseconds; a window of `0` keeps that tenant's data forever, and a
negative one is refused. A block is deleted when it is older than its own
tenant's window. `overrides_ms` is optional, but a 6.4 store refuses it as an
unknown field, which fixes the upgrade order
([../runbooks/split-demo.md](../runbooks/split-demo.md#upgrading-from-64-to-65)).
A `500` still carries the count.

```http
POST /internal/v1/logs/retention
```

The same body as `/metrics/retention` → `{"deleted":n}`, where `n` counts the
log chunk files deleted. A chunk file is deleted when its newest entry is older
than its stream's tenant window, so the granularity is a whole file; an entry
still in the head is not subject to retention until a flush writes it to a
chunk. A read that races a deletion skips the missing file. A `500` still
carries the count.
