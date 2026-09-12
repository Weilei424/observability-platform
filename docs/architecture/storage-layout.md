# Storage Layout

Everything the backend persists lives under `OBS_DATA_DIR` (default `data/`).
Metrics and logs are separate subtrees with separate write-ahead logs, so a
problem in one cannot corrupt the other.

This document describes what the code **actually writes**, not what it was
planned to write. `TestStorageLayoutDocMatchesDisk` drives the real ingest and
flush paths into a temporary directory, walks the result, and fails if the tree
and the table below disagree in either direction — a path produced but not
documented, or documented but not produced.

Diagrams of the paths that create these files are in [README.md](README.md).

## Layout

Variable segments are written `<like-this>`. Directory rows end in `/`.

| Path | Written by | Contents |
|---|---|---|
| `data/metrics/` | `metrics.NewBlockStore` | Everything belonging to the metrics TSDB |
| `data/metrics/wal/` | `wal.Open` | Metrics write-ahead log |
| `data/metrics/wal/<segment>.wal` | `wal.WAL.Append` | Length-prefixed sample records; segments roll at `wal_segment_max_bytes` |
| `data/metrics/checkpoint` | `metrics.WALStore.FlushBlock` | The WAL segment index whose samples are now durable in a block |
| `data/metrics/blocks/` | `metrics.NewBlockStore` | Immutable time blocks |
| `data/metrics/blocks/<block-id>/` | `block.Writer` | One block: a closed time range of samples |
| `data/metrics/blocks/<block-id>/meta.json` | `block.Meta.Write` | Block metadata — see below |
| `data/metrics/blocks/<block-id>/chunks` | `block.Writer` | Encoded sample chunks, concatenated |
| `data/metrics/blocks/<block-id>/index` | `block.Writer` | Series label sets and their chunk references |
| `data/metrics/blocks/<block-id>/postings` | `block.Writer` | Label pair → series ID lists |
| `data/metrics/tmp/` | `metrics.NewBlockStore` | Scratch space for blocks being written or deleted; emptied at startup |
| `data/logs/` | `logs.NewStore` | Everything belonging to the log store |
| `data/logs/wal/` | `logs.NewStore` | Logs write-ahead log, independent of the metrics WAL |
| `data/logs/wal/<segment>.wal` | `logwal` | Length-prefixed log entry records |
| `data/logs/chunks/` | `logs.NewStore` | Compressed log chunks |
| `data/logs/chunks/<stream>-<min-ts>-<rand>.chunk` | `logs.Store.Flush` | One flushed chunk for one stream |
| `data/logs/index/` | `logs.NewStore` | Stream index |
| `data/logs/index/streams.index` | `logs.Store.Flush` | Label pair → stream IDs, and stream ID → chunk references |

A block ID and a log chunk name are both content-derived, so neither depends on
wall-clock ordering or a counter that a restart could reuse.

## File formats

### Metrics WAL segment

Each record is a 4-byte big-endian body length followed by the body: a record
type byte, a label count byte, each label as (1-byte name length, name, 2-byte
value length, value), then an 8-byte millisecond timestamp and an 8-byte float64
value.

There is **no per-record checksum**. Durability comes from the length prefix plus
an explicit repair policy: on replay, a torn trailing record in the final segment
is tolerated by truncating the file back to the end of the last complete record,
and that truncation is fsynced before a newer segment is opened. Without the
fsync, a crash could lose the repair while keeping the newer segment, turning a
torn tail into a permanent startup failure. A torn record anywhere other than the
final segment's tail is an error, not a repair — it means something other than a
crash-at-write happened.

### Block `meta.json`

```json
{
  "block_id": "62fda5d6d0afea5e",
  "min_time": 1710000000000,
  "max_time": 1710003600000,
  "num_series": 1,
  "num_samples": 120,
  "created_at": "2026-09-12T00:00:00Z",
  "level": 1,
  "sources": [],
  "max_gen": 120
}
```

`level` is the compaction level: 1 is a freshly flushed head block, higher values
are the result of merges. `sources` lists the block IDs merged into a compacted
block. `max_gen` is the highest per-sample write generation in the block; at
startup it seeds the in-memory generation counter so replayed and new writes
always outrank persisted data under last-write-wins.

### Block `index`, `postings`, `chunks`

`chunks` holds encoded sample chunks concatenated into one file, addressed by
offset. `index` maps each series' label set to its chunk references. `postings`
maps a label pair to the series IDs carrying it, which is what makes a selector
an index lookup rather than a scan.

### Log chunk file

A 41-byte header followed by a DEFLATE-compressed entry block:

```text
magic(4) · version(1) · minTs(8) · maxTs(8) · numEntries(4)
uncompressedLen(4) · compressedLen(4) · headerCRC(4) · payloadCRC(4)
```

Both CRCs are Castagnoli. `headerCRC` covers the first 33 bytes, so timestamp
bounds and counts are **authenticated by a header-only read** — the index can
trust a chunk's range without decompressing it. `payloadCRC` covers the
compressed body.

Version 1 chunks (which lacked the header CRC) are rejected with a version error
rather than decoded, matching the metrics chunk format's policy of refusing
superseded layouts instead of carrying multi-version decoders.

A chunk's uncompressed size is capped at 128 MiB, which bounds the buffer a
reader will allocate from a length field that corruption could control. The store
splits large flushes to stay under it, so a writer can never produce a chunk that
no reader will open.

## Lifecycle

1. **Append.** A sample or log line is written to its WAL and only then
   acknowledged. The in-memory head is for query speed; the WAL is the system of
   record.
2. **Flush.** The maintenance loop turns head chunks into an immutable block
   (metrics) or writes a compressed chunk and updates the stream index (logs).
   Both use a temp file, fsync, atomic rename, and a directory fsync, so a
   half-written block or chunk is never visible.
3. **Checkpoint.** After a metrics block lands, the checkpoint records which WAL
   segments are now redundant, and those segments are deleted. This is what makes
   WAL truncation safe — without it the choice would be between an unbounded WAL
   and a window where data is durable in neither place.
4. **Compact.** Small blocks are merged into larger ones and their index is
   rebuilt. Compaction never reduces resolution; there is no downsampling.
5. **Retain.** Blocks entirely older than `retention` are deleted whole.
   Retention is off by default (`0s` means keep everything).

## What this document does not cover

`TestStorageLayoutDocMatchesDisk` exercises metrics ingest, block flush, log
append, and log chunk flush. It does **not** exercise compaction or retention, so
the files those paths produce and delete are described here from the code rather
than verified against a produced tree. Compaction writes a new block directory of
exactly the shape documented above and removes its sources; retention removes
block directories. If either grows a new file type, this table will not catch it —
extend the test first.
