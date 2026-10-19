# Architecture — Streaming Lakehouse Platform

## Component view

```
                 ┌────────────────────┐
  producers ───▶ │  Transport (Kafka- │ ──▶ micro-batches
                 │  shaped protocols) │
                 └────────────────────┘
                            │
                            ▼
                 ┌────────────────────┐      failures + reasons
                 │  Data Quality      │ ───────────────────────┐
                 │  Engine (gate)     │                        ▼
                 └────────────────────┘              ┌──────────────────┐
                       │ valid events                │ quarantine table │
                       ▼                             └──────────────────┘
        ┌──────────────────────────────┐
        │  Stream Processor            │   event-time tumbling windows,
        │  (watermarks, late handling) │   watermark = max(event_time)
        └──────────────────────────────┘   − allowed_lateness
                       │ window results
                       ▼
   ┌───────────────────────────────────────────────┐
   │  Lakehouse (filesystem here; S3 + Parquet     │
   │  in production)                               │
   │                                               │
   │  orders_raw/                  _delta_log/     │
   │    date=…/hour=…/part-*.ndjson  00000.json …  │
   │  orders_by_region_window/                     │
   │  orders_quarantine/                           │
   └───────────────────────────────────────────────┘
                       │
                       ▼
   LatencyTracker ──▶ SLIEvaluator ──▶ PipelineReport (per run)
```

## Data flow

1. **Ingestion.** Events arrive through an `EventSource`. Every event
   carries both `event_time` (source clock) and `ingest_time` (platform
   clock, stamped by the transport). The difference is the end-to-end
   latency SLI — you cannot measure what you do not stamp.
2. **Quality gate.** Each micro-batch is checked by declarative rules
   (schema, range, uniqueness, timeliness). The gate is fail-closed:
   an event reaches the lakehouse only if every rule passes. Failures go
   to the quarantine table *with their reasons*, so a bad upstream deploy
   is debuggable from data, not from logs.
3. **Processing.** Valid events feed a tumbling-window aggregator.
   Windows close on a watermark, not on wall-clock timers, so results are
   deterministic under replay. Late events are counted and excluded —
   the late-event rate is itself an SLI.
4. **Storage.** Raw events and window aggregates land in lakehouse
   tables. A commit is: write immutable data files, then append one
   transaction-log entry. Readers resolve the log, so a crashed writer
   never leaves a half-visible table — worst case, an orphan file no
   commit references.
5. **Operation.** Each run emits a report: counts, latency percentiles,
   per-SLI pass/fail. The same report shape backs the CLI, the optional
   HTTP API (`/metrics`, `/run`), and CI smoke tests.

## Key decisions

### Why protocols instead of a Kafka client?

`EventSource`/`EventSink` are two-method protocols. The in-process bus
and the NDJSON file transports implement them locally; a Kafka transport
implements the same surface against a real broker. Pipeline, DQ and
processing code never imports a broker client. Benefits: unit tests run
in milliseconds with no infrastructure, demos run anywhere, and broker
migrations (Kafka ↔ Kinesis ↔ Pub/Sub) become a transport project, not a
platform rewrite.

### Why a transaction log instead of "just files in a bucket"?

Directory listings lie: they cannot distinguish a complete commit from a
crashed one, and they give no history. The `_delta_log` makes commits
atomic-by-construction and enables snapshot (time-travel) reads — table
version *N* is exactly the union of commits `0..N`. This is the core
idea Delta Lake and Iceberg productionise with Parquet and richer
metadata; the semantics here are identical, the encoding is JSON/NDJSON
for inspectability.

### Why partition by event time (date/hour)?

Queries and backfills are overwhelmingly time-scoped. Partitioning on
event time gives partition pruning for time-filtered reads and makes
late data land in its correct historical partition instead of polluting
"today". The cost — many small files under high throughput — is the
classic compaction problem, noted as future work below.

### Watermarks and lateness

`watermark = max_event_time_seen − allowed_lateness`. Raising
allowed-lateness improves completeness (fewer late drops) and delays
every result by the same amount. The default (30 s) is a demo value;
production values are set per feed from measured producer delay
distributions — which is why the platform tracks the late-event rate as
an SLI rather than hiding it.

## Failure modes considered

| Failure | Behaviour |
|---|---|
| Malformed / corrupt records | Quarantined with reasons; batch continues |
| Broken producer clock (future/ancient event times) | Timeliness rule quarantines; visible in DQ report |
| Duplicate deliveries (at-least-once) | Uniqueness rule catches in-batch dupes; event IDs are the dedupe key |
| Writer crash mid-commit | Orphan data files possible; no log entry ⇒ invisible to readers |
| Out-of-order events | Handled by event-time windows + watermark; beyond-lateness events counted as late |
| Replay / backfill | File replay source is deterministic; table reads can pin a version |

## Scaling notes (what changes at production scale)

* NDJSON → Parquet data files; filesystem → object storage. The
  transaction-log and partitioning semantics carry over unchanged.
* Single-process windows → a real stream processor (Flink / Spark
  Structured Streaming) with keyed state and checkpointing; the window
  and watermark semantics implemented here are the same contract.
* Add a **compaction** job (small-file problem) and **schema evolution**
  handling in the log (column adds are additive entries).
* DQ rules → a shared contracts registry so producers and consumers agree
  on schemas before deploy, not after quarantine fills up.
