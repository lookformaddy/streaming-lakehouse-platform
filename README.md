# Streaming Lakehouse Platform

**Kafka-style ingestion → data quality gate → event-time stream processing → lakehouse tables with a transaction log — operated against latency and data-quality SLIs.**

![CI](https://github.com/maddy121312/streaming-lakehouse-platform/actions/workflows/ci.yml/badge.svg)
`topic: data-engineering` · Python 3.11+ · zero core dependencies

---

## The enterprise problem

Every enterprise data platform eventually has to answer the same five
questions about streaming data:

1. **Can we trust what landed?** Feeds break silently — fields disappear,
   types drift, producer clocks skew. Bad data that reaches the lakehouse
   becomes wrong dashboards and wrong decisions.
2. **How fresh is it?** "Real-time" is a claim until end-to-end latency
   (event time → queryable) is measured and held to an SLO.
3. **What happens to late and duplicate data?** At-least-once delivery and
   out-of-order arrivals are the norm, not the exception.
4. **Can we replay and audit?** When logic changes or a feed is fixed,
   the platform must reprocess deterministically and show what the table
   contained at any point in time.
5. **Can it run anywhere?** Pipelines welded to one broker or one cloud
   are migration projects waiting to happen.

This project is a compact, working answer to all five — the patterns a
production platform (Kafka + Flink/Spark + Delta/Iceberg) implements,
distilled into dependency-free Python you can read in an afternoon and
run in ten seconds.

## Architecture at a glance

```
 producers ─▶ transport ─▶ DQ gate ─▶ stream processor ─▶ lakehouse tables
 (synthetic)  (Kafka-       │  └─▶ quarantine table          ├─ orders_raw
               shaped        │                                ├─ orders_by_region_window
               protocols)    └─▶ latency tracker ─▶ SLI report └─ _delta_log/ (tx log)
```

Full detail, decisions and failure-mode analysis: **[docs/architecture.md](docs/architecture.md)**.

## What it demonstrates

| Capability | Where |
|---|---|
| Pluggable transports behind Kafka-shaped protocols (in-process bus, NDJSON file log/replay) | `transport.py` |
| Data quality gate: schema, range, uniqueness, timeliness rules; quarantine with reasons | `quality.py` |
| Event-time tumbling windows, watermarks, late-event handling | `processing.py` |
| Lakehouse tables: Hive-style partitioning, transaction log, snapshot/time-travel reads, partition pruning | `table.py` |
| Latency percentiles + SLI/SLO evaluation per run | `metrics.py` |
| Deterministic synthetic data generator with deliberate corruption to exercise the DQ path | `generator.py` |
| End-to-end pipeline orchestration with an operator-grade run report | `pipeline.py` |
| Optional FastAPI surface (`/health`, `/metrics`, `/run`) | `api.py` |

## Quick start

```bash
pip install -e ".[dev]"

# Run the test suite (36 tests)
python -m pytest -q

# Run the demo: 1000 synthetic orders (3% deliberately corrupt),
# print the full run report, and write a real lakehouse to ./lakehouse-data
python -m lakehouse_platform.cli --events 1000 --out ./lakehouse-data --fresh
```

Sample report (abridged):

```json
{
  "events_read": 1000,
  "events_valid": 979,
  "events_quarantined": 21,
  "windows_emitted": 5,
  "latency": {"count": 979, "p50": 1.51, "p95": 2.85, "p99": 2.97, "max": 3.0},
  "slis": [
    {"name": "e2e_latency_p95_seconds", "value": 2.85, "target": 5.0, "met": true},
    {"name": "data_quality_pass_rate", "value": 0.979, "target": 0.95, "met": true}
  ],
  "healthy": true
}
```

Inspect what landed:

```bash
find lakehouse-data -type f | sort        # data files + _delta_log commits
cat lakehouse-data/orders_raw/_delta_log/00000000000000000000.json
```

Replay a file of events instead of generating them:

```bash
pip install -e ".[api]"
uvicorn lakehouse_platform.api:app --port 8000
curl -X POST localhost:8000/run -H 'content-type: application/json' \
  -d '{"events_file": "examples/sample_events.ndjson"}'
```

Or with Docker: `docker build -t streaming-lakehouse . && docker run --rm streaming-lakehouse`

## Design decisions & trade-offs

* **Protocols over broker clients.** The pipeline depends on two-method
  `EventSource`/`EventSink` protocols, never on Kafka APIs. A real Kafka
  transport slots in without touching pipeline code — and tests need no
  infrastructure. *Trade-off:* broker-specific features (exactly-once
  transactions, consumer-group rebalancing) live in the transport layer
  and must be re-expressed per broker.
* **Fail-closed data quality.** An event lands only if every rule passes;
  failures are quarantined *with machine-readable reasons*, not dropped.
  *Trade-off:* a bad rule can quarantine good data — which is why the DQ
  pass rate is itself an SLI, so the gate is monitored like any service.
* **Transaction log as source of truth.** Commits are immutable data
  files + one log entry; readers resolve the log. This gives atomic
  visibility and time-travel reads. *Trade-off:* metadata grows with
  commit count (production systems checkpoint it; see architecture doc).
* **Event-time windows with watermarks.** Results are deterministic
  under replay; lateness is a tunable, measured quantity (late-event
  SLI), not a surprise. *Trade-off:* allowed-lateness delays every
  window by that bound.
* **NDJSON instead of Parquet.** Zero dependencies and human-inspectable
  output for a reference implementation. The layout, partitioning and log
  semantics are encoding-agnostic — Parquet on object storage is the
  production encoding, not a redesign.

## Repository layout

```
src/lakehouse_platform/
  events.py       Event model (event time vs ingest time, latency)
  transport.py    EventSource/EventSink protocols, in-process bus, file log/replay
  generator.py    Deterministic synthetic order feed (+ controlled corruption)
  quality.py      Data quality rules, reports, quarantine
  processing.py   Tumbling windows, watermarks, late-data handling
  table.py        Lakehouse table: partitions + transaction log + snapshots
  metrics.py      Latency percentiles, SLI/SLO evaluation
  pipeline.py     Orchestration + run report
  cli.py          Demo runner
  api.py          Optional FastAPI surface
tests/            36 pytest tests across all components
docs/             Architecture deep-dive
examples/         Sample NDJSON event file for replay
```

## Roadmap

- [ ] Parquet data files + object-storage backend
- [ ] Compaction job for small files; log checkpointing
- [ ] Schema registry + contract tests for producers
- [ ] Real Kafka transport implementation
- [ ] Change-data-capture (CDC) source connector

## License

MIT
