"""Pipeline orchestration: source → DQ gate → lakehouse + windows → SLI.

The pipeline is deliberately boring orchestration around the four real
components, which is exactly what production streaming jobs look like:

1. **Poll** a micro-batch from the source transport.
2. **Gate** it through the data-quality engine. Valid events continue;
   invalid ones are written to a quarantine table with their failure
   reasons (never silently dropped).
3. **Land** valid events in the raw lakehouse table, and feed them to the
   window processor; completed windows land in an aggregates table.
4. **Measure**: record end-to-end latency for every event and evaluate
   the run's SLIs at the end.

Every run returns a :class:`PipelineReport` — the same numbers an
operator would see on a dashboard.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .events import Event
from .metrics import LatencyTracker, SLIEvaluator, SLIResult
from .processing import TumblingWindowProcessor
from .quality import DataQualityEngine
from .table import LakehouseTable
from .transport import EventSource


@dataclass
class PipelineReport:
    events_read: int = 0
    events_valid: int = 0
    events_quarantined: int = 0
    windows_emitted: int = 0
    raw_table_version: int = -1
    agg_table_version: int = -1
    latency: dict[str, Any] = field(default_factory=dict)
    slis: list[SLIResult] = field(default_factory=list)

    @property
    def healthy(self) -> bool:
        return all(s.met for s in self.slis)

    def summary(self) -> dict[str, Any]:
        return {
            "events_read": self.events_read,
            "events_valid": self.events_valid,
            "events_quarantined": self.events_quarantined,
            "windows_emitted": self.windows_emitted,
            "raw_table_version": self.raw_table_version,
            "agg_table_version": self.agg_table_version,
            "latency": self.latency,
            "slis": [s.to_dict() for s in self.slis],
            "healthy": self.healthy,
        }


def _event_record(event: Event) -> dict[str, Any]:
    """Flatten an event into a raw-table record (payload columns promoted)."""
    record: dict[str, Any] = {
        "event_id": event.event_id,
        "event_type": event.event_type,
        "key": event.key,
        "event_time": event.event_time.isoformat(),
        "ingest_time": event.ingest_time.isoformat() if event.ingest_time else None,
    }
    record.update(event.payload)
    return record


class StreamingPipeline:
    """Run the streaming lakehouse pipeline over a source transport."""

    def __init__(
        self,
        source: EventSource,
        lakehouse_root: str | Path,
        topic: str = "orders",
        processor: TumblingWindowProcessor | None = None,
        dq_engine: DataQualityEngine | None = None,
        sli_evaluator: SLIEvaluator | None = None,
        batch_size: int = 500,
    ) -> None:
        self.source = source
        self.topic = topic
        self.batch_size = batch_size
        self.processor = processor or TumblingWindowProcessor()
        self.dq_engine = dq_engine or DataQualityEngine()
        self.sli_evaluator = sli_evaluator or SLIEvaluator()
        self.latency = LatencyTracker()
        self.raw_table = LakehouseTable(lakehouse_root, "orders_raw")
        self.agg_table = LakehouseTable(lakehouse_root, "orders_by_region_window")
        self.quarantine_table = LakehouseTable(lakehouse_root, "orders_quarantine")

    def _run_batch(self, events: list[Event], report: PipelineReport) -> None:
        report.events_read += len(events)
        quality = self.dq_engine.check(events)
        report.events_valid += quality.valid_count
        report.events_quarantined += quality.quarantined_count

        for event in quality.valid:
            latency = event.latency_seconds
            if latency is not None and latency >= 0:
                self.latency.record(latency)

        if quality.valid:
            self.raw_table.append(_event_record(e) for e in quality.valid)

        if quality.quarantined:
            self.quarantine_table.append(
                {
                    **_event_record(event),
                    "dq_failures": "; ".join(reasons),
                }
                for event, reasons in quality.quarantined
            )

        window_records = []
        for event in quality.valid:
            window_records.extend(r.to_record() for r in self.processor.add(event))
        if window_records:
            self.agg_table.append(window_records)
            report.windows_emitted += len(window_records)

    def run(self, max_events: int | None = None) -> PipelineReport:
        """Consume until the source is exhausted (or ``max_events`` hit)."""
        report = PipelineReport()
        read = 0
        while max_events is None or read < max_events:
            want = self.batch_size if max_events is None else min(self.batch_size, max_events - read)
            batch = self.source.poll(self.topic, max_records=want)
            if len(batch) == 0:
                break
            read += len(batch)
            self._run_batch(list(batch), report)

        # End of stream: flush any open windows.
        tail = [r.to_record() for r in self.processor.flush()]
        if tail:
            self.agg_table.append(tail)
            report.windows_emitted += len(tail)

        report.raw_table_version = self.raw_table.version
        report.agg_table_version = self.agg_table.version
        report.latency = self.latency.summary()
        processed = self.processor.stats.processed
        report.slis = self.sli_evaluator.evaluate(
            latency=self.latency,
            dq_pass_rate=(
                report.events_valid / report.events_read if report.events_read else 1.0
            ),
            late_events=self.processor.stats.late,
            processed_events=processed,
        )
        return report
