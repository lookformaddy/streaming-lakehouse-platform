"""Stream processing: event-time tumbling windows with watermarks.

The processor aggregates events into fixed-size windows keyed by the
event's partition key (e.g. region). Production streaming systems
(Flink, Spark Structured Streaming, Kafka Streams) all answer the same
three questions, and this module answers them the same way:

* **When is a window complete?** When the *watermark* — the highest event
  time seen minus an allowed-lateness bound — passes the window's end.
* **What happens to late events?** Events arriving after the watermark has
  passed their window are counted as late and diverted to a late-data
  counter instead of corrupting already-emitted results.
* **What does a result look like?** One aggregate record per
  (window, key): count, sum, average and min/max of a numeric measure.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from .events import Event


@dataclass(frozen=True)
class WindowResult:
    """One completed (window, key) aggregate."""

    window_start: datetime
    window_end: datetime
    key: str
    count: int
    total: float
    minimum: float
    maximum: float

    @property
    def average(self) -> float:
        return self.total / self.count if self.count else 0.0

    def to_record(self) -> dict:
        """Flatten to a lakehouse-ready record."""
        return {
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "key": self.key,
            "event_count": self.count,
            "total_amount": round(self.total, 2),
            "avg_amount": round(self.average, 2),
            "min_amount": self.minimum,
            "max_amount": self.maximum,
            # Partition columns for the lakehouse writer.
            "event_time": self.window_start.isoformat(),
        }


@dataclass
class _Bucket:
    count: int = 0
    total: float = 0.0
    minimum: float = float("inf")
    maximum: float = float("-inf")

    def add(self, value: float) -> None:
        self.count += 1
        self.total += value
        self.minimum = min(self.minimum, value)
        self.maximum = max(self.maximum, value)


@dataclass
class ProcessingStats:
    processed: int = 0
    late: int = 0
    windows_emitted: int = 0


class TumblingWindowProcessor:
    """Aggregate a numeric payload field into event-time tumbling windows.

    Args:
        window_size: Length of each window (e.g. ``timedelta(minutes=1)``).
        measure: Payload field to aggregate (must be numeric).
        allowed_lateness: How far behind the maximum event time the
            watermark trails. Larger values tolerate more out-of-order
            data at the cost of later results.
    """

    def __init__(
        self,
        window_size: timedelta = timedelta(minutes=1),
        measure: str = "amount",
        allowed_lateness: timedelta = timedelta(seconds=30),
    ) -> None:
        if window_size.total_seconds() <= 0:
            raise ValueError("window_size must be positive")
        self.window_size = window_size
        self.measure = measure
        self.allowed_lateness = allowed_lateness
        self.stats = ProcessingStats()
        self._buckets: dict[tuple[datetime, str], _Bucket] = {}
        self._max_event_time: datetime | None = None

    # -- internals ---------------------------------------------------------
    def _window_start(self, ts: datetime) -> datetime:
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        seconds = (ts - epoch).total_seconds()
        size = self.window_size.total_seconds()
        start = (seconds // size) * size
        return epoch + timedelta(seconds=start)

    @property
    def watermark(self) -> datetime | None:
        if self._max_event_time is None:
            return None
        return self._max_event_time - self.allowed_lateness

    # -- public API --------------------------------------------------------
    def add(self, event: Event) -> list[WindowResult]:
        """Add one event; returns any windows completed by this event."""
        value = event.payload.get(self.measure)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return []  # unmeasurable events are the DQ layer's concern
        self.stats.processed += 1
        if self._max_event_time is None or event.event_time > self._max_event_time:
            self._max_event_time = event.event_time

        start = self._window_start(event.event_time)
        end = start + self.window_size
        watermark = self.watermark
        if watermark is not None and end <= watermark:
            self.stats.late += 1
            return self._emit_ready()

        bucket = self._buckets.setdefault((start, event.key), _Bucket())
        bucket.add(float(value))
        return self._emit_ready()

    def _emit_ready(self) -> list[WindowResult]:
        watermark = self.watermark
        if watermark is None:
            return []
        ready = [
            (start, key)
            for (start, key) in self._buckets
            if start + self.window_size <= watermark
        ]
        return self._emit(ready)

    def _emit(self, keys: list[tuple[datetime, str]]) -> list[WindowResult]:
        results: list[WindowResult] = []
        for start, key in sorted(keys):
            bucket = self._buckets.pop((start, key))
            results.append(
                WindowResult(
                    window_start=start,
                    window_end=start + self.window_size,
                    key=key,
                    count=bucket.count,
                    total=bucket.total,
                    minimum=bucket.minimum,
                    maximum=bucket.maximum,
                )
            )
            self.stats.windows_emitted += 1
        return results

    def flush(self) -> list[WindowResult]:
        """Emit every open window (end-of-stream / bounded source)."""
        return self._emit(list(self._buckets.keys()))
