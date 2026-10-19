"""Transport layer: pluggable event sources and sinks.

The platform talks to Kafka-shaped interfaces, never to a concrete broker.
A *source* yields events; a *sink* accepts them. Two implementations ship
in the box:

* :class:`InProcessBus` — an in-memory, Kafka-like log with named topics.
  Used for local development, demos and tests.
* :class:`FileReplaySource` / :class:`FileSink` — NDJSON files on disk.
  A file sink is a durable append-only log; a replay source re-reads it,
  which makes pipeline runs fully reproducible.

Swapping in a real Kafka transport means implementing the same two small
protocols (``publish`` on the sink side, ``poll``/``stream`` on the source
side) — no pipeline code changes. That is the standard enterprise pattern
for keeping streaming logic portable across brokers.
"""

from __future__ import annotations

import json
from collections import defaultdict, deque
from pathlib import Path
from typing import Iterator, Protocol, runtime_checkable

from .events import Event, EventBatch, utcnow


@runtime_checkable
class EventSink(Protocol):
    """Anything events can be published to."""

    def publish(self, topic: str, event: Event) -> None: ...


@runtime_checkable
class EventSource(Protocol):
    """Anything events can be consumed from."""

    def poll(self, topic: str, max_records: int = 500) -> EventBatch: ...


class InProcessBus:
    """In-memory multi-topic log with per-consumer offsets.

    Topics are append-only deques. Each named consumer group tracks its own
    read offset per topic, so independent consumers do not steal each
    other's records — the same contract Kafka consumer groups provide.
    Publishing stamps ``ingest_time`` if it is not already set.
    """

    def __init__(self) -> None:
        self._topics: dict[str, list[Event]] = defaultdict(list)
        self._offsets: dict[tuple[str, str], int] = defaultdict(int)

    # -- sink side ---------------------------------------------------------
    def publish(self, topic: str, event: Event) -> None:
        if event.ingest_time is None:
            event = event.stamped()
        self._topics[topic].append(event)

    # -- source side -------------------------------------------------------
    def poll(
        self, topic: str, max_records: int = 500, group: str = "default"
    ) -> EventBatch:
        log = self._topics.get(topic, [])
        start = self._offsets[(group, topic)]
        batch = log[start : start + max_records]
        self._offsets[(group, topic)] = start + len(batch)
        return EventBatch(events=list(batch))

    def pending(self, topic: str, group: str = "default") -> int:
        """Records published but not yet consumed by ``group``."""
        return max(0, len(self._topics.get(topic, [])) - self._offsets[(group, topic)])

    def topics(self) -> list[str]:
        return sorted(self._topics)


class FileSink:
    """Append events to an NDJSON file — a durable, inspectable log."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def publish(self, topic: str, event: Event) -> None:
        if event.ingest_time is None:
            event = event.stamped()
        record = {"topic": topic, "event": event.to_dict()}
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, separators=(",", ":")) + "\n")


class FileReplaySource:
    """Replay a previously written NDJSON log as a stream.

    Replay is deterministic: the same file always produces the same event
    sequence, which is what makes backfills and regression tests possible.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._cache: dict[str, deque[Event]] = {}

    def _load(self, topic: str) -> deque[Event]:
        if topic in self._cache:
            return self._cache[topic]
        events: deque[Event] = deque()
        if self.path.exists():
            with self.path.open(encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    record = json.loads(line)
                    # Two on-disk shapes are supported: FileSink records
                    # ({"topic": ..., "event": {...}}) and bare events.
                    if "event" in record:
                        if record.get("topic") != topic:
                            continue
                        events.append(Event.from_dict(record["event"]))
                    else:
                        events.append(Event.from_dict(record))
        self._cache[topic] = events
        return events

    def poll(self, topic: str, max_records: int = 500, group: str = "default") -> EventBatch:
        queue = self._load(topic)
        batch = [queue.popleft() for _ in range(min(max_records, len(queue)))]
        return EventBatch(events=batch)

    def stream(self, topic: str) -> Iterator[Event]:
        """Yield every event for ``topic`` in file order."""
        yield from list(self._load(topic))


def replay_with_original_timing(source: FileReplaySource, topic: str) -> Iterator[Event]:
    """Yield events re-stamped so end-to-end latency can be measured live."""
    for event in source.stream(topic):
        yield event.stamped(utcnow())
