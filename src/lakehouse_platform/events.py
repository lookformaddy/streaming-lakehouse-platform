"""Event model for the streaming lakehouse platform.

An :class:`Event` is the unit of data that flows through the platform. It
mirrors a Kafka record: a key, a typed payload, an event time (when the
thing happened) and an ingestion time (when the platform saw it). Keeping
event time and ingestion time separate is what makes latency SLIs and
late-arrival handling possible.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Mapping


def utcnow() -> datetime:
    """Current time as a timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def _parse_ts(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


@dataclass(frozen=True)
class Event:
    """An immutable streaming event.

    Attributes:
        event_id: Globally unique identifier (used for de-duplication).
        event_type: Logical topic / event name, e.g. ``"order.created"``.
        key: Partition key (records with the same key stay ordered together).
        event_time: When the event occurred at the source (UTC).
        payload: The event body. Must be JSON-serialisable.
        ingest_time: When the platform ingested the event (UTC). ``None``
            until the event passes through an ingestion transport.
    """

    event_id: str
    event_type: str
    key: str
    event_time: datetime
    payload: Mapping[str, Any]
    ingest_time: datetime | None = None

    @classmethod
    def new(
        cls,
        event_type: str,
        key: str,
        payload: Mapping[str, Any],
        event_time: datetime | None = None,
        event_id: str | None = None,
    ) -> "Event":
        return cls(
            event_id=event_id or str(uuid.uuid4()),
            event_type=event_type,
            key=key,
            event_time=event_time or utcnow(),
            payload=dict(payload),
        )

    def stamped(self, ingest_time: datetime | None = None) -> "Event":
        """Return a copy with ``ingest_time`` set (defaults to now)."""
        return replace(self, ingest_time=ingest_time or utcnow())

    @property
    def latency_seconds(self) -> float | None:
        """End-to-end ingestion latency, or ``None`` if not yet ingested."""
        if self.ingest_time is None:
            return None
        return (self.ingest_time - self.event_time).total_seconds()

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "key": self.key,
            "event_time": self.event_time.isoformat(),
            "ingest_time": self.ingest_time.isoformat() if self.ingest_time else None,
            "payload": dict(self.payload),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Event":
        ingest = data.get("ingest_time")
        return cls(
            event_id=str(data["event_id"]),
            event_type=str(data["event_type"]),
            key=str(data["key"]),
            event_time=_parse_ts(str(data["event_time"])),
            payload=dict(data.get("payload") or {}),
            ingest_time=_parse_ts(str(ingest)) if ingest else None,
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"), sort_keys=True)

    @classmethod
    def from_json(cls, line: str) -> "Event":
        return cls.from_dict(json.loads(line))


@dataclass
class EventBatch:
    """A micro-batch of events pulled from a transport in one poll."""

    events: list[Event] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.events)

    def __iter__(self):
        return iter(self.events)
