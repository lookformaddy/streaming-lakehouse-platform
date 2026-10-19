"""Synthetic event generator.

Produces realistic, fully synthetic retail order events (no real data of
any kind) so the platform can be demoed, load-tested and regression-tested
anywhere. A fixed ``seed`` makes runs deterministic. A configurable
``corrupt_fraction`` deliberately injects malformed records so the data
quality quarantine path is exercised on every demo run.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Iterator

from .events import Event

_REGIONS = ["north", "south", "east", "west", "central"]
_CHANNELS = ["web", "mobile", "store", "partner"]


class SyntheticOrderGenerator:
    """Generate a stream of synthetic ``order.created`` events."""

    def __init__(
        self,
        seed: int = 42,
        corrupt_fraction: float = 0.0,
        start_time: datetime | None = None,
        events_per_second: float = 10.0,
        span_seconds: float | None = None,
    ) -> None:
        if not 0.0 <= corrupt_fraction <= 1.0:
            raise ValueError("corrupt_fraction must be between 0 and 1")
        self._rng = random.Random(seed)
        self._corrupt_fraction = corrupt_fraction
        self._start = start_time or datetime(2026, 1, 1, tzinfo=timezone.utc)
        self._step = timedelta(seconds=1.0 / events_per_second)
        self._span_seconds = span_seconds
        self._seq = 0

    def _clean_payload(self, seq: int) -> dict:
        rng = self._rng
        return {
            "order_id": f"ord-{seq:06d}",
            "customer_id": f"cust-{rng.randint(1, 5000):05d}",
            "region": rng.choice(_REGIONS),
            "channel": rng.choice(_CHANNELS),
            "amount": round(rng.uniform(5.0, 500.0), 2),
            "currency": "USD",
            "items": rng.randint(1, 8),
        }

    def _corrupt(self, payload: dict) -> dict:
        """Break a payload in one of the ways real feeds break."""
        mode = self._rng.randint(0, 3)
        payload = dict(payload)
        if mode == 0:
            payload.pop("amount", None)  # missing required field
        elif mode == 1:
            payload["amount"] = -abs(payload.get("amount", 1.0))  # invalid range
        elif mode == 2:
            payload["amount"] = "not-a-number"  # wrong type
        else:
            payload["region"] = None  # null in a non-nullable field
        return payload

    def generate(self, count: int) -> Iterator[Event]:
        step = self._step
        if self._span_seconds is not None and count > 0:
            # Spread the batch evenly over span_seconds of event time —
            # useful for demos that start "just now" and measure latency.
            step = timedelta(seconds=self._span_seconds / count)
        for _ in range(count):
            self._seq += 1
            seq = self._seq
            event_time = self._start + step * (seq - 1)
            payload = self._clean_payload(seq)
            if self._rng.random() < self._corrupt_fraction:
                payload = self._corrupt(payload)
            yield Event.new(
                event_type="order.created",
                key=str(payload.get("region") or "unknown"),
                payload=payload,
                event_time=event_time,
                event_id=f"evt-{seq:06d}",
            )
