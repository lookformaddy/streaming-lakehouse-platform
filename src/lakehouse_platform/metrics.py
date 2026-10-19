"""Latency tracking and SLI/SLO evaluation.

Streaming platforms are operated against *service level indicators*
(SLIs) — most importantly end-to-end latency (event time → available in
the lakehouse) and data-quality pass rate. This module keeps latency
samples, computes percentiles, and evaluates measured SLIs against SLO
targets so a breach is a computed fact, not a vibe.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any


class LatencyTracker:
    """Collect latency samples (seconds) and summarise their distribution."""

    def __init__(self) -> None:
        self._samples: list[float] = []

    def record(self, latency_seconds: float) -> None:
        if latency_seconds < 0:
            raise ValueError("latency cannot be negative")
        self._samples.append(float(latency_seconds))

    def __len__(self) -> int:
        return len(self._samples)

    def percentile(self, p: float) -> float:
        """Nearest-rank percentile, ``p`` in [0, 100]."""
        if not self._samples:
            return 0.0
        if not 0 <= p <= 100:
            raise ValueError("p must be in [0, 100]")
        ordered = sorted(self._samples)
        rank = max(1, math.ceil(p / 100 * len(ordered)))
        return ordered[rank - 1]

    def summary(self) -> dict[str, Any]:
        if not self._samples:
            return {"count": 0, "mean": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
        return {
            "count": len(self._samples),
            "mean": round(sum(self._samples) / len(self._samples), 4),
            "p50": round(self.percentile(50), 4),
            "p95": round(self.percentile(95), 4),
            "p99": round(self.percentile(99), 4),
            "max": round(max(self._samples), 4),
        }


@dataclass(frozen=True)
class SLO:
    """A service level objective over one indicator."""

    name: str
    target: float
    unit: str
    direction: str = "max"  # "max": value must be <= target; "min": >= target

    def evaluate(self, value: float) -> "SLIResult":
        met = value <= self.target if self.direction == "max" else value >= self.target
        return SLIResult(slo=self, value=value, met=met)


@dataclass(frozen=True)
class SLIResult:
    slo: SLO
    value: float
    met: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.slo.name,
            "value": round(self.value, 4),
            "target": self.slo.target,
            "unit": self.slo.unit,
            "direction": self.slo.direction,
            "met": self.met,
        }


# Default objectives for this platform, stated the way an SRE review would.
DEFAULT_SLOS: list[SLO] = [
    SLO(name="e2e_latency_p95_seconds", target=5.0, unit="seconds", direction="max"),
    SLO(name="e2e_latency_p99_seconds", target=15.0, unit="seconds", direction="max"),
    SLO(name="data_quality_pass_rate", target=0.95, unit="ratio", direction="min"),
    SLO(name="late_event_rate", target=0.02, unit="ratio", direction="max"),
]


@dataclass
class SLIEvaluator:
    """Compute indicator values from run statistics and grade them."""

    slos: list[SLO] = field(default_factory=lambda: list(DEFAULT_SLOS))

    def evaluate(
        self,
        latency: LatencyTracker,
        dq_pass_rate: float,
        late_events: int,
        processed_events: int,
    ) -> list[SLIResult]:
        summary = latency.summary()
        late_rate = late_events / processed_events if processed_events else 0.0
        values = {
            "e2e_latency_p95_seconds": float(summary["p95"]),
            "e2e_latency_p99_seconds": float(summary["p99"]),
            "data_quality_pass_rate": float(dq_pass_rate),
            "late_event_rate": float(late_rate),
        }
        return [slo.evaluate(values[slo.name]) for slo in self.slos if slo.name in values]
