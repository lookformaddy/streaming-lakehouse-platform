"""Data quality engine.

Every micro-batch passes through a set of declarative rules *before* it is
allowed into the lakehouse. Events that fail are not dropped silently —
they are quarantined with the exact reasons attached, and the batch report
records pass rates per rule. That report feeds the data-quality SLI.

Rule kinds implemented:

* **Schema** — required fields present, with the expected JSON types.
* **Range** — numeric fields inside an allowed interval.
* **Uniqueness** — no duplicate ``event_id`` inside a batch.
* **Timeliness** — event time not implausibly far in the future or past
  (catches broken producer clocks, a classic silent-corruption source).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Callable

from .events import Event, utcnow


@dataclass
class RuleResult:
    rule: str
    checked: int = 0
    failed: int = 0

    @property
    def pass_rate(self) -> float:
        return 1.0 if self.checked == 0 else 1.0 - self.failed / self.checked


@dataclass
class QualityReport:
    """Outcome of checking one batch."""

    total: int = 0
    valid: list[Event] = field(default_factory=list)
    quarantined: list[tuple[Event, list[str]]] = field(default_factory=list)
    rules: dict[str, RuleResult] = field(default_factory=dict)

    @property
    def valid_count(self) -> int:
        return len(self.valid)

    @property
    def quarantined_count(self) -> int:
        return len(self.quarantined)

    @property
    def pass_rate(self) -> float:
        return 1.0 if self.total == 0 else len(self.valid) / self.total

    def summary(self) -> dict[str, Any]:
        return {
            "total": self.total,
            "valid": self.valid_count,
            "quarantined": self.quarantined_count,
            "pass_rate": round(self.pass_rate, 4),
            "rules": {
                name: {
                    "checked": r.checked,
                    "failed": r.failed,
                    "pass_rate": round(r.pass_rate, 4),
                }
                for name, r in sorted(self.rules.items())
            },
        }


# A rule returns a failure reason string, or None when the event passes.
RuleFn = Callable[[Event], str | None]


def schema_rule(required: dict[str, type | tuple[type, ...]]) -> tuple[str, RuleFn]:
    """Build a schema rule from {payload_field: expected_type}."""

    def check(event: Event) -> str | None:
        for name, expected in required.items():
            if name not in event.payload or event.payload[name] is None:
                return f"schema: missing or null field '{name}'"
            value = event.payload[name]
            # bool is a subclass of int; reject it for numeric fields.
            if expected in (int, float) or (
                isinstance(expected, tuple) and any(t in (int, float) for t in expected)
            ):
                if isinstance(value, bool) or not isinstance(value, (int, float)):
                    return f"schema: field '{name}' has wrong type {type(value).__name__}"
            elif not isinstance(value, expected):
                return f"schema: field '{name}' has wrong type {type(value).__name__}"
        return None

    return "schema", check


def range_rule(field_name: str, minimum: float, maximum: float) -> tuple[str, RuleFn]:
    def check(event: Event) -> str | None:
        value = event.payload.get(field_name)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None  # type problems are the schema rule's job
        if not minimum <= float(value) <= maximum:
            return f"range: '{field_name}'={value} outside [{minimum}, {maximum}]"
        return None

    return f"range:{field_name}", check


def timeliness_rule(
    max_future_skew: timedelta = timedelta(minutes=5),
    max_age: timedelta = timedelta(days=7),
) -> tuple[str, RuleFn]:
    def check(event: Event) -> str | None:
        now = utcnow()
        if event.event_time - now > max_future_skew:
            return "timeliness: event_time too far in the future"
        if now - event.event_time > max_age:
            return "timeliness: event_time too old"
        return None

    return "timeliness", check


class DataQualityEngine:
    """Apply rules to a batch; split it into valid and quarantined events."""

    def __init__(self, rules: list[tuple[str, RuleFn]] | None = None) -> None:
        self._rules = rules if rules is not None else default_order_rules()

    def check(self, events: list[Event]) -> QualityReport:
        report = QualityReport(total=len(events))
        for name, _ in self._rules:
            report.rules[name] = RuleResult(rule=name)
        seen_ids: set[str] = set()
        report.rules["uniqueness"] = RuleResult(rule="uniqueness")

        for event in events:
            reasons: list[str] = []
            for name, fn in self._rules:
                result = report.rules[name]
                result.checked += 1
                failure = fn(event)
                if failure:
                    result.failed += 1
                    reasons.append(failure)
            uniq = report.rules["uniqueness"]
            uniq.checked += 1
            if event.event_id in seen_ids:
                uniq.failed += 1
                reasons.append(f"uniqueness: duplicate event_id '{event.event_id}'")
            seen_ids.add(event.event_id)

            if reasons:
                report.quarantined.append((event, reasons))
            else:
                report.valid.append(event)
        return report


def default_order_rules() -> list[tuple[str, RuleFn]]:
    """The rule set for the synthetic ``order.created`` feed."""
    return [
        schema_rule(
            {
                "order_id": str,
                "customer_id": str,
                "region": str,
                "amount": (int, float),
                "currency": str,
                "items": int,
            }
        ),
        range_rule("amount", minimum=0.01, maximum=1_000_000.0),
        timeliness_rule(),
    ]
