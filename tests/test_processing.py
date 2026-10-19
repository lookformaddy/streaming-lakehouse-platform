from datetime import datetime, timedelta, timezone

from lakehouse_platform.events import Event
from lakehouse_platform.processing import TumblingWindowProcessor

BASE = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def _event(seconds: float, key: str = "north", amount: float = 10.0, eid: str | None = None):
    return Event.new(
        "order.created",
        key,
        {"amount": amount},
        event_time=BASE + timedelta(seconds=seconds),
        event_id=eid or f"e-{seconds}-{key}-{amount}",
    )


def test_window_aggregates_and_emits_on_watermark():
    proc = TumblingWindowProcessor(
        window_size=timedelta(minutes=1), allowed_lateness=timedelta(seconds=10)
    )
    assert proc.add(_event(5, amount=10.0)) == []
    assert proc.add(_event(20, amount=30.0)) == []
    # An event at t=80 pushes the watermark to t=70, closing [0,60).
    emitted = proc.add(_event(80))
    assert len(emitted) == 1
    result = emitted[0]
    assert result.window_start == BASE
    assert result.count == 2
    assert result.total == 40.0
    assert result.average == 20.0
    assert result.minimum == 10.0
    assert result.maximum == 30.0


def test_windows_are_keyed_separately():
    proc = TumblingWindowProcessor(
        window_size=timedelta(minutes=1), allowed_lateness=timedelta(seconds=0)
    )
    proc.add(_event(5, key="north", amount=10.0))
    proc.add(_event(5, key="south", amount=50.0))
    emitted = proc.add(_event(61, key="north"))
    by_key = {r.key: r for r in emitted}
    assert by_key["north"].total == 10.0
    assert by_key["south"].total == 50.0


def test_late_event_is_counted_not_aggregated():
    proc = TumblingWindowProcessor(
        window_size=timedelta(minutes=1), allowed_lateness=timedelta(seconds=0)
    )
    proc.add(_event(5, amount=10.0))
    proc.add(_event(61))  # closes [0,60)
    emitted = proc.add(_event(10, amount=999.0))  # late for the closed window
    assert emitted == []
    assert proc.stats.late == 1
    rest = proc.flush()
    totals = {r.window_start: r.total for r in rest}
    assert 999.0 not in totals.values()


def test_flush_emits_open_windows():
    proc = TumblingWindowProcessor(window_size=timedelta(minutes=5))
    proc.add(_event(30, amount=7.5))
    results = proc.flush()
    assert len(results) == 1
    assert results[0].count == 1
    assert results[0].total == 7.5
    assert proc.flush() == []  # nothing left open


def test_unmeasurable_events_are_skipped():
    proc = TumblingWindowProcessor()
    event = Event.new("order.created", "north", {"amount": "n/a"}, event_time=BASE)
    assert proc.add(event) == []
    assert proc.stats.processed == 0
