from datetime import timedelta

from lakehouse_platform.events import Event, utcnow
from lakehouse_platform.pipeline import StreamingPipeline
from lakehouse_platform.transport import InProcessBus


def _order(seq: int, seconds_ago: float, amount: float = 25.0, **overrides) -> Event:
    payload = {
        "order_id": f"ord-{seq}",
        "customer_id": "cust-1",
        "region": "north",
        "amount": amount,
        "currency": "USD",
        "items": 1,
    }
    payload.update(overrides)
    return Event.new(
        "order.created",
        "north",
        payload,
        event_time=utcnow() - timedelta(seconds=seconds_ago),
        event_id=f"evt-{seq}",
    )


def _bus_with(events) -> InProcessBus:
    bus = InProcessBus()
    for event in events:
        bus.publish("orders", event)
    return bus


def test_end_to_end_happy_path(tmp_path):
    events = [_order(i, seconds_ago=2.0 - i * 0.01, amount=10.0 * (i + 1)) for i in range(20)]
    pipeline = StreamingPipeline(source=_bus_with(events), lakehouse_root=tmp_path)
    report = pipeline.run()

    assert report.events_read == 20
    assert report.events_valid == 20
    assert report.events_quarantined == 0
    assert report.windows_emitted >= 1
    assert pipeline.raw_table.count() == 20
    agg_rows = pipeline.agg_table.read()
    assert sum(r["event_count"] for r in agg_rows) == 20
    assert report.latency["count"] == 20
    assert report.healthy  # latency ~2s, DQ 100%, no late events


def test_corrupt_events_are_quarantined_not_landed(tmp_path):
    good = _order(1, seconds_ago=1.0)
    bad = _order(2, seconds_ago=1.0, amount=-3.0)
    pipeline = StreamingPipeline(source=_bus_with([good, bad]), lakehouse_root=tmp_path)
    report = pipeline.run()

    assert report.events_valid == 1
    assert report.events_quarantined == 1
    assert pipeline.raw_table.count() == 1
    quarantine = pipeline.quarantine_table.read()
    assert len(quarantine) == 1
    assert "range" in quarantine[0]["dq_failures"]


def test_pipeline_report_summary_shape(tmp_path):
    pipeline = StreamingPipeline(source=_bus_with([_order(1, 0.5)]), lakehouse_root=tmp_path)
    summary = pipeline.run().summary()
    for key in ("events_read", "events_valid", "latency", "slis", "healthy"):
        assert key in summary
    assert {s["name"] for s in summary["slis"]} == {
        "e2e_latency_p95_seconds",
        "e2e_latency_p99_seconds",
        "data_quality_pass_rate",
        "late_event_rate",
    }
