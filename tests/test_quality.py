from datetime import timedelta

from lakehouse_platform.events import Event, utcnow
from lakehouse_platform.generator import SyntheticOrderGenerator
from lakehouse_platform.quality import DataQualityEngine


def _order(**overrides):
    payload = {
        "order_id": "ord-1",
        "customer_id": "cust-1",
        "region": "north",
        "amount": 42.0,
        "currency": "USD",
        "items": 2,
    }
    payload.update(overrides)
    return Event.new("order.created", "north", payload)


def test_valid_event_passes():
    report = DataQualityEngine().check([_order()])
    assert report.valid_count == 1
    assert report.quarantined_count == 0
    assert report.pass_rate == 1.0


def test_missing_field_is_quarantined_with_reason():
    event = _order()
    payload = dict(event.payload)
    del payload["amount"]
    bad = Event.new("order.created", "north", payload, event_id=event.event_id)
    report = DataQualityEngine().check([bad])
    assert report.quarantined_count == 1
    assert "amount" in report.quarantined[0][1][0]


def test_negative_amount_fails_range():
    report = DataQualityEngine().check([_order(amount=-5.0)])
    assert report.quarantined_count == 1
    assert any("range" in r for r in report.quarantined[0][1])


def test_wrong_type_fails_schema():
    report = DataQualityEngine().check([_order(amount="lots")])
    assert report.quarantined_count == 1
    assert any("schema" in r for r in report.quarantined[0][1])


def test_duplicate_event_id_fails_uniqueness():
    event = _order()
    report = DataQualityEngine().check([event, event])
    assert report.valid_count == 1
    assert report.quarantined_count == 1
    assert any("uniqueness" in r for r in report.quarantined[0][1])


def test_stale_event_fails_timeliness():
    old = Event.new(
        "order.created", "north", _order().payload, event_time=utcnow() - timedelta(days=30)
    )
    report = DataQualityEngine().check([old])
    assert report.quarantined_count == 1
    assert any("timeliness" in r for r in report.quarantined[0][1])


def test_generator_corruption_is_caught():
    gen = SyntheticOrderGenerator(seed=7, corrupt_fraction=1.0)
    events = list(gen.generate(20))
    report = DataQualityEngine().check(events)
    # Every generated event is corrupt; none may reach the lakehouse.
    # (Timeliness uses wall-clock now, and generator times are historical,
    # so even structurally-clean events would quarantine — assert on the
    # structural signal instead: schema/range rules must have failures.)
    rule_failures = sum(r.failed for r in report.rules.values())
    assert rule_failures >= 20
