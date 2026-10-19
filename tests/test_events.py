from datetime import timedelta

from lakehouse_platform.events import Event, utcnow


def test_event_round_trip_json():
    event = Event.new("order.created", "north", {"amount": 12.5})
    restored = Event.from_json(event.to_json())
    assert restored == event


def test_latency_requires_ingest_stamp():
    event = Event.new("order.created", "north", {})
    assert event.latency_seconds is None
    stamped = event.stamped(event.event_time + timedelta(seconds=2))
    assert stamped.latency_seconds == 2.0
    # original is unchanged (immutability)
    assert event.ingest_time is None


def test_new_event_defaults():
    before = utcnow()
    event = Event.new("order.created", "west", {"a": 1})
    assert event.event_id
    assert before <= event.event_time <= utcnow()
    assert event.payload == {"a": 1}
