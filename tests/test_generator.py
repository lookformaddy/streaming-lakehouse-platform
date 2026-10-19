from lakehouse_platform.generator import SyntheticOrderGenerator


def test_generator_is_deterministic():
    a = [e.to_dict() for e in SyntheticOrderGenerator(seed=3).generate(10)]
    b = [e.to_dict() for e in SyntheticOrderGenerator(seed=3).generate(10)]
    assert a == b


def test_generator_event_shape():
    events = list(SyntheticOrderGenerator(seed=1).generate(5))
    assert len(events) == 5
    for event in events:
        assert event.event_type == "order.created"
        assert event.payload["currency"] == "USD"
        assert event.payload["amount"] > 0


def test_generator_event_times_increase():
    events = list(SyntheticOrderGenerator(seed=1, events_per_second=10).generate(5))
    times = [e.event_time for e in events]
    assert times == sorted(times)


def test_generator_span_mode():
    events = list(SyntheticOrderGenerator(seed=1, span_seconds=60.0).generate(61))
    span = (events[-1].event_time - events[0].event_time).total_seconds()
    assert 59.0 <= span <= 60.0
