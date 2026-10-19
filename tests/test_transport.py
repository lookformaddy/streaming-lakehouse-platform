from lakehouse_platform.events import Event
from lakehouse_platform.transport import FileReplaySource, FileSink, InProcessBus


def test_bus_publish_poll_and_offsets():
    bus = InProcessBus()
    for i in range(5):
        bus.publish("orders", Event.new("order.created", "north", {"n": i}))
    assert bus.pending("orders") == 5

    first = bus.poll("orders", max_records=2)
    assert [e.payload["n"] for e in first] == [0, 1]
    rest = bus.poll("orders")
    assert [e.payload["n"] for e in rest] == [2, 3, 4]
    assert bus.pending("orders") == 0


def test_bus_consumer_groups_are_independent():
    bus = InProcessBus()
    bus.publish("orders", Event.new("order.created", "east", {}))
    assert len(bus.poll("orders", group="a")) == 1
    assert len(bus.poll("orders", group="b")) == 1  # group b has its own offset


def test_bus_stamps_ingest_time():
    bus = InProcessBus()
    bus.publish("orders", Event.new("order.created", "east", {}))
    event = bus.poll("orders").events[0]
    assert event.ingest_time is not None
    assert event.latency_seconds is not None


def test_file_sink_and_replay_round_trip(tmp_path):
    path = tmp_path / "log.ndjson"
    sink = FileSink(path)
    for i in range(3):
        sink.publish("orders", Event.new("order.created", "south", {"n": i}))
    sink.publish("other", Event.new("order.created", "south", {"n": 99}))

    source = FileReplaySource(path)
    events = source.stream("orders")
    assert [e.payload["n"] for e in events] == [0, 1, 2]
    # Replay is deterministic: a fresh source sees the same stream.
    assert [e.payload["n"] for e in FileReplaySource(path).stream("orders")] == [0, 1, 2]
