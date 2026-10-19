from lakehouse_platform.table import LakehouseTable


def _record(eid: str, ts: str, amount: float = 10.0) -> dict:
    return {"event_id": eid, "event_time": ts, "amount": amount}


def test_append_and_read_round_trip(tmp_path):
    table = LakehouseTable(tmp_path, "orders")
    version = table.append(
        [
            _record("a", "2026-01-01T10:15:00+00:00"),
            _record("b", "2026-01-01T10:45:00+00:00"),
            _record("c", "2026-01-01T11:05:00+00:00"),
        ]
    )
    assert version == 0
    assert table.version == 0
    assert table.count() == 3
    rows = table.read()
    assert {r["event_id"] for r in rows} == {"a", "b", "c"}


def test_partition_layout_and_pruning(tmp_path):
    table = LakehouseTable(tmp_path, "orders")
    table.append(
        [
            _record("a", "2026-01-01T10:15:00+00:00"),
            _record("b", "2026-01-01T11:15:00+00:00"),
            _record("c", "2026-01-02T10:15:00+00:00"),
        ]
    )
    assert (tmp_path / "orders" / "date=2026-01-01" / "hour=10").is_dir()
    assert (tmp_path / "orders" / "date=2026-01-02" / "hour=10").is_dir()

    hour10 = table.read(hour="10")
    assert {r["event_id"] for r in hour10} == {"a", "c"}
    day2 = table.read(date="2026-01-02")
    assert [r["event_id"] for r in day2] == ["c"]
    assert len(table.partitions()) == 3


def test_time_travel_snapshot_reads(tmp_path):
    table = LakehouseTable(tmp_path, "orders")
    table.append([_record("a", "2026-01-01T10:00:00+00:00")])
    table.append([_record("b", "2026-01-01T10:30:00+00:00")])
    assert table.version == 1
    assert table.count() == 2
    assert table.count(version=0) == 1
    snapshot = table.read(version=0)
    assert [r["event_id"] for r in snapshot] == ["a"]
    history = table.history()
    assert [h["version"] for h in history] == [0, 1]
    assert all(h["operation"] == "APPEND" for h in history)


def test_empty_append_is_noop(tmp_path):
    table = LakehouseTable(tmp_path, "orders")
    assert table.append([]) == -1
    assert table.version == -1
    assert table.history() == []


def test_record_without_event_time_rejected(tmp_path):
    table = LakehouseTable(tmp_path, "orders")
    try:
        table.append([{"event_id": "x"}])
        raise AssertionError("expected ValueError")
    except ValueError:
        pass
