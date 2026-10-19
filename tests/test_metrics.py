import pytest

from lakehouse_platform.metrics import LatencyTracker, SLIEvaluator, SLO


def test_percentiles_nearest_rank():
    tracker = LatencyTracker()
    for value in range(1, 101):
        tracker.record(float(value))
    assert tracker.percentile(50) == 50.0
    assert tracker.percentile(95) == 95.0
    assert tracker.percentile(99) == 99.0
    assert tracker.percentile(100) == 100.0
    summary = tracker.summary()
    assert summary["count"] == 100
    assert summary["max"] == 100.0


def test_empty_tracker_summary_is_zeroed():
    assert LatencyTracker().summary()["count"] == 0
    assert LatencyTracker().percentile(95) == 0.0


def test_negative_latency_rejected():
    with pytest.raises(ValueError):
        LatencyTracker().record(-1.0)


def test_slo_directions():
    assert SLO("x", target=5.0, unit="s", direction="max").evaluate(3.0).met
    assert not SLO("x", target=5.0, unit="s", direction="max").evaluate(7.0).met
    assert SLO("y", target=0.99, unit="ratio", direction="min").evaluate(0.995).met
    assert not SLO("y", target=0.99, unit="ratio", direction="min").evaluate(0.9).met


def test_evaluator_grades_a_run():
    tracker = LatencyTracker()
    for _ in range(100):
        tracker.record(1.0)
    results = SLIEvaluator().evaluate(
        latency=tracker, dq_pass_rate=0.99, late_events=1, processed_events=1000
    )
    by_name = {r.slo.name: r for r in results}
    assert by_name["e2e_latency_p95_seconds"].met
    assert by_name["data_quality_pass_rate"].met
    assert by_name["late_event_rate"].met
    assert by_name["late_event_rate"].value == pytest.approx(0.001)
