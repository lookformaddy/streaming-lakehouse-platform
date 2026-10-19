"""Demo CLI: generate synthetic orders, run the pipeline, print the report.

Usage::

    python -m lakehouse_platform.cli --events 1000 --out ./lakehouse-data

The run is fully local: an in-process bus stands in for Kafka, the
lakehouse is a directory on disk, and the report printed at the end is
the same summary an operator would see (counts, latency percentiles,
SLI pass/fail, table versions).
"""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import timedelta
from pathlib import Path

from .events import utcnow
from .generator import SyntheticOrderGenerator
from .pipeline import StreamingPipeline
from .transport import InProcessBus


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lakehouse-platform")
    parser.add_argument("--events", type=int, default=1000, help="synthetic events to generate")
    parser.add_argument("--out", default="./lakehouse-data", help="lakehouse output directory")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--corrupt-fraction",
        type=float,
        default=0.03,
        help="fraction of events deliberately corrupted to exercise the DQ quarantine path",
    )
    parser.add_argument(
        "--span-seconds",
        type=float,
        default=3.0,
        help="event-time span of the generated batch ending now (drives measured latency)",
    )
    parser.add_argument("--fresh", action="store_true", help="delete --out before running")
    args = parser.parse_args(argv)

    out = Path(args.out)
    if args.fresh and out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)

    bus = InProcessBus()
    generator = SyntheticOrderGenerator(
        seed=args.seed,
        corrupt_fraction=args.corrupt_fraction,
        start_time=utcnow() - timedelta(seconds=args.span_seconds),
        span_seconds=args.span_seconds,
    )
    for event in generator.generate(args.events):
        bus.publish("orders", event)

    pipeline = StreamingPipeline(source=bus, lakehouse_root=out)
    report = pipeline.run()

    print(json.dumps(report.summary(), indent=2))
    print(
        f"\nTables under {out}: orders_raw (v{report.raw_table_version}), "
        f"orders_by_region_window (v{report.agg_table_version}), orders_quarantine"
    )
    print("SLI status:", "HEALTHY" if report.healthy else "SLO BREACH — see slis above")
    return 0 if report.healthy else 1


if __name__ == "__main__":
    raise SystemExit(main())
