"""Optional HTTP surface for the platform (requires the ``api`` extra).

Exposes the operational endpoints an SRE or data-platform team expects:

* ``GET  /health``  — liveness.
* ``GET  /metrics`` — latency summary + table versions for a lakehouse root.
* ``POST /run``     — run one pipeline pass over an NDJSON file of events
  and return the run report.

Install with ``pip install -e ".[api]"`` and serve with
``uvicorn lakehouse_platform.api:app``. The core platform has no web
dependency; this module is a thin shell over it.
"""

from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .pipeline import StreamingPipeline
from .table import LakehouseTable
from .transport import FileReplaySource

LAKEHOUSE_ROOT = Path(os.environ.get("LAKEHOUSE_ROOT", "./lakehouse-data"))

app = FastAPI(title="Streaming Lakehouse Platform", version="0.1.0")


class RunRequest(BaseModel):
    events_file: str
    topic: str = "orders"


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/metrics")
def metrics() -> dict:
    tables = {}
    for name in ("orders_raw", "orders_by_region_window", "orders_quarantine"):
        table = LakehouseTable(LAKEHOUSE_ROOT, name)
        tables[name] = {"version": table.version, "records": table.count()}
    return {"lakehouse_root": str(LAKEHOUSE_ROOT), "tables": tables}


@app.post("/run")
def run(request: RunRequest) -> dict:
    path = Path(request.events_file)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"events file not found: {path}")
    pipeline = StreamingPipeline(
        source=FileReplaySource(path), lakehouse_root=LAKEHOUSE_ROOT, topic=request.topic
    )
    return pipeline.run().summary()
