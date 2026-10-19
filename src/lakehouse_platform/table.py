"""Lakehouse table: partitioned files + a transaction log.

A deliberately small, honest implementation of the lakehouse pattern
(Delta Lake / Apache Iceberg style) on a local filesystem:

* **Data files** are NDJSON, laid out in Hive-style partitions
  (``date=YYYY-MM-DD/hour=HH/``) derived from each record's event time.
* **A transaction log** (``_delta_log/NNNN.json``) records every commit:
  which files it added, how many records, and when. The log — not the
  directory listing — is the source of truth for what the table contains.
* **Snapshot reads** can pin any historical version (time travel), because
  commits are append-only and files are never mutated in place.
* **Partition pruning**: reads filtered on partition columns only open
  files under matching partition directories.

In production the same layout maps onto object storage + Parquet; the
semantics (log, snapshots, pruning) are the parts that matter.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

_VERSION_RE = re.compile(r"^(\d{20})\.json$")


class LakehouseTable:
    """An append-only, versioned, partitioned table on disk."""

    def __init__(self, root: str | Path, name: str) -> None:
        self.root = Path(root)
        self.name = name
        self.table_dir = self.root / name
        self.log_dir = self.table_dir / "_delta_log"
        self.log_dir.mkdir(parents=True, exist_ok=True)

    # -- transaction log ---------------------------------------------------
    def _commit_files(self) -> list[Path]:
        files = []
        for path in self.log_dir.iterdir():
            if _VERSION_RE.match(path.name):
                files.append(path)
        return sorted(files)

    def _read_commit(self, path: Path) -> dict[str, Any]:
        return json.loads(path.read_text(encoding="utf-8"))

    @property
    def version(self) -> int:
        """Latest committed version, or -1 for an empty table."""
        files = self._commit_files()
        if not files:
            return -1
        return int(files[-1].stem)

    def history(self) -> list[dict[str, Any]]:
        """All commit entries, oldest first."""
        return [self._read_commit(p) for p in self._commit_files()]

    def _active_files(self, version: int | None = None) -> list[str]:
        target = self.version if version is None else version
        files: list[str] = []
        for entry in self.history():
            if entry["version"] <= target:
                files.extend(f["path"] for f in entry["added"])
        return files

    # -- writes --------------------------------------------------------------
    @staticmethod
    def _partition_of(record: Mapping[str, Any]) -> tuple[str, str]:
        ts = record.get("event_time")
        if isinstance(ts, datetime):
            moment = ts
        elif isinstance(ts, str):
            moment = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        else:
            raise ValueError("record must carry an 'event_time' (datetime or ISO string)")
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        return moment.strftime("%Y-%m-%d"), moment.strftime("%H")

    def append(self, records: Iterable[Mapping[str, Any]]) -> int:
        """Append records as a new commit; returns the new version.

        Records are grouped by (date, hour) partition; each partition gets
        one new data file per commit. Empty appends are a no-op that
        returns the current version (no empty commits in the log).
        """
        records = list(records)
        if not records:
            return self.version
        new_version = self.version + 1

        by_partition: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for record in records:
            by_partition.setdefault(self._partition_of(record), []).append(dict(record))

        added: list[dict[str, Any]] = []
        for (date_part, hour_part), rows in sorted(by_partition.items()):
            part_dir = self.table_dir / f"date={date_part}" / f"hour={hour_part}"
            part_dir.mkdir(parents=True, exist_ok=True)
            rel = f"date={date_part}/hour={hour_part}/part-{new_version:05d}-000.ndjson"
            with (self.table_dir / rel).open("w", encoding="utf-8") as fh:
                for row in rows:
                    fh.write(json.dumps(row, default=str, separators=(",", ":")) + "\n")
            added.append(
                {"path": rel, "partition": {"date": date_part, "hour": hour_part},
                 "records": len(rows)}
            )

        commit = {
            "version": new_version,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "operation": "APPEND",
            "records": len(records),
            "added": added,
        }
        (self.log_dir / f"{new_version:020d}.json").write_text(
            json.dumps(commit, indent=2), encoding="utf-8"
        )
        return new_version

    # -- reads ---------------------------------------------------------------
    def read(
        self,
        version: int | None = None,
        date: str | None = None,
        hour: str | None = None,
    ) -> list[dict[str, Any]]:
        """Read a snapshot. ``date``/``hour`` filters prune partitions."""
        rows: list[dict[str, Any]] = []
        for rel in self._active_files(version):
            parts = Path(rel).parts
            if date is not None and f"date={date}" not in parts:
                continue
            if hour is not None and f"hour={hour}" not in parts:
                continue
            with (self.table_dir / rel).open(encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        rows.append(json.loads(line))
        return rows

    def count(self, version: int | None = None) -> int:
        target = self.version if version is None else version
        return sum(
            entry["records"] for entry in self.history() if entry["version"] <= target
        )

    def partitions(self) -> list[dict[str, str]]:
        seen: list[dict[str, str]] = []
        for entry in self.history():
            for f in entry["added"]:
                if f["partition"] not in seen:
                    seen.append(f["partition"])
        return sorted(seen, key=lambda p: (p["date"], p["hour"]))
