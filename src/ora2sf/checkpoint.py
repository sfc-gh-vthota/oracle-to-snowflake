from __future__ import annotations

"""Checkpoint tracking for resume capability."""

import json
from pathlib import Path
from datetime import datetime


class Checkpoint:
    def __init__(self, filepath: str = ".ora2sf_checkpoint.json"):
        self.filepath = Path(filepath)
        self._data = self._load()

    def _load(self) -> dict:
        if self.filepath.exists():
            with open(self.filepath) as f:
                return json.load(f)
        return {"tables": {}, "last_run": None}

    def _save(self):
        with open(self.filepath, "w") as f:
            json.dump(self._data, f, indent=2, default=str)

    def get_table_state(self, table: str) -> dict | None:
        return self._data["tables"].get(table)

    def mark_started(self, table: str, mode: str):
        self._data["tables"][table] = {
            "status": "in_progress",
            "mode": mode,
            "started_at": datetime.now().isoformat(),
        }
        self._save()

    def mark_completed(self, table: str, rows: int, scn: int | None = None,
                       timestamp: str | None = None):
        entry = self._data["tables"].get(table, {})
        entry.update({
            "status": "completed",
            "rows_loaded": rows,
            "completed_at": datetime.now().isoformat(),
        })
        if scn is not None:
            entry["last_scn"] = scn
        if timestamp is not None:
            entry["last_timestamp"] = timestamp
        self._data["tables"][table] = entry
        self._data["last_run"] = datetime.now().isoformat()
        self._save()

    def mark_failed(self, table: str, error: str):
        entry = self._data["tables"].get(table, {})
        entry.update({
            "status": "failed",
            "error": error,
            "failed_at": datetime.now().isoformat(),
        })
        self._data["tables"][table] = entry
        self._save()

    def get_last_scn(self, table: str) -> int | None:
        state = self.get_table_state(table)
        return state.get("last_scn") if state else None

    def get_last_timestamp(self, table: str) -> str | None:
        state = self.get_table_state(table)
        return state.get("last_timestamp") if state else None

    def summary(self) -> dict:
        completed = sum(1 for t in self._data["tables"].values() if t["status"] == "completed")
        failed = sum(1 for t in self._data["tables"].values() if t["status"] == "failed")
        in_progress = sum(1 for t in self._data["tables"].values() if t["status"] == "in_progress")
        return {
            "total": len(self._data["tables"]),
            "completed": completed,
            "failed": failed,
            "in_progress": in_progress,
            "last_run": self._data.get("last_run"),
        }
