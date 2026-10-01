"""Durable task table (SQLite). One row per submitted task."""

from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    mode TEXT NOT NULL,
    task TEXT NOT NULL,
    max_turns INTEGER,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    pid INTEGER,
    exit_code INTEGER,
    workdir TEXT NOT NULL,
    answer TEXT,
    error TEXT,
    error_code TEXT,
    complete INTEGER,
    deliverables_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status, created_at);
"""

ACTIVE = ("queued", "running", "cancelling")
FINAL = ("completed", "failed", "cancelled", "timed_out")
# Columns added after the first release; created on open if missing.
_MIGRATIONS = {"error_code": "TEXT"}


def now() -> str:
    return datetime.now(UTC).isoformat()


class TaskStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as con, con:
            con.executescript(_SCHEMA)
            have = {r[1] for r in con.execute("PRAGMA table_info(tasks)")}
            for col, typ in _MIGRATIONS.items():
                if col not in have:
                    try:
                        con.execute(f"ALTER TABLE tasks ADD COLUMN {col} {typ}")
                    except sqlite3.OperationalError as exc:   # another instance won the race
                        if "duplicate column" not in str(exc):
                            raise

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.path, timeout=30)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        return con

    def create(self, *, task: str, mode: str, request_id: str | None,
               max_turns: int | None, workdir_root: Path) -> tuple[dict[str, Any], bool]:
        """Insert a task; an existing ``request_id`` returns that task instead."""
        if request_id:
            existing = self.get_by_request(request_id)
            if existing:
                return existing, False
        task_id = uuid.uuid4().hex
        row = {
            "id": task_id, "request_id": request_id or task_id, "mode": mode, "task": task,
            "max_turns": max_turns, "status": "queued", "created_at": now(),
            "workdir": str(Path(workdir_root) / task_id),
        }
        try:
            with closing(self._connect()) as con, con:
                con.execute(
                    "INSERT INTO tasks (id, request_id, mode, task, max_turns, status, created_at, workdir) "
                    "VALUES (:id, :request_id, :mode, :task, :max_turns, :status, :created_at, :workdir)", row)
        except sqlite3.IntegrityError:
            # Concurrent duplicate submit with the same request_id.
            existing = self.get_by_request(request_id or "")
            if existing:
                return existing, False
            raise
        return self.get(task_id) or row, True

    def get(self, task_id: str) -> dict[str, Any] | None:
        with closing(self._connect()) as con:
            r = con.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        return _row(r)

    def get_by_request(self, request_id: str) -> dict[str, Any] | None:
        with closing(self._connect()) as con:
            r = con.execute("SELECT * FROM tasks WHERE request_id=?", (request_id,)).fetchone()
        return _row(r)

    def list(self, *, status: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        sql, params = "SELECT * FROM tasks", []
        if status:
            sql += " WHERE status=?"
            params.append(status)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        with closing(self._connect()) as con:
            return [r for r in (_row(x) for x in con.execute(sql, params).fetchall()) if r]

    def update(self, task_id: str, **fields: Any) -> None:
        if "deliverables" in fields:
            fields["deliverables_json"] = json.dumps(fields.pop("deliverables"), ensure_ascii=False)
        cols = ", ".join(f"{k}=?" for k in fields)
        with closing(self._connect()) as con, con:
            con.execute(f"UPDATE tasks SET {cols} WHERE id=?", [*fields.values(), task_id])

    def transition(self, task_id: str, from_status: tuple[str, ...], to_status: str, **fields: Any) -> bool:
        """Compare-and-set status change; False when the task was not in *from_status*."""
        if "deliverables" in fields:
            fields["deliverables_json"] = json.dumps(fields.pop("deliverables"), ensure_ascii=False)
        marks = ",".join("?" for _ in from_status)
        sets = ", ".join(["status=?", *(f"{k}=?" for k in fields)])
        with closing(self._connect()) as con, con:
            cur = con.execute(
                f"UPDATE tasks SET {sets} WHERE id=? AND status IN ({marks})",
                [to_status, *fields.values(), task_id, *from_status],
            )
            return cur.rowcount == 1


def _row(r: sqlite3.Row | None) -> dict[str, Any] | None:
    if r is None:
        return None
    d = dict(r)
    d["deliverables"] = json.loads(d.pop("deliverables_json") or "[]")
    d["complete"] = bool(d["complete"]) if d["complete"] is not None else None
    return d
