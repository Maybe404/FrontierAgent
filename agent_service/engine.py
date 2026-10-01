"""Adapter between the service and FrontierAgent.

The only module that knows how a task is executed (the ``frontier-agent``
CLI in native, non-interactive, auto-approve mode, one process per task) and
where its journal and deliverables land. When upstream changes the CLI,
run layout or journal format, this file changes and nothing else does.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

MODES = ("react", "agent_team", "research", "coding")


def build_command(*, task: str, mode: str, workdir: Path, max_turns: int | None) -> list[str]:
    cmd = [sys.executable, "-m", "apodex", "--native", "--print", "--yes", "--no-tui",
           "--mode", mode, "--cwd", str(workdir)]
    if max_turns:
        cmd += ["--max-turns", str(max_turns)]
    # "--" so a task starting with "-" is never parsed as an option.
    return [*cmd, "--", task]


def build_env(*, task_id: str, request_id: str) -> dict[str, str]:
    env = dict(os.environ)
    env["FRONTIER_TASK_ID"] = task_id
    env["FRONTIER_REQUEST_ID"] = request_id
    env["PYTHONUNBUFFERED"] = "1"
    env.setdefault("NO_COLOR", "1")
    return env


def run_dir(workdir: Path) -> Path | None:
    """The session directory the CLI created under *workdir* (one per task)."""
    runs = workdir / ".apodex" / "runs"
    if not runs.is_dir():
        return None
    dirs = sorted((d for d in runs.iterdir() if (d / "journal.db").exists()),
                  key=lambda d: d.stat().st_mtime)
    return dirs[-1] if dirs else None


def outputs_dir(workdir: Path) -> Path | None:
    rd = run_dir(workdir)
    return rd / "outputs" if rd else None


@dataclass
class RunResult:
    status: str = ""            # completed | error | cancelled | crashed | "" (no run.end)
    answer: str = ""
    error: str = ""
    complete: bool = False
    deliverables: list[dict[str, Any]] = field(default_factory=list[dict[str, Any]])


def read_result(workdir: Path) -> RunResult:
    rd = run_dir(workdir)
    result = RunResult()
    if rd is None:
        return result
    with closing(sqlite3.connect(rd / "journal.db")) as con:
        rows = con.execute(
            "SELECT kind, payload_json FROM context_journal "
            "WHERE kind IN ('run.end', 'deliverable') ORDER BY sequence"
        ).fetchall()
    for kind, payload in rows:
        data = json.loads(payload).get("data") or {}
        if kind == "deliverable":
            result.deliverables.append({k: data.get(k) for k in ("path", "bytes", "sha256", "media_type", "error")})
        else:
            result.status = str(data.get("status") or "")
            answer = data.get("output")
            if isinstance(answer, dict) and "blob" in answer:
                answer = _blob(rd, answer["blob"])
            result.answer = str(answer or "")
            result.error = str(data.get("error") or "")
            result.complete = bool(data.get("complete"))
    return result


def _blob(rd: Path, digest: str) -> str:
    try:
        return (rd / "blobs" / digest[:2] / digest[2:]).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def read_events(workdir: Path, after: int = 0, limit: int = 500) -> list[dict[str, Any]]:
    """Journal events (all agents) after sequence *after*, oldest first."""
    rd = run_dir(workdir)
    if rd is None:
        return []
    with closing(sqlite3.connect(rd / "journal.db")) as con:
        rows = con.execute(
            "SELECT sequence, agent_id, kind, payload_json FROM context_journal "
            "WHERE sequence > ? ORDER BY sequence LIMIT ?", (after, limit),
        ).fetchall()
    return [{"cursor": r[0], "agent_id": r[1], "kind": r[2], **json.loads(r[3])} for r in rows]


async def recover(workdir: Path) -> None:
    """Close a run whose worker died before writing run.end."""
    rd = run_dir(workdir)
    if rd is None:
        return
    from frontier_agent.telemetry.run import recover_stale

    await recover_stale(rd)
