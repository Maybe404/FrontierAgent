"""Per-run telemetry journal: the local source of truth for agent traces.

One ``TelemetryRun`` per user task. Events land in
``<run_dir>/journal.db`` (AgentCore's append-only SQLite journal) with large
fields in ``<run_dir>/blobs/`` (content-addressed, fsynced, read-only).
Writes never raise into the agent: failures are counted, logged and reported
on ``run.end`` as ``complete: false``.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import hashlib
import json
import logging
import mimetypes
import os
import socket
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from agent_core.context import ContextManager, ContextScope
from agent_core.context.blob_store import FileBlobStore
from agent_core.context.models import BlobRef
from agent_core.context.sqlite_store import SQLiteJournalStore

logger = logging.getLogger(__name__)

SCHEMA = "fa.event/v1"
# Strings longer than this move out of the row into a blob.
INLINE_LIMIT = 8 * 1024
_WRITE_ATTEMPTS = 3
HEARTBEAT_S = 10.0
LEASE_TIMEOUT_S = 120.0
# Deliverables up to this size are copied into the journal's blob store.
DELIVERABLE_BLOB_LIMIT = 50 * 1024 * 1024

current_run: contextvars.ContextVar[TelemetryRun | None] = contextvars.ContextVar(
    "fa_telemetry_run", default=None,
)
# Span id of the agent loop that is running in this context; a sub-agent
# spawned from it inherits the value as its parent.
current_agent_span: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "fa_telemetry_agent_span", default=None,
)


def enabled() -> bool:
    return os.getenv("FRONTIER_TELEMETRY", "1").strip() not in ("0", "false", "off")


def new_span_id() -> str:
    return uuid.uuid4().hex[:16]


def _json_default(value: Any) -> Any:
    for attr in ("model_dump", "to_dict", "_asdict"):
        fn = getattr(value, attr, None)
        if callable(fn):
            try:
                return fn()
            except Exception:
                pass
    if isinstance(value, (set, frozenset, tuple)):
        return list(value)
    if isinstance(value, bytes):
        return f"<{len(value)} bytes>"
    return repr(value)


def to_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=_json_default, sort_keys=True)


class TelemetryRun:
    """Journal for one run. Safe to share across all agents of the run."""

    def __init__(
        self,
        run_dir: Path,
        *,
        session_id: str,
        run_id: str | None = None,
        workflow: str = "",
    ) -> None:
        self.run_dir = Path(run_dir)
        self.session_id = session_id
        self.run_id = run_id or f"{session_id}.{uuid.uuid4().hex[:8]}"
        self.trace_id = uuid.uuid4().hex
        self.root_span_id = new_span_id()
        self.workflow = workflow
        self._ctx = ContextManager(
            SQLiteJournalStore(self.run_dir / "journal.db"),
            FileBlobStore(self.run_dir / "blobs"),
        )
        self._lock = asyncio.Lock()
        self._seq = 0
        self.errors = 0
        # Set by the top-level agent's loop.end; reported on run.end.
        self.final_output = ""
        # True once the caller reported the answer itself (workflow state),
        # which then wins over the last top-level loop's final content.
        self.answer_explicit = False
        # Caller-reported outcome (status, error); wins over "completed" when
        # the block exits normally but the task did not succeed.
        self.outcome: tuple[str, str] | None = None
        self.lost_seqs: list[int] = []
        self._started_mono = time.monotonic()

    def set_outcome(self, status: str, error: str = "") -> None:
        self.outcome = (status, error)

    def set_answer(self, text: str) -> None:
        if text:
            self.final_output = text
            self.answer_explicit = True

    # -- writing ------------------------------------------------------------

    async def put_blob(self, data: str | bytes, media_type: str = "text/plain") -> BlobRef:
        raw = data.encode("utf-8") if isinstance(data, str) else data
        return await self._ctx.blobs.put(raw, media_type=media_type)

    async def _offload(self, payload: dict[str, Any], blobs: dict[str, BlobRef]) -> dict[str, Any]:
        """Move oversized top-level string fields into blobs."""
        out: dict[str, Any] = {}
        for key, value in payload.items():
            if isinstance(value, str) and len(value) > INLINE_LIMIT:
                ref = await self.put_blob(value)
                blobs[key] = ref
                out[key] = {"blob": ref.digest, "bytes": ref.size, "preview": value[:500]}
            else:
                out[key] = value
        return out

    async def emit(
        self,
        kind: str,
        data: dict[str, Any] | None = None,
        *,
        agent_id: str = "",
        span_id: str | None = None,
        parent_span_id: str | None = None,
        turn: int | None = None,
        blobs: dict[str, BlobRef] | None = None,
    ) -> None:
        """Append one event. Never raises."""
        async with self._lock:
            self._seq += 1
            seq = self._seq
            try:
                refs: dict[str, BlobRef] = dict(blobs or {})
                # Round-trip through JSON so non-serialisable values are
                # stringified once, here, instead of failing in the store.
                body = json.loads(to_json(data or {}))
                body = await self._offload(body, refs)
                payload = {
                    "schema": SCHEMA,
                    "seq": seq,
                    "ts": datetime.now(UTC).isoformat(),
                    "mono_ms": round((time.monotonic() - self._started_mono) * 1000, 3),
                    "trace_id": self.trace_id,
                    "span_id": span_id,
                    "parent_span_id": parent_span_id,
                    "turn": turn,
                    "data": body,
                }
            except Exception as exc:
                self._record_loss(seq, kind, exc)
                return
            scope = ContextScope(
                session_id=self.session_id, run_id=self.run_id, agent_id=agent_id,
            )
            from agent_core.context.models import JournalEntry

            entry = JournalEntry(scope=scope, kind=kind, payload=payload, blobs=refs)
            for attempt in range(_WRITE_ATTEMPTS):
                try:
                    await self._ctx.journal.append(entry)
                    return
                except Exception as exc:
                    if attempt == _WRITE_ATTEMPTS - 1:
                        self._record_loss(seq, kind, exc)
                    else:
                        await asyncio.sleep(0.05 * (attempt + 1))

    def _record_loss(self, seq: int, kind: str, exc: BaseException) -> None:
        self.errors += 1
        self.lost_seqs.append(seq)
        logger.error("telemetry: lost event seq=%d kind=%s: %s", seq, kind, exc)

    def note_failure(self, where: str, exc: BaseException) -> None:
        """A telemetry step failed outside ``emit`` (e.g. a blob write): the
        run is reported incomplete even though no sequence number was lost."""
        self.errors += 1
        logger.error("telemetry: %s failed: %s", where, exc)

    # -- lifecycle ----------------------------------------------------------

    async def start(self, *, task: str, config: dict[str, Any] | None = None) -> None:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        (self.run_dir / "run.lock").write_text(
            to_json({"pid": os.getpid(), "run_id": self.run_id, "host": socket.gethostname(),
                     "pid_started": await asyncio.to_thread(_proc_start_marker)}),
            encoding="utf-8",
        )
        self._heartbeat = asyncio.create_task(self._beat())
        await self.emit("run.start", {
            "session_id": self.session_id,
            "run_id": self.run_id,
            "workflow": self.workflow,
            "task": task,
            "config": config or {},
            "version": _version_info(),
            "host": socket.gethostname(),
            "pid": os.getpid(),
            "env": {k: os.environ[k] for k in _ENV_KEYS if os.environ.get(k)},
        }, span_id=self.root_span_id)

    def stop_heartbeat(self) -> None:
        hb = getattr(self, "_heartbeat", None)
        if hb is not None:
            hb.cancel()
            self._heartbeat = None

    async def _beat(self) -> None:
        """Keep the run.lock lease fresh; ``recover_stale`` treats a lease
        silent for LEASE_TIMEOUT_S as abandoned."""
        lock = self.run_dir / "run.lock"
        while True:
            await asyncio.sleep(HEARTBEAT_S)
            with contextlib.suppress(OSError):
                os.utime(lock)

    @staticmethod
    def snapshot_outputs(outputs_dir: Path) -> dict[str, tuple[int, int]]:
        """``{relative path: (size, mtime_ns)}`` of the files present now."""
        if not outputs_dir.is_dir():
            return {}
        out = {}
        for p in outputs_dir.rglob("*"):
            if p.is_file():
                st = p.stat()
                out[str(p.relative_to(outputs_dir))] = (st.st_size, st.st_mtime_ns)
        return out

    async def record_outputs(self, outputs_dir: Path,
                             before: dict[str, tuple[int, int]] | None = None) -> None:
        """Journal deliverables: files new or changed since *before*."""
        now = await asyncio.to_thread(self.snapshot_outputs, outputs_dir)
        for rel in sorted(now):
            if before is not None and before.get(rel) == now[rel]:
                continue
            path = outputs_dir / rel
            try:
                size = now[rel][0]
                info: dict[str, Any] = {"path": rel, "bytes": size,
                                        "media_type": mimetypes.guess_type(rel)[0] or ""}
                refs: dict[str, BlobRef] = {}
                if size <= DELIVERABLE_BLOB_LIMIT:
                    data = await asyncio.to_thread(path.read_bytes)
                    ref = await self.put_blob(data, info["media_type"] or "application/octet-stream")
                    refs["content"] = ref
                    info["sha256"] = ref.digest
                else:
                    info["sha256"] = await asyncio.to_thread(_sha256_file, path)
                    info["content"] = "not stored: larger than the deliverable blob limit"
                await self.emit("deliverable", info, span_id=self.root_span_id, blobs=refs)
            except OSError as exc:
                await self.emit("deliverable", {"path": rel, "error": str(exc)},
                                span_id=self.root_span_id)

    async def end(self, *, status: str, output: str | None = None, error: str = "") -> None:
        await self.emit("run.end", {
            "status": status,
            "output": self.final_output if output is None else output,
            "error": error,
            "duration_ms": round((time.monotonic() - self._started_mono) * 1000),
            "events": self._seq + 1,
            "complete": self.errors == 0,
            "telemetry_errors": self.errors,
            "lost_seqs": list(self.lost_seqs),
        }, span_id=self.root_span_id)
        self.stop_heartbeat()
        with contextlib.suppress(OSError):
            (self.run_dir / "run.lock").unlink(missing_ok=True)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _proc_start_marker(pid: int | None = None) -> str:
    """Stable process start marker; distinguishes a reused pid.

    Linux: start time in clock ticks from /proc (no locale, no time zone).
    Elsewhere: ``ps lstart`` under a fixed locale and time zone, so the value
    compares equal whoever runs the check. Empty when unavailable.
    """
    pid = pid or os.getpid()
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
        return "ticks:" + stat.rsplit(")", 1)[1].split()[19]
    except (OSError, IndexError):
        pass
    import subprocess

    try:
        out = subprocess.run(["ps", "-o", "lstart=", "-p", str(pid)], capture_output=True, text=True,
                             timeout=5, env={**os.environ, "LC_ALL": "C", "TZ": "UTC"})
        return "lstart:" + out.stdout.strip() if out.stdout.strip() else ""
    except (OSError, subprocess.SubprocessError):
        return ""


def _same_process_alive(pid: int, started: str | None) -> bool:
    if not pid or not _pid_alive(pid):
        return False
    return not started or _proc_start_marker(pid) == started


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


async def recover_stale(run_dir: Path) -> str | None:
    """Close a run whose process died without writing run.end.

    ``run.lock`` names the owning pid; when that process is gone the run gets
    a synthetic ``run.end`` (status ``crashed``) so it is never left open.
    Returns the recovered run id, if any.
    """
    lock = run_dir / "run.lock"
    try:
        info = json.loads(lock.read_text(encoding="utf-8"))
        age = time.time() - lock.stat().st_mtime
    except (OSError, ValueError):
        return None
    same_host = info.get("host") == socket.gethostname()
    pid, started = int(info.get("pid", 0)), info.get("pid_started")
    owner_alive = same_host and _same_process_alive(pid, started)
    # A same-host owner verified alive (pid and start marker match) may be
    # slow to close, e.g. still journaling deliverables; never steal its run.
    # Our own pid is the exception: a run we still hold the lock of but are
    # not running any more was abandoned.
    if owner_alive and started and pid != os.getpid():
        return None
    owner_dead = same_host and not owner_alive
    # Other hosts (shared volume) are judged only by the heartbeat lease.
    if not owner_dead and age < LEASE_TIMEOUT_S:
        return None
    run_id = str(info.get("run_id") or "")
    db = run_dir / "journal.db"
    import sqlite3

    with contextlib.closing(sqlite3.connect(db)) as con:
        rows = con.execute(
            "SELECT session_id, payload_json, kind FROM context_journal WHERE run_id=? ORDER BY sequence",
            (run_id,),
        ).fetchall()
    if not rows:
        lock.unlink(missing_ok=True)
        return None
    if any(r[2] == "run.end" for r in rows):
        lock.unlink(missing_ok=True)
        return None
    session_id, first = rows[0][0], json.loads(rows[0][1])
    last = json.loads(rows[-1][1])
    run = TelemetryRun(run_dir, session_id=session_id, run_id=run_id)
    run.trace_id, run.root_span_id = first["trace_id"], first["span_id"]
    run._seq = int(last["seq"])
    await run.emit("run.end", {
        "status": "crashed",
        "synthetic": True,
        "error": f"process {info.get('pid')} on {info.get('host')} exited without closing the run",
        "last_event_ts": last.get("ts"),
        "complete": False,
    }, span_id=run.root_span_id)
    lock.unlink(missing_ok=True)
    return run_id


# Deployment identity worth stamping on every run (set by CI/CD or K8S).
_ENV_KEYS = (
    "GIT_COMMIT", "IMAGE_TAG", "APP_VERSION", "POD_NAME", "NODE_NAME",
    "OPENAI_MODEL", "FRONTIER_REQUEST_ID",
)


def _version_info() -> dict[str, str]:
    info: dict[str, str] = {}
    try:
        from importlib.metadata import version

        info["frontier_agent"] = version("frontier-agent")
        info["agent_core"] = version("apodex-agent-core")
    except Exception:
        pass
    head = Path(__file__).resolve().parents[2] / ".git" / "HEAD"
    try:
        ref = head.read_text(encoding="utf-8").strip()
        if ref.startswith("ref: "):
            ref_path = head.parent / ref[5:]
            info["git_ref"] = ref[5:]
            if ref_path.exists():
                info["git_commit"] = ref_path.read_text(encoding="utf-8").strip()
        else:
            info["git_commit"] = ref
    except OSError:
        pass
    return info
