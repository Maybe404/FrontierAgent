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
import json
import logging
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
        self.lost_seqs: list[int] = []
        self._started_mono = time.monotonic()

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

    # -- lifecycle ----------------------------------------------------------

    async def start(self, *, task: str, config: dict[str, Any] | None = None) -> None:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        (self.run_dir / "run.lock").write_text(
            to_json({"pid": os.getpid(), "run_id": self.run_id,
                     "host": socket.gethostname()}),
            encoding="utf-8",
        )
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

    async def end(self, *, status: str, output: str = "", error: str = "") -> None:
        await self.emit("run.end", {
            "status": status,
            "output": output,
            "error": error,
            "duration_ms": round((time.monotonic() - self._started_mono) * 1000),
            "events": self._seq + 1,
            "complete": self.errors == 0,
            "telemetry_errors": self.errors,
            "lost_seqs": list(self.lost_seqs),
        }, span_id=self.root_span_id)
        with contextlib.suppress(OSError):
            (self.run_dir / "run.lock").unlink(missing_ok=True)


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
