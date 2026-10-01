"""Export run journals to Langfuse over OTLP/HTTP JSON.

The journal is the source of truth; Langfuse is a rebuildable view. Spans
are assembled from journal start/end event pairs (run, agent loop, LLM turn,
tool call) and sent once complete; ids come from the journal, and the span
keys already sent are recorded in ``<run_dir>/export.state``, so a live run,
a crash and a later replay converge on the same data without duplicates.
Spans still open at the end of a run (crash, cancellation) are sent on
``final`` export, marked unfinished.

Enabled when ``LANGFUSE_HOST``, ``LANGFUSE_PUBLIC_KEY`` and
``LANGFUSE_SECRET_KEY`` are set. Langfuse v4 accepts traces only on its OTLP
endpoint (``/api/public/otel/v1/traces``).
"""

from __future__ import annotations

import asyncio
import contextlib
import fcntl
import hashlib
import json
import logging
import os
import sqlite3
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

from frontier_agent.telemetry.redact import redact

logger = logging.getLogger(__name__)

_BATCH = 200
# Langfuse caps field sizes; the full value always stays in the local blob.
_FIELD_CAP = 200_000
_LIVE_INTERVAL_S = 2.0
# How long an unconfirmed in-flight span waits before it is resent.
INFLIGHT_GRACE_S = 120.0
_SHOW_LAST_MESSAGES = 8


def configured() -> bool:
    return all(os.getenv(k) for k in ("LANGFUSE_HOST", "LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY"))


def _hex16(*parts: Any) -> str:
    return hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()[:16]


def _ns(ts: str) -> str:
    return str(int(datetime.fromisoformat(ts).timestamp() * 1_000_000_000))


# -- journal access ----------------------------------------------------------

def list_runs(run_dir: Path) -> list[tuple[str, str]]:
    db = run_dir / "journal.db"
    if not db.exists():
        return []
    with contextlib.closing(sqlite3.connect(db)) as con:
        rows = con.execute(
            "SELECT session_id, run_id, MIN(sequence) AS first FROM context_journal "
            "GROUP BY session_id, run_id ORDER BY first"
        ).fetchall()
    return [(r[0], r[1]) for r in rows]


def read_entries(run_dir: Path, session_id: str, run_id: str, after: int = 0) -> list[dict[str, Any]]:
    """All agents' events of one run, in journal order (run-wide query)."""
    with contextlib.closing(sqlite3.connect(run_dir / "journal.db")) as con:
        rows = con.execute(
            "SELECT sequence, entry_id, agent_id, kind, payload_json FROM context_journal "
            "WHERE session_id=? AND run_id=? AND sequence>? ORDER BY sequence",
            (session_id, run_id, after),
        ).fetchall()
    return [
        {"sequence": r[0], "entry_id": r[1], "agent_id": r[2], "kind": r[3], **json.loads(r[4])}
        for r in rows
    ]


def blob_text(run_dir: Path, digest: str) -> str:
    path = run_dir / "blobs" / digest[:2] / digest[2:]
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return f"<missing blob {digest}>"


def _resolve(run_dir: Path, value: Any) -> Any:
    if isinstance(value, dict) and {"blob", "bytes"} <= set(value):
        return blob_text(run_dir, value["blob"])
    return value


def _cap(text: str) -> str:
    if len(text) > _FIELD_CAP:
        return text[:_FIELD_CAP] + f"\n…[truncated {len(text) - _FIELD_CAP} chars; full text in local journal]"
    return text


def _js(value: Any) -> str:
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=str)
    return _cap(redact(text))


# -- export state --------------------------------------------------------------

def _state_path(run_dir: Path) -> Path:
    return run_dir / "export.state"


def _load_state(run_dir: Path) -> dict[str, Any]:
    try:
        return json.loads(_state_path(run_dir).read_text())
    except (OSError, ValueError):
        return {}


def load_state(run_dir: Path, run_id: str) -> tuple[set[str], dict[str, list[Any]]]:
    """``(sent span keys, in-flight span key -> [span id, first sent at])``."""
    entry = _load_state(run_dir).get(run_id, {})
    if isinstance(entry, list):                # pre-inflight format
        return set(entry), {}
    inflight = {k: (v if isinstance(v, list) else [v, 0.0])
                for k, v in dict(entry.get("inflight", {})).items()}
    return set(entry.get("sent", [])), inflight


def load_sent(run_dir: Path, run_id: str) -> set[str]:
    return load_state(run_dir, run_id)[0]


def save_state(run_dir: Path, run_id: str, sent: set[str], inflight: dict[str, list[Any]]) -> None:
    """Atomic write; callers hold the run directory's export lock."""
    path = _state_path(run_dir)
    data = _load_state(run_dir)
    data[run_id] = {"sent": sorted(sent), "inflight": inflight}
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp")
    tmp.write_text(json.dumps(data))
    os.replace(tmp, path)


@contextlib.asynccontextmanager
async def export_lock(run_dir: Path, *, wait: bool) -> AsyncIterator[bool]:
    """Exclusive per-run-directory lock so live and manual exports never
    send the same spans concurrently. Yields False when busy and not waiting.
    A blocking wait runs in a thread so the event loop keeps serving."""
    fh = (run_dir / "export.lock").open("a")
    try:
        try:
            if wait:
                await asyncio.to_thread(fcntl.flock, fh, fcntl.LOCK_EX)
            else:
                fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)
    finally:
        fh.close()


# -- span assembly -------------------------------------------------------------

@dataclass
class _Span:
    key: str
    span_id: str
    parent: str | None
    name: str
    start: str
    end: str | None = None
    attrs: dict[str, Any] = field(default_factory=dict[str, Any])
    error: str = ""
    level: str = ""
    # Ended but held back until its agent moves on, so late additions
    # (tool.result.final from the tail observer) make it into the span.
    hold: bool = False


class SpanBuilder:
    """Pairs journal events into spans; fed incrementally, in journal order."""

    def __init__(self, run_dir: Path) -> None:
        self.run_dir = run_dir
        self.trace_id = ""
        self.last_ts = ""
        self.ended = False
        self.spans: dict[str, _Span] = {}

    def feed(self, entries: list[dict[str, Any]]) -> None:
        run_dir, spans, obs = self.run_dir, self.spans, "langfuse.observation."
        for e in entries:
            self._feed_one(run_dir, spans, obs, e)

    def take(self, sent: set[str], *, final: bool) -> list[_Span]:
        """Spans ready to send and not in *sent*; on *final*, close open ones."""
        obs = "langfuse.observation."
        ready = []
        for s in self.spans.values():
            if s.key in sent:
                continue
            if s.hold and not (final or self.ended):
                continue
            if not s.end and (final or self.ended):
                s.end = self.last_ts
                s.level = "WARNING"
                s.attrs[f"{obs}metadata.unfinished"] = "true"
            if s.end:
                ready.append(s)
        return ready

    def _feed_one(self, run_dir: Path, spans: dict[str, _Span], obs: str, e: dict[str, Any]) -> None:
        self.trace_id = self.trace_id or e["trace_id"]
        self.last_ts = e["ts"]
        if e["kind"] == "run.end":
            self.ended = True
        kind, d, ts = e["kind"], e.get("data") or {}, e["ts"]
        span, parent, turn = str(e.get("span_id") or ""), e.get("parent_span_id"), e.get("turn")
        if not kind.startswith("tool."):
            # The agent moved on: its finished tool spans are complete.
            for t in spans.values():
                if t.hold and t.parent == span:
                    t.hold = False
        meta = {f"{obs}metadata.agent_id": e.get("agent_id") or ""}

        if kind == "run.start":
            spans["run"] = _Span("run", span, None, d.get("workflow") or "run", ts, attrs={
                "langfuse.trace.name": d.get("workflow") or "run",
                "session.id": d.get("session_id") or "",
                "langfuse.trace.metadata.run_id": d.get("run_id") or "",
                "langfuse.trace.metadata.git_commit": (d.get("version") or {}).get("git_commit", ""),
                "langfuse.trace.metadata.host": d.get("host") or "",
                f"{obs}type": "chain",
                f"{obs}input": _js(_resolve(run_dir, d.get("task"))),
                f"{obs}metadata.version": _js(d.get("version") or {}),
                f"{obs}metadata.env": _js(d.get("env") or {}),
            })
        elif kind == "run.end" and "run" in spans:
            s = spans["run"]
            s.end = ts
            s.attrs[f"{obs}output"] = _js({"answer": _resolve(run_dir, d.get("output")),
                                           "status": d.get("status"), "error": d.get("error"),
                                           "complete": d.get("complete"),
                                           "synthetic": d.get("synthetic"),
                                           "telemetry_errors": d.get("telemetry_errors")})
            if d.get("status") != "completed":
                s.error = d.get("error") or d.get("status") or "not completed"
            elif not d.get("complete"):
                s.level = "WARNING"
        elif kind == "loop.start":
            spans[f"agent:{span}"] = _Span(f"agent:{span}", span, parent,
                                           f"agent:{d.get('role_id') or e.get('agent_id')}", ts, attrs={
                f"{obs}type": "agent", **meta,
                f"{obs}metadata.config": _js(d.get("config") or {}),
            })
        elif kind in ("loop.end", "loop.cancelled") and f"agent:{span}" in spans:
            s = spans[f"agent:{span}"]
            s.end = ts
            s.attrs[f"{obs}output"] = _js(_resolve(run_dir, d.get("final_content")) or "")
            s.attrs[f"{obs}metadata.stopped_by"] = d.get("stopped_by") or kind
            s.attrs[f"{obs}metadata.turns"] = str(d.get("turns", ""))
            if kind == "loop.cancelled":
                s.level = "WARNING"
        elif kind == "llm.input":
            msgs = [json.loads(blob_text(run_dir, dg)) for dg in d.get("messages", [])[-_SHOW_LAST_MESSAGES:]]
            key = f"gen:{span}:{turn}"
            spans[key] = _Span(key, _hex16("gen", span, turn), span, f"llm turn {turn}", ts, attrs={
                f"{obs}type": "generation", **meta,
                f"{obs}input": _js(msgs),
                f"{obs}metadata.message_count": str(d.get("message_count", "")),
                f"{obs}metadata.shown_last_messages": str(len(msgs)),
            })
        elif kind == "llm.first_token" and f"gen:{span}:{turn}" in spans:
            # JSON-encoded, as the Langfuse SDKs send it.
            spans[f"gen:{span}:{turn}"].attrs[f"{obs}completion_start_time"] = json.dumps(ts)
        elif kind == "llm.attempt" and f"gen:{span}:{turn}" in spans:
            s = spans[f"gen:{span}:{turn}"]
            outcome = d.get("outcome") or ""
            if outcome:
                attempts = json.loads(s.attrs.get(f"{obs}metadata.attempts", "[]"))
                attempts.append({k: d.get(k) for k in (
                    "attempt_index", "outcome", "reason", "error_type", "duration_ms",
                    "ttft_ms", "finish_reason", "recovery_action")})
                s.attrs[f"{obs}metadata.attempts"] = json.dumps(attempts, ensure_ascii=False)
            if outcome in ("accepted_degraded", "discarded", "failed"):
                s.level = "WARNING"
        elif kind == "llm.response" and f"gen:{span}:{turn}" in spans:
            s = spans[f"gen:{span}:{turn}"]
            s.end = ts
            usage = d.get("usage") or {}
            model = d.get("model") or usage.get("model") or os.getenv("OPENAI_MODEL", "")
            s.attrs[f"{obs}model.name"] = model
            s.attrs[f"{obs}output"] = _js({
                "text": _resolve(run_dir, d.get("text")),
                "thinking": _resolve(run_dir, d.get("thinking")),
                "tool_calls": d.get("tool_calls"),
            })
            details = {k: int(v) for k, v in {
                "input": usage.get("prompt_tokens") or usage.get("input_tokens"),
                "output": usage.get("completion_tokens") or usage.get("output_tokens"),
                "input_cache_read": usage.get("cache_read_tokens") or usage.get("cached_tokens"),
                "output_reasoning": usage.get("reasoning_tokens"),
            }.items() if isinstance(v, (int, float)) and v}
            if details:
                s.attrs[f"{obs}usage_details"] = json.dumps(details)
        elif kind == "tool.call":
            spans[f"tool:{span}"] = _Span(f"tool:{span}", span, parent, f"tool:{d.get('name')}", ts, attrs={
                f"{obs}type": "tool", **meta, f"{obs}input": _js(d.get("args")),
            })
        elif kind == "tool.result":
            key = f"tool:{span}"
            s = spans.get(key) or _Span(key, span, parent, f"tool:{d.get('name')}", ts,
                                         attrs={f"{obs}type": "tool", **meta})
            spans[key] = s
            s.end = ts
            s.hold = True
            s.attrs[f"{obs}output"] = _js(_resolve(run_dir, d.get("result")) or "")
            s.attrs[f"{obs}metadata.duration_ms"] = str(d.get("duration_ms", ""))
            if d.get("is_error"):
                s.error = d.get("error_kind") or "tool error"
        elif kind == "tool.result.final" and f"tool:{span}" in spans:
            s = spans[f"tool:{span}"]
            s.attrs[f"{obs}metadata.raw_output"] = s.attrs.get(f"{obs}output", "")
            s.attrs[f"{obs}output"] = _js(_resolve(run_dir, d.get("result")) or "")
        elif kind == "deliverable":
            key = f"deliverable:{e['entry_id']}"
            preview = ""
            if d.get("sha256") and str(d.get("media_type", "")).startswith(("text/", "application/json")):
                preview = blob_text(run_dir, d["sha256"])
            spans[key] = _Span(key, _hex16("deliverable", e["entry_id"]), span,
                               f"deliverable:{d.get('path')}", ts, end=ts, attrs={
                f"{obs}type": "event",
                f"{obs}output": _js(preview or {k: d.get(k) for k in ("path", "bytes", "sha256")}),
                f"{obs}metadata.path": d.get("path") or "",
                f"{obs}metadata.bytes": str(d.get("bytes", "")),
                f"{obs}metadata.sha256": d.get("sha256") or "",
            }, error=d.get("error") or "")
        elif kind in ("observer.intervention", "context.compact", "context.compacted"):
            key = f"event:{e['entry_id']}"
            name = f"intervention:{d.get('observer')}" if kind == "observer.intervention" else kind
            spans[key] = _Span(key, _hex16("event", e["entry_id"]), span, name, ts, end=ts, attrs={
                f"{obs}type": "event", **meta,
                f"{obs}metadata.detail": _js({k: _resolve(run_dir, v) for k, v in d.items()}),
            }, level="WARNING" if kind == "observer.intervention" else "")



def build_spans(run_dir: Path, entries: list[dict[str, Any]], *, final: bool) -> tuple[str, list[_Span]]:
    """Pair all *entries* into spans. Returns ``(trace_id, spans)``."""
    builder = SpanBuilder(run_dir)
    builder.feed(entries)
    return builder.trace_id, builder.take(set(), final=final)


def _otlp(trace_id: str, spans: list[_Span]) -> dict[str, Any]:
    def attr(k: str, v: Any) -> dict[str, Any]:
        return {"key": k, "value": {"stringValue": v if isinstance(v, str) else json.dumps(v)}}

    out = []
    for s in spans:
        attrs = dict(s.attrs)
        if s.level or s.error:
            attrs["langfuse.observation.level"] = "ERROR" if s.error else s.level
        if s.error:
            attrs["langfuse.observation.status_message"] = s.error
        span: dict[str, Any] = {
            "traceId": trace_id,
            "spanId": s.span_id,
            "name": s.name,
            "kind": 1,
            "startTimeUnixNano": _ns(s.start),
            "endTimeUnixNano": _ns(s.end or s.start),
            "attributes": [attr(k, v) for k, v in attrs.items() if v not in (None, "")],
            "status": {"code": 2, "message": s.error} if s.error else {"code": 1},
        }
        if s.parent:
            span["parentSpanId"] = s.parent
        out.append(span)
    return {"resourceSpans": [{
        "resource": {"attributes": [
            {"key": "service.name", "value": {"stringValue": "frontier-agent"}},
        ]},
        "scopeSpans": [{"scope": {"name": "frontier_agent.telemetry"}, "spans": out}],
    }]}


# -- sending -------------------------------------------------------------------

class ExportError(RuntimeError):
    pass


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=os.environ["LANGFUSE_HOST"].rstrip("/"),
        auth=(os.environ["LANGFUSE_PUBLIC_KEY"], os.environ["LANGFUSE_SECRET_KEY"]),
        headers={"x-langfuse-ingestion-version": "4"},
        timeout=30,
    )


async def _post(client: httpx.AsyncClient, trace_id: str, spans: list[_Span]) -> None:
    resp = await client.post("/api/public/otel/v1/traces", json=_otlp(trace_id, spans))
    if resp.status_code >= 300:
        raise ExportError(f"OTLP HTTP {resp.status_code}: {resp.text[:300]}")
    body = resp.json() if resp.content else {}
    rejected = (body.get("partialSuccess") or {}).get("rejectedSpans")
    if rejected:
        raise ExportError(f"OTLP rejected {rejected} spans: {json.dumps(body)[:300]}")


class Exporter:
    """Incremental exporter for one run: reads only new journal entries.

    Spans are recorded as in-flight before each POST; after a lost response
    the next rounds ask Langfuse which of them arrived. Langfuse ingests
    asynchronously, so an unconfirmed span is only resent after
    ``INFLIGHT_GRACE_S`` (Langfuse v4 keeps duplicates); a failing check never
    blocks the rest of the export.
    """

    def __init__(self, run_dir: Path, session_id: str, run_id: str) -> None:
        self.run_dir, self.session_id, self.run_id = run_dir, session_id, run_id
        self.builder = SpanBuilder(run_dir)
        self.cursor = 0
        self.pending = False        # spans awaiting confirmation remain

    def _advance(self) -> None:
        entries = read_entries(self.run_dir, self.session_id, self.run_id, self.cursor)
        if entries:
            self.builder.feed(entries)
            self.cursor = entries[-1]["sequence"]

    async def step(self, *, final: bool = False, wait_lock: bool = False, resend: bool = False) -> int:
        """Send what is ready. Returns spans sent (0 when another export holds the lock)."""
        async with export_lock(self.run_dir, wait=wait_lock) as locked:
            if not locked:
                return 0
            await asyncio.to_thread(self._advance)
            sent, inflight = (set(), {}) if resend else load_state(self.run_dir, self.run_id)
            now = time.time()
            async with _client() as client:
                if inflight:
                    inflight = await self._reconcile(client, sent, inflight, now)
                    save_state(self.run_dir, self.run_id, sent, inflight)
                todo = self.builder.take(sent | set(inflight), final=final)
                for i in range(0, len(todo), _BATCH):
                    chunk = todo[i:i + _BATCH]
                    batch = {s.key: [s.span_id, now] for s in chunk}
                    inflight.update(batch)
                    save_state(self.run_dir, self.run_id, sent, inflight)
                    await _post(client, self.builder.trace_id, chunk)
                    sent.update(batch)
                    for k in batch:
                        inflight.pop(k, None)
                    save_state(self.run_dir, self.run_id, sent, inflight)
            self.pending = bool(inflight)
            return len(todo)

    async def _reconcile(self, client: httpx.AsyncClient, sent: set[str],
                         inflight: dict[str, list[Any]], now: float) -> dict[str, list[Any]]:
        """Confirmed spans move to *sent*; unconfirmed ones wait out the grace
        period, then are dropped from in-flight so they are resent."""
        try:
            arrived: set[str] | None = await _existing_ids(client, self.builder.trace_id)
        except Exception as exc:
            logger.warning("telemetry: in-flight check for %s failed: %s", self.run_id, exc)
            arrived = None
        keep: dict[str, list[Any]] = {}
        for key, (span_id, first) in inflight.items():
            if arrived is not None and span_id in arrived:
                sent.add(key)
            elif now - float(first or 0) < INFLIGHT_GRACE_S * (1 if arrived is not None else 5):
                keep[key] = [span_id, first]
        return keep


async def _existing_ids(client: httpx.AsyncClient, trace_id: str) -> set[str]:
    ids: set[str] = set()
    cursor = None
    while True:
        params: dict[str, Any] = {"traceId": trace_id, "limit": 1000, "fields": "core"}
        if cursor:
            params["cursor"] = cursor
        resp = await client.get("/api/public/v2/observations", params=params)
        resp.raise_for_status()
        body = resp.json()
        ids.update(o["id"] for o in body.get("data", []))
        cursor = (body.get("meta") or {}).get("cursor")
        if not cursor:
            return ids


async def export_run(
    run_dir: Path, session_id: str, run_id: str, *, final: bool = False, resend: bool = False,
) -> int:
    """One-shot export of a run (CLI, replay): waits for the export lock."""
    return await Exporter(run_dir, session_id, run_id).step(final=final, wait_lock=True, resend=resend)


def expected_observation_ids(run_dir: Path, session_id: str, run_id: str) -> tuple[str, set[str]]:
    entries = read_entries(run_dir, session_id, run_id)
    trace_id, spans = build_spans(run_dir, entries, final=True)
    return trace_id, {s.span_id for s in spans}


class LiveExporter:
    """Background exporter for a running run; failures only delay export."""

    def __init__(self, run: Any) -> None:
        self.run = run
        self._exporter: Exporter | None = None
        self._stop = asyncio.Event()
        self._task = asyncio.create_task(self._loop())

    @classmethod
    def maybe_start(cls, run: Any) -> LiveExporter | None:
        return cls(run) if configured() else None

    async def _once(self, final: bool = False) -> None:
        if self._exporter is None:
            self._exporter = Exporter(self.run.run_dir, self.run.session_id, self.run.run_id)
        await self._exporter.step(final=final, wait_lock=final)

    async def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self._once()
            except Exception as exc:
                logger.warning("telemetry: Langfuse export deferred: %s", exc)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop.wait(), _LIVE_INTERVAL_S)

    async def finish(self) -> None:
        self._stop.set()
        with contextlib.suppress(Exception):
            await self._task
        try:
            await self._once(final=True)
        except Exception as exc:
            logger.warning(
                "telemetry: final Langfuse export failed (%s); replay with "
                "`python -m frontier_agent.telemetry export %s`", exc, self.run.run_dir,
            )
