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
import hashlib
import json
import logging
import os
import sqlite3
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


def load_sent(run_dir: Path, run_id: str) -> set[str]:
    try:
        return set(json.loads(_state_path(run_dir).read_text()).get(run_id, []))
    except (OSError, ValueError):
        return set()


def save_sent(run_dir: Path, run_id: str, keys: set[str]) -> None:
    path = _state_path(run_dir)
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        data = {}
    data[run_id] = sorted(keys)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data))
    os.replace(tmp, path)


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


def build_spans(run_dir: Path, entries: list[dict[str, Any]], *, final: bool) -> tuple[str, list[_Span]]:
    """Pair journal events into spans. Returns ``(trace_id, spans)``."""
    trace_id = entries[0]["trace_id"] if entries else ""
    spans: dict[str, _Span] = {}
    last_ts = entries[-1]["ts"] if entries else ""
    obs = "langfuse.observation."

    for e in entries:
        kind, d, ts = e["kind"], e.get("data") or {}, e["ts"]
        span, parent, turn = str(e.get("span_id") or ""), e.get("parent_span_id"), e.get("turn")
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
            s.attrs[f"{obs}output"] = _js(_resolve(run_dir, d.get("result")) or "")
            s.attrs[f"{obs}metadata.duration_ms"] = str(d.get("duration_ms", ""))
            if d.get("is_error"):
                s.error = d.get("error_kind") or "tool error"
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

    done = [s for s in spans.values() if s.end]
    if final:
        for s in spans.values():
            if not s.end:
                s.end = last_ts
                s.level = "WARNING"
                s.attrs[f"{obs}metadata.unfinished"] = "true"
                done.append(s)
    return trace_id, done


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


async def export_run(
    run_dir: Path, session_id: str, run_id: str, *, final: bool = False, resend: bool = False,
) -> int:
    """Send every completed, not-yet-sent span of one run. Returns spans sent."""
    entries = read_entries(run_dir, session_id, run_id)
    if not entries:
        return 0
    final = final or any(e["kind"] == "run.end" for e in entries)
    trace_id, spans = build_spans(run_dir, entries, final=final)
    sent = set() if resend else load_sent(run_dir, run_id)
    todo = [s for s in spans if s.key not in sent]
    async with _client() as client:
        for i in range(0, len(todo), _BATCH):
            chunk = todo[i:i + _BATCH]
            await _post(client, trace_id, chunk)
            sent.update(s.key for s in chunk)
            save_sent(run_dir, run_id, sent)
    return len(todo)


def expected_observation_ids(run_dir: Path, session_id: str, run_id: str) -> tuple[str, set[str]]:
    entries = read_entries(run_dir, session_id, run_id)
    trace_id, spans = build_spans(run_dir, entries, final=True)
    return trace_id, {s.span_id for s in spans}


class LiveExporter:
    """Background exporter for a running run; failures only delay export."""

    def __init__(self, run: Any) -> None:
        self.run = run
        self._stop = asyncio.Event()
        self._task = asyncio.create_task(self._loop())

    @classmethod
    def maybe_start(cls, run: Any) -> LiveExporter | None:
        return cls(run) if configured() else None

    async def _once(self, final: bool = False) -> None:
        await export_run(self.run.run_dir, self.run.session_id, self.run.run_id, final=final)

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
