"""HTTP API for the Go backend.

    POST /v1/tasks                      submit (idempotent on request_id) -> 202
    GET  /v1/tasks                      list recent tasks
    GET  /v1/tasks/{id}                 status, answer, deliverables
    GET  /v1/tasks/{id}/events          SSE progress; resumable via Last-Event-ID / ?after=
    GET  /v1/tasks/{id}/files/{path}    download a deliverable
    POST /v1/tasks/{id}/cancel          cooperative cancel, then terminate
    GET  /healthz                       liveness

Progress events are read from the task's run journal, so the stream, the
logs and Langfuse all show the same data.
"""

from __future__ import annotations

import asyncio
import json
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

from agent_service import engine
from agent_service.config import ServiceConfig
from agent_service.runner import Runner
from agent_service.store import FINAL, TaskStore
from frontier_agent.telemetry.redact import redact

_SSE_POLL_S = 0.5
_SSE_KEEPALIVE_S = 15.0
_EVENT_TEXT_CAP = 2000


class SubmitRequest(BaseModel):
    task: str = Field(min_length=1)
    mode: str | None = None
    request_id: str | None = Field(default=None, max_length=200)
    max_turns: int | None = Field(default=None, ge=1, le=1000)


def _public(row: dict[str, Any]) -> dict[str, Any]:
    keys = ("id", "request_id", "mode", "status", "created_at", "started_at", "finished_at",
            "answer", "error", "error_code", "complete", "deliverables", "exit_code")
    return {k: row.get(k) for k in keys}


def _trim(value: Any) -> Any:
    if isinstance(value, str):
        value = redact(value)
        return value if len(value) <= _EVENT_TEXT_CAP else value[:_EVENT_TEXT_CAP] + "…"
    if isinstance(value, dict):
        if {"blob", "bytes"} <= set(value):
            return {"bytes": value["bytes"], "preview": _trim(value.get("preview", ""))}
        return {k: _trim(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_trim(v) for v in value[:50]]
    return value


def _sse_event(e: dict[str, Any]) -> str:
    data = e.get("data") or {}
    if e["kind"] == "llm.input":
        data = {"message_count": data.get("message_count")}
    body = {"seq": e.get("seq"), "kind": e["kind"], "ts": e.get("ts"), "agent_id": e.get("agent_id"),
            "turn": e.get("turn"), "span_id": e.get("span_id"),
            "parent_span_id": e.get("parent_span_id"), "data": _trim(data)}
    return f"id: {e['cursor']}\nevent: {e['kind']}\ndata: {json.dumps(body, ensure_ascii=False)}\n\n"


def create_app(cfg: ServiceConfig | None = None) -> FastAPI:
    cfg = cfg or ServiceConfig.from_env()
    store = TaskStore(cfg.db_path)
    runner = Runner(store, cfg)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        await runner.start()
        try:
            yield
        finally:
            await runner.stop()

    app = FastAPI(title="FrontierAgent service", version="0.1.0", lifespan=lifespan)
    app.state.store, app.state.runner, app.state.cfg = store, runner, cfg

    def auth(authorization: str = Header(default="")) -> None:
        if not cfg.api_token:
            return
        expected = f"Bearer {cfg.api_token}"
        if not secrets.compare_digest(authorization.encode(), expected.encode()):
            raise HTTPException(401, "invalid or missing bearer token")

    def _task(task_id: str) -> dict[str, Any]:
        row = store.get(task_id)
        if row is None:
            raise HTTPException(404, "task not found")
        return row

    @app.get("/healthz")
    def healthz() -> dict[str, Any]:
        return {"status": "ok"}

    @app.post("/v1/tasks", status_code=202, dependencies=[Depends(auth)])
    def submit(req: SubmitRequest) -> dict[str, Any]:
        mode = req.mode or cfg.default_mode
        if mode not in engine.MODES:
            raise HTTPException(422, f"mode must be one of {list(engine.MODES)}")
        if len(req.task) > cfg.max_task_chars:
            raise HTTPException(413, f"task longer than {cfg.max_task_chars} characters")
        row, created = store.create(task=req.task, mode=mode, request_id=req.request_id,
                                    max_turns=req.max_turns, workdir_root=cfg.tasks_root)
        if created:
            runner.submit(row["id"])
        return {**_public(row), "created": created}

    @app.get("/v1/tasks", dependencies=[Depends(auth)])
    def list_tasks(status: str | None = None, limit: int = 50) -> dict[str, Any]:
        return {"data": [_public(r) for r in store.list(status=status, limit=min(limit, 500))]}

    @app.get("/v1/tasks/{task_id}", dependencies=[Depends(auth)])
    def get_task(task_id: str) -> dict[str, Any]:
        return _public(_task(task_id))

    @app.post("/v1/tasks/{task_id}/cancel", dependencies=[Depends(auth)])
    async def cancel(task_id: str) -> dict[str, Any]:
        _task(task_id)
        row = await runner.cancel(task_id)
        return _public(row or _task(task_id))

    @app.get("/v1/tasks/{task_id}/files/{path:path}", dependencies=[Depends(auth)])
    def download(task_id: str, path: str) -> FileResponse:
        out = engine.outputs_dir(Path(_task(task_id)["workdir"]))
        if out is None:
            raise HTTPException(404, "no outputs yet")
        root = out.resolve()
        target = (root / path).resolve()
        if not target.is_relative_to(root) or not target.is_file():
            raise HTTPException(404, "file not found")
        return FileResponse(target, filename=target.name)

    @app.get("/v1/tasks/{task_id}/events", dependencies=[Depends(auth)])
    async def events(task_id: str, request: Request, after: int = 0,
                     last_event_id: str | None = Header(default=None)) -> StreamingResponse:
        _task(task_id)
        cursor = int(last_event_id) if last_event_id and last_event_id.isdigit() else after

        async def stream() -> AsyncIterator[str]:
            nonlocal cursor
            idle = 0.0
            while not await request.is_disconnected():
                row = store.get(task_id) or {}
                batch = await asyncio.to_thread(engine.read_events, Path(row["workdir"]), cursor)
                for e in batch:
                    cursor = e["cursor"]
                    yield _sse_event(e)
                if batch:
                    idle = 0.0
                    continue
                if row.get("status") in FINAL:
                    yield f"event: task.end\ndata: {json.dumps(_public(row), ensure_ascii=False)}\n\n"
                    return
                await asyncio.sleep(_SSE_POLL_S)
                idle += _SSE_POLL_S
                if idle >= _SSE_KEEPALIVE_S:
                    idle = 0.0
                    yield ": keepalive\n\n"

        return StreamingResponse(stream(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    return app
