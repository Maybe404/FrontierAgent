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
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi import Path as PathParam
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from agent_service import docs, engine
from agent_service.config import ServiceConfig
from agent_service.runner import Runner
from agent_service.schemas import (
    API_DESCRIPTION,
    OPENAPI_TAGS,
    ErrorResponse,
    Health,
    SubmitRequest,
    SubmitResponse,
    Task,
    TaskList,
    TaskStatus,
)
from agent_service.store import FINAL, TaskStore
from frontier_agent.telemetry.redact import redact

_SSE_POLL_S = 0.5
_SSE_KEEPALIVE_S = 15.0
_EVENT_TEXT_CAP = 2000

_bearer = HTTPBearer(auto_error=False, description="The service's `SERVICE_API_TOKEN`.")


def _err(description: str) -> dict[str, Any]:
    return {"model": ErrorResponse, "description": description}


_Responses = dict[int | str, dict[str, Any]]
_UNAUTHORIZED: _Responses = {401: _err("Missing or invalid bearer token.")}
_NOT_FOUND: _Responses = {404: _err("No task with this id.")}
TaskId = Annotated[str, PathParam(description="Task id returned by submit.")]

_EVENTS_DESCRIPTION = """\
Server-Sent Events stream of the task's progress, read from its run journal.

Each frame is
```
id: <cursor>
event: <kind>
data: {"seq", "kind", "ts", "agent_id", "turn", "span_id", "parent_span_id", "data"}
```
`kind` is the journal event kind (for example `run.start`, `llm.response`,
`tool.call`, `tool.result`, `deliverable`, `run.end`). Long strings in `data`
are redacted and truncated to 2000 characters.

When the task is final and all events were sent, one `task.end` frame carries
the task (same shape as `GET /v1/tasks/{task_id}`) and the stream closes.
Comment lines (`: keepalive`) are sent every 15 s while idle.

**Resuming:** reconnect with the `Last-Event-ID` header (browsers do this
automatically) or `?after=<cursor>` to receive only later events.
"""


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

    app = FastAPI(title="FrontierAgent service", version="0.1.0", lifespan=lifespan,
                  description=API_DESCRIPTION, openapi_tags=OPENAPI_TAGS, docs_url=None, redoc_url=None)
    app.state.store, app.state.runner, app.state.cfg = store, runner, cfg

    def auth(credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)]) -> None:
        if not cfg.api_token:
            return
        given = credentials.credentials if credentials else ""
        if not secrets.compare_digest(given.encode(), cfg.api_token.encode()):
            raise HTTPException(401, "invalid or missing bearer token")

    def _task(task_id: str) -> dict[str, Any]:
        row = store.get(task_id)
        if row is None:
            raise HTTPException(404, "task not found")
        return row

    @app.get("/healthz", response_model=Health, tags=["system"], summary="Liveness check")
    def healthz() -> dict[str, Any]:
        """Returns 200 while the service process is up. Needs no token."""
        return {"status": "ok"}

    @app.post("/v1/tasks", status_code=202, dependencies=[Depends(auth)], response_model=SubmitResponse,
              tags=["tasks"], summary="Submit a task",
              responses={**_UNAUTHORIZED, 413: _err("`task` is longer than the service limit."),
                         422: {"description": "Invalid body, or unknown `mode`."}})
    def submit(req: SubmitRequest) -> dict[str, Any]:
        """Queue a task and return immediately with status `queued`.

        Idempotent on `request_id`: a repeated submit returns the existing
        task with `created: false` and does not start it again.
        """
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

    @app.get("/v1/tasks", dependencies=[Depends(auth)], response_model=TaskList, tags=["tasks"],
             summary="List tasks", responses=_UNAUTHORIZED)
    def list_tasks(
        status: Annotated[TaskStatus | None, Query(description="Only tasks in this status.")] = None,
        limit: Annotated[int, Query(description="Maximum number of tasks to return; capped at 500.")] = 50,
    ) -> dict[str, Any]:
        """Most recently submitted tasks first."""
        return {"data": [_public(r) for r in store.list(status=status, limit=min(limit, 500))]}

    @app.get("/v1/tasks/{task_id}", dependencies=[Depends(auth)], response_model=Task, tags=["tasks"],
             summary="Get a task", responses={**_UNAUTHORIZED, **_NOT_FOUND})
    def get_task(task_id: TaskId) -> dict[str, Any]:
        """Status of the task; answer, error and deliverables once it is final."""
        return _public(_task(task_id))

    @app.post("/v1/tasks/{task_id}/cancel", dependencies=[Depends(auth)], response_model=Task,
              tags=["tasks"], summary="Cancel a task", responses={**_UNAUTHORIZED, **_NOT_FOUND})
    async def cancel(task_id: TaskId) -> dict[str, Any]:
        """Ask the worker to stop, then terminate it if it does not exit in time.

        A queued task becomes `cancelled` at once. A running task becomes
        `cancelling`, then `cancelled` when its worker exits. A final task is
        returned unchanged.
        """
        _task(task_id)
        row = await runner.cancel(task_id)
        return _public(row or _task(task_id))

    @app.get("/v1/tasks/{task_id}/files/{path:path}", dependencies=[Depends(auth)], tags=["results"],
             summary="Download a produced file", response_class=FileResponse,
             responses={200: {"description": "The file content.",
                              "content": {"application/octet-stream": {"schema": {"type": "string",
                                                                                  "format": "binary"}}}},
                        **_UNAUTHORIZED,
                        404: _err("No such task, the task has no outputs yet, or no such file.")})
    def download(task_id: TaskId,
                 path: Annotated[str, PathParam(description="A deliverable's `path`, as listed on the task.")],
                 ) -> FileResponse:
        """Download a file from the task's outputs directory."""
        out = engine.outputs_dir(Path(_task(task_id)["workdir"]))
        if out is None:
            raise HTTPException(404, "no outputs yet")
        root = out.resolve()
        target = (root / path).resolve()
        if not target.is_relative_to(root) or not target.is_file():
            raise HTTPException(404, "file not found")
        return FileResponse(target, filename=target.name)

    @app.get("/v1/tasks/{task_id}/events", dependencies=[Depends(auth)], tags=["results"],
             summary="Stream progress events", description=_EVENTS_DESCRIPTION, response_class=StreamingResponse,
             responses={200: {"description": "Event stream.",
                              "content": {"text/event-stream": {"schema": {"type": "string"}}}},
                        **_UNAUTHORIZED, **_NOT_FOUND})
    async def events(request: Request, task_id: TaskId,
                     after: Annotated[int, Query(description="Send only events after this cursor.")] = 0,
                     last_event_id: Annotated[str | None, Header(
                         description="Cursor of the last event received; overrides `after`.")] = None,
                     ) -> StreamingResponse:
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

    docs.mount(app)
    return app
