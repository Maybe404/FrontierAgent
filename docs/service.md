# Agent service (HTTP API)

[Documentation index](README.md) · [Run journal](telemetry.md)

`agent_service` puts an HTTP API in front of FrontierAgent so a backend (for
example a Go gateway that owns auth, users and the frontend protocol) can
submit long-running agent tasks, stream their progress and fetch results.

## Architecture

```text
caller ──HTTP──> agent_service (FastAPI)
                   ├─ service.db          task table (SQLite)
                   ├─ Runner              queue + max concurrency + timeout + cancel
                   └─ one worker process per task:
                        frontier-agent --native --print --yes --mode <mode> --cwd tasks/<id>
                          └─ tasks/<id>/.apodex/runs/<session>/journal.db   (run journal)
```

- Each task runs in its own process: process-wide runtime settings never mix,
  a crash affects one task, and cancel can always stop it.
- `agent_service/engine.py` is the only module that knows how a task is run
  and where its journal lives — the adapter to upstream changes.
- Progress events are read from the run journal, so the SSE stream, the local
  logs and Langfuse show the same data.

## Run

```bash
uv sync --extra service
SERVICE_API_TOKEN=... uv run python -m agent_service --host 127.0.0.1 --port 8800
```

| Variable | Default | Meaning |
|---|---|---|
| `SERVICE_API_TOKEN` | empty (no auth) | required `Authorization: Bearer <token>` |
| `SERVICE_DATA_DIR` | `./.service` | task db and per-task working directories |
| `SERVICE_MAX_CONCURRENCY` | `2` | tasks running at once; the rest queue |
| `SERVICE_TASK_TIMEOUT_S` | `7200` | hard limit per task |
| `SERVICE_CANCEL_GRACE_S` | `60` | wait after SIGINT before SIGTERM/SIGKILL |
| `SERVICE_DEFAULT_MODE` | `react` | `react`, `agent_team`, `research`, `coding` |
| `SERVICE_MAX_TASK_CHARS` | `20000` | reject longer task text |

The model, search and Langfuse settings come from the same `.env` as the CLI
(loaded at start; workers inherit it).

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/tasks` | `{"task", "mode"?, "request_id"?, "max_turns"?}` → `202`. Same `request_id` returns the existing task (`created: false`). |
| `GET` | `/v1/tasks/{id}` | status, `answer`, `error`, `complete`, `deliverables` |
| `GET` | `/v1/tasks` | recent tasks, `?status=` filter |
| `GET` | `/v1/tasks/{id}/events` | Server-Sent Events. `id:` is a cursor; reconnect with `Last-Event-ID` (or `?after=`) to resume without gaps. Ends with `event: task.end`. |
| `GET` | `/v1/tasks/{id}/files/{path}` | download a deliverable from the task's outputs |
| `POST` | `/v1/tasks/{id}/cancel` | queued → `cancelled`; running → `cancelling` → `cancelled` |
| `GET` | `/healthz` | liveness |

Status values: `queued`, `running`, `cancelling`, `completed`, `failed`,
`cancelled`. `request_id` is written into the run journal
(`run.start.env.FRONTIER_REQUEST_ID`), so a gateway log line and an agent trace
can be joined on it.

SSE event `kind`s are the journal event types (`run.start`, `loop.start`,
`llm.response`, `tool.call`, `tool.result`, `observer.intervention`,
`deliverable`, `run.end`, …; see [telemetry.md](telemetry.md)). Text fields
are redacted and capped at 2,000 characters; the full content stays in the
journal.

## Failure handling

| Situation | Result |
|---|---|
| worker exits non-zero or without `run.end` | task `failed`; journal gets a synthetic `run.end` (`crashed`) |
| timeout | SIGINT → grace → SIGTERM → SIGKILL; task `cancelled` with a timeout error |
| service restarts | rows left `running` become `failed` (orphan workers are stopped); `queued` rows run again |
