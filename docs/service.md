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
| `SERVICE_API_TOKEN` | — | required `Authorization: Bearer <token>`; the app refuses to start without it unless bound to loopback via `python -m agent_service` or `SERVICE_ALLOW_NO_AUTH=1` |
| `SERVICE_DATA_DIR` | `$XDG_DATA_HOME/frontier-agent/service` (`~/.local/share/...`) | task db and per-task working directories; keep it outside any project tree |
| `SERVICE_SHUTDOWN_GRACE_S` | `25` | total time to stop running workers on shutdown (keep below the orchestrator's grace period) |
| `SERVICE_MAX_CONCURRENCY` | `2` | tasks running at once; the rest queue |
| `SERVICE_TASK_TIMEOUT_S` | `7200` | hard limit per task |
| `SERVICE_CANCEL_GRACE_S` | `60` | wait after SIGINT before SIGTERM/SIGKILL |
| `SERVICE_DEFAULT_MODE` | `react` | `react`, `agent_team`, `research`, `coding` |
| `SERVICE_MAX_TASK_CHARS` | `20000` | reject longer task text |
| `SERVICE_ALLOW_FAKE_IP` | off | local development only: pass `FRONTIER_AGENT_ALLOW_FAKE_IP_CIDRS` to workers; ignored unless bound to loopback |

The service refuses to start on a non-loopback address without
`SERVICE_API_TOKEN`: workers auto-approve every tool. `create_app()` enforces
the same rule when embedded in another ASGI server, and an app factory never
enables the fake-IP passthrough (its bind address is unknown).

The model, search and Langfuse settings come from the same `.env` as the CLI
(loaded at start). Workers receive an **allowlisted** environment — model,
search and runtime variables only. `SERVICE_API_TOKEN` and `LANGFUSE_*` never
reach a worker; Langfuse export runs in the service process, reading each
task's journal. `FRONTIER_TELEMETRY` is forced on for workers because task
results are read from the journal. Workers also get `APODEX_NO_DOTENV=1` and
`APODEX_ENV_FILE=/dev/null`, so the CLI does not re-load a project or user
`.env` and undo the allowlist.

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
`cancelled`, `timed_out`. Non-completed tasks carry `error_code`:
`llm_error`, `incomplete`, `agent_error`, `worker_crashed`, `timeout`,
`cancelled` (by the caller), `service_shutdown` (safe to resubmit),
`service_restart`, `internal`. A run that completed wins over a
cancel that arrived late; final states are never rewritten. `request_id` is written into the run journal
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
| worker exits non-zero or without `run.end` | task `failed` (`worker_crashed`, or the run's own reason); journal gets a synthetic `run.end` (`crashed`) |
| agent ends with an LLM error or an incomplete answer | CLI exits non-zero; task `failed` with `llm_error` / `incomplete` |
| timeout | SIGINT → grace → SIGTERM → SIGKILL; task `timed_out` |
| service shutdown | running workers get SIGINT and are awaited within `SERVICE_SHUTDOWN_GRACE_S`; tasks end `cancelled` / `service_shutdown`; open SSE streams are closed |
| service restarts | a task whose journal shows it completed stays `completed`; other rows left `running` become `failed` (`service_restart`). A leftover worker is signalled only if its `--cwd` argument is exactly that task's workdir (read from `/proc`, else `ps -ww`); if identity cannot be verified it is left alone and a warning is logged. `queued` rows run again |
| Langfuse unavailable at task end | the final export is retried every 60s until it succeeds |
