# Run journal and Langfuse export

[Documentation index](README.md) · [Run artifacts](run-artifacts.md)

Every task run through `frontier-agent` is journaled by
`frontier_agent/telemetry`. The journal is the source of truth for debugging,
evaluation and incident review; Langfuse is an optional, rebuildable view.

## What is recorded

One `TelemetryObserver` is attached to every agent loop — main agents through
the loop adapter (`frontier_agent/core/runtime/loop/agent_loop.py`) and
sub-agents through the AgentBus composition
(`frontier_agent/components/agent_bus/bus.py`) — so workflows need no changes.
Each existing observer is wrapped so the interventions it returns are
journaled too.

Runs are opened by every entry point:

| Entry point | Journal location |
|---|---|
| `frontier-agent` CLI / TUI | `<cwd>/.apodex/runs/<session-id>/` |
| `agent_service` HTTP API | `<SERVICE_DATA_DIR>/tasks/<task-id>/.apodex/runs/<session-id>/` |
| benchmarks (Harbor) and the web demo (`BenchmarkSession` with `_trial_dir`) | `<trial_dir>/journal/` |

On exit, the top-level agent's final answer is written to `run.end` and every
file in the outputs directory is journaled as a `deliverable` (path, size,
sha256, content up to 50 MB).

| Event | Content |
|---|---|
| `run.start` / `run.end` | task, versions and git commit, deployment env, status, `complete` flag |
| `loop.start` / `loop.end` / `loop.cancelled` | role, loop config, stop reason, turns, final content |
| `llm.input` | the exact messages sent, each stored once by content hash |
| `llm.attempt` / `llm.first_token` / `llm.response` | every attempt with outcome, latency, TTFT, usage; text, reasoning, tool calls |
| `tool.call` / `tool.result` | arguments, full result, duration, error |
| `observer.intervention` | which observer stopped, rolled back or nudged the loop, on which turn |
| `context.compact` / `context.compacted` | compaction before/after sizes and summary |

Events carry a per-run gap-free `seq`, UTC timestamp, `trace_id`, `span_id` and
`parent_span_id` (sub-agents point at the agent that spawned them).

## Storage

```text
<cwd>/.apodex/runs/<session-id>/
├── journal.db     # AgentCore append-only SQLite journal (WAL, one transaction per event)
├── blobs/         # content-addressed, fsynced, read-only large fields and messages
└── export.state   # spans already sent to Langfuse
```

Journal writes never raise into the agent. A failed write is retried, then
counted; `run.end` reports `complete: false` with the lost sequence numbers.
Set `FRONTIER_TELEMETRY=0` to disable journaling.

## Commands

```bash
python -m frontier_agent.telemetry runs   <run_dir>   # runs in a session directory
python -m frontier_agent.telemetry show   <run_dir>   # event timeline (--full for data)
python -m frontier_agent.telemetry check  <run_dir>   # seq gaps, missing run.end, lost writes
python -m frontier_agent.telemetry export <run_dir>   # send unsent spans to Langfuse
python -m frontier_agent.telemetry verify <run_dir>   # reconcile journal vs Langfuse
python -m frontier_agent.telemetry recover <run_dir>  # close a run left open by a dead process
python -m frontier_agent.telemetry archive <runs_root> --archive-root DIR --older-than-days 7
python -m frontier_agent.telemetry prune   <runs_root> --archive-root DIR --older-than-days 30 [--max-gb 50] [--dry-run]
```

A run whose process died is closed automatically (synthetic `run.end`,
status `crashed`) the next time a run starts in that directory. `archive`
writes checksummed `.tar.gz` files through `ArchiveStore` (local today; an
object-storage backend implements the same two methods). `prune` deletes the
oldest closed runs by age or total size, and only runs already archived
unless `--force`.

## Redaction

Exported text (Langfuse, API event stream) is masked: values of env vars named
like `*KEY*`/`*TOKEN*`/`*SECRET*`/`*PASSWORD*`, API keys, bearer tokens, JWTs,
private keys, emails, CN mobile and ID numbers. The local journal keeps the
original. `FRONTIER_TELEMETRY_REDACT=0` disables masking.

## Langfuse

Set `LANGFUSE_HOST`, `LANGFUSE_PUBLIC_KEY` and `LANGFUSE_SECRET_KEY` (a local
stack is in [`deploy/langfuse`](../deploy/langfuse/README.md)). A background
exporter then sends completed spans over OTLP/HTTP every two seconds and once
more when the run ends. Export failures only delay export; rerun `export` to
catch up. Spans still open when a run ends abnormally are sent marked
`unfinished`.

Langfuse v4 does not deduplicate re-sent spans, so `export.state` records what
was sent and `export --resend` is for debugging only. `verify` compares unique
observation ids.
