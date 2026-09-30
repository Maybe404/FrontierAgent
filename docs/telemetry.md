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
```

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
