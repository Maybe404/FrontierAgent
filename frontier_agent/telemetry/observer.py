"""TelemetryObserver: records every loop lifecycle event into the run journal.

Critical (awaited in order) so events are journaled before the loop moves
on; it never returns an intervention. One instance per agent loop.
"""

from __future__ import annotations

import contextlib
import functools
import inspect
import time
from collections.abc import Awaitable, Callable
from typing import Any, cast

from agent_core.loop_types import (
    AgentLoopResult,
    BaseObserver,
    CompactionEvent,
    ContextCompactionContext,
    Intervention,
    LLMAttemptContext,
    LLMDeltaContext,
    LoopConfig,
    ToolCallIntervention,
    ToolResult,
    TurnContext,
)

from frontier_agent.telemetry.run import (
    TelemetryRun,
    current_agent_span,
    new_span_id,
    to_json,
)

_CONFIG_FIELDS = (
    "max_turns", "max_tool_calls_per_turn", "tool_timeout", "llm_timeout",
    "context_token_limit", "compact_after_turns", "keep_recent",
    "max_llm_retries", "max_context_length", "max_completion_tokens",
    "task_id", "role_id",
)


def _guarded(fn: Any) -> Any:
    """Journal failures never reach the loop, but always reach the run's
    error count (so run.end reports complete=false)."""
    @functools.wraps(fn)
    async def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            return await fn(self, *args, **kwargs)
        except Exception as exc:
            self.run.note_failure(f"{type(self).__name__}.{fn.__name__}", exc)
            return None
    return wrapper


class TelemetryObserver(BaseObserver):
    critical = True

    def __init__(self, run: TelemetryRun, *, parent_span_id: str | None) -> None:
        self.run = run
        self.span_id = new_span_id()
        self.parent_span_id = parent_span_id
        self.agent_id = ""
        self._token = None
        self._first_token_seen: set[str] = set()
        self._tool_spans: dict[str, str] = {}
        self._tool_started: dict[str, float] = {}
        self._raw_results: dict[str, str] = {}
        self._result_spans: dict[str, str] = {}

    async def _emit(self, kind: str, data: dict[str, Any], *, turn: int | None = None,
                    span_id: str | None = None, parent: str | None = None, **kw: Any) -> None:
        await self.run.emit(
            kind, data, agent_id=self.agent_id, turn=turn,
            span_id=span_id or self.span_id,
            parent_span_id=parent if parent is not None else self.parent_span_id,
            **kw,
        )

    # -- loop ---------------------------------------------------------------

    async def on_loop_start(self, config: LoopConfig) -> None:
        # Sub-agents share the parent's task_id, so the span makes it unique.
        self.agent_id = f"{config.role_id or 'agent'}:{self.span_id[:8]}"
        # Sub-agents started from inside this loop inherit this span as parent.
        self._token = current_agent_span.set(self.span_id)
        await self._emit("loop.start", {
            "role_id": config.role_id,
            "task_id": config.task_id,
            "config": {k: getattr(config, k, None) for k in _CONFIG_FIELDS},
            "loop_policy": getattr(config, "loop_policy", None),
        })

    async def on_llm_input(self, ctx: TurnContext) -> None:
        # Each message is stored once by content hash, so the full prompt of
        # every turn is recoverable without re-storing the shared history.
        refs = {}
        digests = []
        for i, msg in enumerate(ctx.messages):
            ref = await self.run.put_blob(to_json(msg), "application/json")
            refs[f"m{i:05d}"] = ref
            digests.append(ref.digest)
        await self._emit("llm.input", {
            "message_count": len(ctx.messages),
            "messages": digests,
            "roles": [str(m.get("role", "")) for m in ctx.messages if isinstance(m, dict)],
        }, turn=ctx.turn, blobs=refs)

    async def on_llm_delta(self, ctx: LLMDeltaContext) -> Intervention | None:
        key = ctx.attempt_id or f"{ctx.turn}"
        if key not in self._first_token_seen:
            self._first_token_seen.add(key)
            await self._emit("llm.first_token", {
                "attempt_id": ctx.attempt_id, "call_id": ctx.call_id,
            }, turn=ctx.turn)
        return None

    async def on_llm_attempt(self, ctx: LLMAttemptContext) -> None:
        await self._emit("llm.attempt", {
            "call_id": ctx.call_id,
            "attempt_id": ctx.attempt_id,
            "attempt_index": ctx.attempt_index,
            "phase": ctx.phase,
            "outcome": ctx.outcome,
            "reason": ctx.reason,
            "recovery_action": ctx.recovery_action,
            "error_type": ctx.error_type,
            "duration_ms": ctx.duration_ms,
            "ttft_ms": ctx.ttft_ms,
            "finish_reason": ctx.finish_reason,
            "usage": dict(ctx.usage or {}),
            "visible_chars": ctx.visible_chars,
            "reasoning_chars": ctx.reasoning_chars,
            "tool_calls_count": ctx.tool_calls_count,
            "max_tokens": ctx.max_tokens,
            "thinking_mode": ctx.thinking_mode,
        }, turn=ctx.turn)

    async def on_llm_response(self, ctx: TurnContext) -> Intervention | None:
        await self._emit("llm.response", {
            "text": ctx.ai_text or "",
            "thinking": ctx.thinking or "",
            "leaked_reasoning": ctx.leaked_reasoning or "",
            "tool_calls": list(ctx.tool_calls or []),
            "blocked_tool_calls": list(ctx.blocked_tool_calls or []),
            "usage": dict(ctx.usage or {}),
            "model": (ctx.usage or {}).get("model", ""),
        }, turn=ctx.turn)
        return None

    async def on_tool_call(self, ctx: TurnContext, tool_call: dict[str, Any]) -> ToolCallIntervention | None:
        call_id = str(tool_call.get("id") or "")
        span = new_span_id()
        self._tool_spans[call_id] = span
        self._tool_started[call_id] = time.monotonic()
        name, args = _call_name_args(tool_call)
        await self._emit("tool.call", {
            "tool_call_id": call_id, "name": name, "args": args,
        }, turn=ctx.turn, span_id=span, parent=self.span_id)
        return None

    async def on_tool_result(self, ctx: TurnContext, result: ToolResult) -> ToolResult | None:
        span = self._tool_spans.pop(result.tool_call_id, None) or new_span_id()
        self._tool_started.pop(result.tool_call_id, None)
        self._raw_results[result.tool_call_id] = result.result or ""
        self._result_spans[result.tool_call_id] = span
        await self._emit("tool.result", {
            "tool_call_id": result.tool_call_id,
            "name": result.name,
            "args": dict(result.args or {}),
            "result": result.result or "",
            "duration_ms": result.duration_ms,
            "is_error": result.is_error,
            "error_kind": result.error_kind,
            "interrupted": result.interrupted,
            "repeat_count": result.repeat_count,
        }, turn=ctx.turn, span_id=span, parent=self.span_id)
        return None

    async def on_turn_end(self, ctx: TurnContext) -> Intervention | None:
        await self._emit("turn.end", {
            "tool_calls": len(ctx.tool_calls or []),
            "usage": dict(ctx.usage or {}),
        }, turn=ctx.turn)
        return None

    async def on_compaction(self, event: CompactionEvent) -> None:
        await self._emit("context.compact", {
            "seq": event.seq,
            "selected": event.selected,
            "tokens_before": event.tokens_before,
            "tokens_after": event.tokens_after,
            "relief_met": event.relief_met,
            "spill_refs": event.spill_refs,
            "attempts": event.attempts,
            "summary": event.summary,
            "rollback_reason": event.rollback_reason,
        }, turn=event.turn)

    async def on_context_compacted(self, ctx: ContextCompactionContext) -> None:
        await self._emit("context.compacted", {
            "reason": ctx.reason,
            "compactor": ctx.compactor,
            "policy": ctx.policy,
            "tokens_before": ctx.tokens_before,
            "messages_before": len(ctx.messages_before),
            "messages_after": len(ctx.messages_after),
            "after": to_json(ctx.messages_after),
        }, turn=ctx.turn)

    async def on_loop_end(self, result: AgentLoopResult) -> None:
        await self._emit("loop.end", {
            "stopped_by": result.stopped_by,
            "turns": result.turns_used,
            "tool_calls": result.tool_calls_count,
            "final_content": result.final_content or "",
        })
        if self.parent_span_id == self.run.root_span_id and not self.run.answer_explicit:
            self.run.final_output = result.final_content or ""
        self._reset_span()

    async def on_loop_cancelled(self) -> None:
        await self._emit("loop.cancelled", {})
        self._reset_span()

    def _reset_span(self) -> None:
        if self._token is not None:
            # A reset from another context (cancellation path) raises
            # ValueError; the loop's context is going away anyway.
            with contextlib.suppress(ValueError):
                current_agent_span.reset(self._token)
            self._token = None


def _guard_hooks(cls: type) -> None:
    for name, fn in list(vars(cls).items()):
        if name.startswith("on_") and inspect.iscoroutinefunction(fn):
            setattr(cls, name, _guarded(fn))


_guard_hooks(TelemetryObserver)


class ToolResultTail(BaseObserver):
    """Runs after every other observer: journals the tool result as the
    observers left it, when one of them reshaped it. A loop-level cap
    (``tool_result_max_chars``) applied later is not reflected here."""

    critical = True

    def __init__(self, head: TelemetryObserver) -> None:
        self.head = head
        self.run = head.run            # for the failure guard

    async def on_tool_result(self, ctx: TurnContext, result: ToolResult) -> ToolResult | None:
        raw = self.head._raw_results.pop(result.tool_call_id, None)
        final = result.result or ""
        if raw is not None and raw != final:
            await self.head._emit("tool.result.final", {
                "tool_call_id": result.tool_call_id, "name": result.name,
                "result": final, "raw_chars": len(raw), "final_chars": len(final),
            }, turn=ctx.turn, span_id=self.head._result_spans.pop(result.tool_call_id, None),
               parent=self.head.span_id)
        return None


_guard_hooks(ToolResultTail)


def _call_name_args(tool_call: dict[str, Any]) -> tuple[str, Any]:
    fn = tool_call.get("function")
    if isinstance(fn, dict):
        return str(fn.get("name", "")), fn.get("arguments")
    return str(tool_call.get("name", "")), tool_call.get("args", tool_call.get("arguments"))


class InterventionRecorder:
    """Transparent proxy that journals interventions returned by *inner*.

    Only ``on_*`` coroutine hooks are wrapped; every other attribute
    (``critical``, setters, isinstance-free duck typing) passes through.
    """

    def __init__(self, inner: Any, telemetry: TelemetryObserver) -> None:
        self._inner = inner
        self._telemetry = telemetry

    def __getattr__(self, name: str) -> Any:
        attr: Any = getattr(self._inner, name)
        if not (name.startswith("on_") and callable(attr)):
            return attr

        hook = cast("Callable[..., Awaitable[Any]]", attr)

        async def _wrapped(*args: Any, **kwargs: Any) -> Any:
            rv = await hook(*args, **kwargs)
            if isinstance(rv, (Intervention, ToolCallIntervention)):
                ctx = args[0] if args else None
                await self._telemetry._emit("observer.intervention", {
                    "observer": type(self._inner).__name__,
                    "hook": name,
                    "intervention": {
                        k: v for k, v in vars(rv).items() if v not in (None, False, [])
                    },
                }, turn=getattr(ctx, "turn", None))
            return rv

        return _wrapped
