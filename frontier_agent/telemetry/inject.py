"""Attach telemetry to an agent loop's observer list.

Called from the two places every FrontierAgent loop passes through: the
product loop adapter (main agents) and the AgentBus composition (sub-agents).
A no-op unless a ``TelemetryRun`` is active in the current context.
"""

from __future__ import annotations

import functools
from typing import Any

from frontier_agent.telemetry.run import current_agent_span, current_run


def with_telemetry(observers: list[Any] | None) -> list[Any] | None:
    run = current_run.get()
    if run is None:
        return observers
    from frontier_agent.telemetry.observer import InterventionRecorder, TelemetryObserver

    if any(isinstance(o, TelemetryObserver) for o in observers or []):
        return observers
    telemetry = TelemetryObserver(
        run, parent_span_id=current_agent_span.get() or run.root_span_id,
    )
    wrapped = [InterventionRecorder(o, telemetry) for o in observers or []]
    # First, so every other observer's view of a turn is journaled after the
    # raw event it reacted to.
    return [telemetry, *wrapped]


def wrap_loop(fn: Any) -> Any:
    """Wrap a ``run_agent_loop`` callable so its observers get telemetry."""
    if getattr(fn, "__fa_telemetry__", False):
        return fn

    @functools.wraps(fn)
    async def _run(*args: Any, **kwargs: Any) -> Any:
        kwargs["observers"] = with_telemetry(kwargs.get("observers"))
        return await fn(*args, **kwargs)

    _run.__fa_telemetry__ = True  # type: ignore[attr-defined]
    return _run
