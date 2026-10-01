"""Run boundary: open a journal for one task and close it on every exit path."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from frontier_agent.telemetry.run import TelemetryRun, current_run, enabled, recover_stale

logger = logging.getLogger(__name__)


@contextlib.asynccontextmanager
async def telemetry_run(
    *,
    run_dir: Path,
    session_id: str,
    task: str,
    workflow: str = "",
    config: dict[str, Any] | None = None,
    outputs_dir: Path | None = None,
) -> AsyncIterator[TelemetryRun | None]:
    """Journal everything the agents do inside the block.

    Nested use (a task that re-enters ``run_task``) joins the outer run. On
    exit the files in *outputs_dir* are journaled as deliverables.
    """
    if not enabled() or current_run.get() is not None:
        yield current_run.get()
        return
    with contextlib.suppress(Exception):
        recovered = await recover_stale(Path(run_dir))
        if recovered:
            logger.warning("telemetry: closed run %s left open by a dead process", recovered)
    run = TelemetryRun(run_dir, session_id=session_id, workflow=workflow)
    # Snapshot before the run opens: from run.start to the try below there
    # must be no await, or a cancel there would leave the run unclosed.
    before = None
    if outputs_dir is not None:
        try:
            before = await asyncio.to_thread(TelemetryRun.snapshot_outputs, Path(outputs_dir))
        except Exception as exc:     # never fail the task over a snapshot
            run.note_failure("outputs snapshot", exc)
    try:
        await run.start(task=task, config=config)
    except asyncio.CancelledError:
        run.stop_heartbeat()
        with contextlib.suppress(Exception):
            await asyncio.shield(run.end(status="cancelled"))
        raise
    except Exception:
        run.stop_heartbeat()
        logger.exception("telemetry: could not start run journal; continuing without it")
        yield None
        return
    token = current_run.set(run)
    status, error = "completed", ""
    exporter = _start_exporter(run)
    try:
        yield run
        if run.outcome is not None:
            status, error = run.outcome
    except (asyncio.CancelledError, KeyboardInterrupt):
        status = "cancelled"
        raise
    except BaseException as exc:
        status, error = "error", f"{type(exc).__name__}: {exc}"
        raise
    finally:
        current_run.reset(token)
        # Stop renewing the lease first: if run.end below is interrupted, the
        # lease must expire so the run can be recovered.
        run.stop_heartbeat()
        if outputs_dir is not None:
            try:
                await asyncio.shield(run.record_outputs(Path(outputs_dir), before))
            except Exception as exc:
                run.note_failure("record outputs", exc)
        with contextlib.suppress(Exception):
            await asyncio.shield(run.end(status=status, error=error))
        if exporter is not None:
            with contextlib.suppress(Exception):
                await asyncio.shield(exporter.finish())


def _start_exporter(run: TelemetryRun) -> Any:
    try:
        from frontier_agent.telemetry.langfuse_export import LiveExporter

        return LiveExporter.maybe_start(run)
    except Exception:
        logger.exception("telemetry: Langfuse exporter failed to start; local journal unaffected")
        return None
