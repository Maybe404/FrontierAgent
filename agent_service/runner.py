"""Task scheduler: one worker process per task, bounded concurrency.

Lifecycle: queued -> running -> completed | failed | cancelled | timed_out
(cancelling is the transient state between a cancel request and the
worker exiting). Transitions are compare-and-set in the task store, so a
cancel racing with completion has exactly one winner, and a finished task
is never rewritten.

Failed and timed-out tasks carry an ``error_code`` the caller can branch on:
``llm_error``, ``incomplete``, ``agent_error``, ``worker_crashed``,
``timeout``, ``service_restart``, ``internal``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import subprocess
from pathlib import Path
from typing import Any

from agent_service import engine
from agent_service.config import ServiceConfig
from agent_service.store import TaskStore, now

logger = logging.getLogger(__name__)

_EXPORT_INTERVAL_S = 5.0


class Runner:
    def __init__(self, store: TaskStore, cfg: ServiceConfig) -> None:
        self.store = store
        self.cfg = cfg
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._procs: dict[str, asyncio.subprocess.Process] = {}
        self._workers: list[asyncio.Task[None]] = []
        self._terminations: dict[str, asyncio.Task[None]] = {}

    # -- lifecycle --------------------------------------------------------------

    async def start(self) -> None:
        await self._recover_after_restart()
        self._workers = [asyncio.create_task(self._worker(i)) for i in range(self.cfg.max_concurrency)]

    async def stop(self) -> None:
        """Stop accepting work and wait (bounded) for running workers to end."""
        for task_id in list(self._procs):
            await self.cancel(task_id, reason="service shutdown")
        pending = list(self._terminations.values())
        if pending:
            await asyncio.wait(pending, timeout=self.cfg.cancel_grace_s + 20)
        # Let the per-task finish logic record the final states.
        deadline = asyncio.get_running_loop().time() + 10
        while self._procs and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.1)
        for w in self._workers:
            w.cancel()

    async def _recover_after_restart(self) -> None:
        """Rows left running by a dead service are failed (their worker is
        stopped if it is verifiably still ours); queued rows run again."""
        for row in self.store.list(limit=10_000):
            if row["status"] in ("running", "cancelling"):
                workdir = Path(row["workdir"])
                pid = row.get("pid")
                if pid and _is_our_worker(pid, workdir):
                    await _kill_group_and_wait(pid)
                code = "cancelled" if row["status"] == "cancelling" else "service_restart"
                await self._finish(row["id"], workdir, exit_code=None,
                                   forced=("cancelled" if code == "cancelled" else "failed", code),
                                   note="service restarted while task was running")
            elif row["status"] == "queued":
                self._queue.put_nowait(row["id"])

    # -- api ---------------------------------------------------------------------

    def submit(self, task_id: str) -> None:
        self._queue.put_nowait(task_id)

    async def cancel(self, task_id: str, *, reason: str = "cancelled by caller") -> dict[str, Any] | None:
        if self.store.transition(task_id, ("queued",), "cancelled", finished_at=now(),
                                 error=reason, error_code="cancelled"):
            return self.store.get(task_id)
        if self.store.transition(task_id, ("running",), "cancelling", error=reason, error_code="cancelled"):
            self._start_termination(task_id)
        return self.store.get(task_id)

    def _start_termination(self, task_id: str) -> None:
        proc = self._procs.get(task_id)
        if proc is not None and task_id not in self._terminations:
            t = asyncio.create_task(self._terminate(proc))
            self._terminations[task_id] = t
            t.add_done_callback(lambda _t, k=task_id: self._terminations.pop(k, None))

    # -- execution ---------------------------------------------------------------

    async def _worker(self, idx: int) -> None:
        while True:
            task_id = await self._queue.get()
            try:
                await self._run(task_id)
            except Exception:
                logger.exception("task %s: runner error", task_id)
                self.store.transition(task_id, ("queued", "running", "cancelling"), "failed",
                                      finished_at=now(), error="internal runner error", error_code="internal")
            finally:
                self._queue.task_done()

    async def _run(self, task_id: str) -> None:
        row = self.store.get(task_id)
        if row is None or row["status"] != "queued":
            return                      # cancelled while queued, or already handled
        workdir = Path(row["workdir"])
        workdir.mkdir(parents=True, exist_ok=True)
        cmd = engine.build_command(task=row["task"], mode=row["mode"], workdir=workdir,
                                   max_turns=row.get("max_turns"))
        env = engine.build_env(task_id=task_id, request_id=row["request_id"],
                               allow_fake_ip=self.cfg.allow_fake_ip)
        log = (workdir / "worker.log").open("ab")
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, cwd=str(workdir), env=env, stdin=asyncio.subprocess.DEVNULL,
                stdout=log, stderr=asyncio.subprocess.STDOUT, start_new_session=True,
            )
        finally:
            log.close()
        self._procs[task_id] = proc
        try:
            if not self.store.transition(task_id, ("queued",), "running", started_at=now(), pid=proc.pid):
                await self._terminate(proc)        # cancelled between dequeue and start
                await self._finish(task_id, workdir, exit_code=proc.returncode,
                                   forced=("cancelled", "cancelled"))
                return
            exporter = asyncio.create_task(self._export_loop(workdir))
            timed_out = False
            try:
                await asyncio.wait_for(proc.wait(), timeout=self.cfg.task_timeout_s)
            except TimeoutError:
                timed_out = True
                self.store.transition(task_id, ("running",), "cancelling",
                                      error=f"timed out after {self.cfg.task_timeout_s}s", error_code="timeout")
                await self._terminate(proc)
            finally:
                exporter.cancel()
            await self._finish(task_id, workdir, exit_code=proc.returncode, timed_out=timed_out)
        finally:
            self._procs.pop(task_id, None)

    async def _terminate(self, proc: asyncio.subprocess.Process) -> None:
        """SIGINT (graceful: the CLI stops at a turn boundary and journals
        run.end), then SIGTERM, then SIGKILL of the whole process group."""
        for sig, wait in ((signal.SIGINT, self.cfg.cancel_grace_s), (signal.SIGTERM, 10), (signal.SIGKILL, 5)):
            if proc.returncode is not None:
                return
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, sig)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(proc.wait(), timeout=wait)

    async def _export_loop(self, workdir: Path) -> None:
        """Langfuse export runs here, in the service, so workers never hold
        the Langfuse credentials."""
        from frontier_agent.telemetry import langfuse_export as lf

        if not lf.configured():
            return
        exporters: dict[str, lf.Exporter] = {}
        while True:
            await asyncio.sleep(_EXPORT_INTERVAL_S)
            await _export_once(workdir, exporters, final=False)

    async def _finish(self, task_id: str, workdir: Path, *, exit_code: int | None,
                      forced: tuple[str, str] | None = None, timed_out: bool = False,
                      note: str = "") -> None:
        await engine.recover(workdir)            # crashed worker -> synthetic run.end
        result = engine.read_result(workdir)
        with contextlib.suppress(Exception):
            await _export_once(workdir, {}, final=True)
        row = self.store.get(task_id) or {}
        status, code = _classify(result, exit_code, row, forced=forced, timed_out=timed_out)
        error = None
        if status != "completed":
            error = note or row.get("error") or result.error or \
                f"worker exited with code {exit_code}; run status {result.status or 'unknown'}"
        done = self.store.transition(
            task_id, ("queued", "running", "cancelling"), status,
            finished_at=now(), exit_code=exit_code, answer=result.answer, error=error,
            error_code=code, complete=int(result.complete), deliverables=result.deliverables,
        )
        logger.info("task %s finished: %s/%s (exit %s)%s", task_id, status, code, exit_code,
                    "" if done else " [already final, not overwritten]")


def _classify(result: engine.RunResult, exit_code: int | None, row: dict[str, Any], *,
              forced: tuple[str, str] | None, timed_out: bool) -> tuple[str, str | None]:
    """``(status, error_code)``. A run that completed wins over a late cancel."""
    if forced:
        return forced
    if exit_code == 0 and result.status == "completed":
        return "completed", None
    if timed_out or row.get("error_code") == "timeout":
        return "timed_out", "timeout"
    if row.get("status") == "cancelling" or result.status == "cancelled":
        return "cancelled", "cancelled"
    if result.status == "crashed" or not result.status:
        return "failed", "worker_crashed"
    if result.status == "incomplete":
        return "failed", "incomplete"
    if result.error.startswith(("llm_error", "LLMError")):
        return "failed", "llm_error"
    return "failed", "agent_error"


async def _export_once(workdir: Path, exporters: dict[str, Any], *, final: bool) -> None:
    from frontier_agent.telemetry import langfuse_export as lf

    if not lf.configured():
        return
    for rd, session_id, run_id in await asyncio.to_thread(engine.session_ids, workdir):
        ex = exporters.setdefault(run_id, lf.Exporter(rd, session_id, run_id))
        try:
            await ex.step(final=final, wait_lock=final)
        except Exception as exc:
            logger.warning("Langfuse export for %s deferred: %s", run_id, exc)


def _is_our_worker(pid: int, workdir: Path) -> bool:
    """True only if *pid* is alive and its command line names this task's
    workdir, so a reused pid is never signalled."""
    try:
        out = subprocess.run(["ps", "-o", "command=", "-p", str(pid)],
                             capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return False
    return out.returncode == 0 and str(workdir) in out.stdout


async def _kill_group_and_wait(pid: int) -> None:
    for sig in (signal.SIGTERM, signal.SIGKILL):
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(pid, sig)          # workers lead their own session/group
        for _ in range(50):
            if not _alive(pid):
                return
            await asyncio.sleep(0.2)


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True
