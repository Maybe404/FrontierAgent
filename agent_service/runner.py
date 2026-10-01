"""Task scheduler: one worker process per task, bounded concurrency.

Lifecycle: queued -> running -> completed | failed | cancelled | timed_out
(cancelling is the transient state between a cancel request and the
worker exiting). Transitions are compare-and-set in the task store, so a
cancel racing with completion has exactly one winner, and a finished task
is never rewritten.

Failed and timed-out tasks carry an ``error_code`` the caller can branch on:
``llm_error``, ``incomplete``, ``agent_error``, ``worker_crashed``,
``timeout``, ``cancelled``, ``service_shutdown``, ``service_restart``,
``internal``.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
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
_EXPORT_RETRY_S = 60.0


class Runner:
    def __init__(self, store: TaskStore, cfg: ServiceConfig) -> None:
        self.store = store
        self.cfg = cfg
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._procs: dict[str, asyncio.subprocess.Process] = {}
        self._workers: list[asyncio.Task[None]] = []
        self._terminations: dict[str, asyncio.Task[None]] = {}
        # Pids already sent SIGINT, so a replaced termination never sends another.
        self._interrupted: set[int] = set()
        # Tasks between dequeue and their worker landing in ``_procs``.
        self._spawning: set[str] = set()
        self._stopping = False
        # Tasks whose final Langfuse export is still owed; retried by the
        # sweeper. Persisted as ``exported=0`` so a restart picks them up.
        self._export_backlog: dict[str, Path] = {}
        self._exports: set[asyncio.Task[None]] = set()
        self._sweeper: asyncio.Task[None] | None = None

    # -- lifecycle --------------------------------------------------------------

    async def start(self) -> None:
        await self._recover_after_restart()
        self._workers = [asyncio.create_task(self._worker(i)) for i in range(self.cfg.max_concurrency)]
        self._sweeper = asyncio.create_task(self._sweep_exports())

    async def stop(self) -> None:
        """Stop workers within ``shutdown_grace_s`` in total. Tasks stopped
        here end ``cancelled``/``service_shutdown`` so callers can resubmit;
        queued tasks stay queued and run after the restart."""
        self._stopping = True
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.cfg.shutdown_grace_s
        shut: set[str] = set()
        # Re-scan while waiting: a worker may have been spawning when stop began.
        while (self._procs or self._spawning) and loop.time() < deadline:
            for task_id in list(self._procs):
                if task_id not in shut and self._shutdown_task(task_id, deadline - loop.time()):
                    shut.add(task_id)
            await asyncio.sleep(0.1)
        for task_id, proc in list(self._procs.items()):
            if proc.returncode is not None:
                continue                # exited; only its bookkeeping is pending
            logger.warning("task %s: worker still running at shutdown deadline; killing it", task_id)
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGKILL)
        for t in (*self._workers, *self._exports, *([self._sweeper] if self._sweeper else [])):
            t.cancel()

    def _shutdown_task(self, task_id: str, remaining: float) -> bool:
        """Terminate *task_id* within *remaining* seconds (SIGINT wait plus
        the SIGTERM/SIGKILL steps). False while its row is not yet running."""
        if not self.store.transition(task_id, ("running",), "cancelling",
                                     error="service shutdown", error_code="service_shutdown"):
            row = self.store.get(task_id) or {}
            if row.get("status") != "cancelling":
                return row.get("status") not in ("queued", "running")
        # A cancel already in progress has the long cancel grace; replace it.
        old = self._terminations.pop(task_id, None)
        if old is not None:
            old.cancel()
        self._start_termination(task_id, grace=max(0.0, remaining - 8))
        return True

    async def _recover_after_restart(self) -> None:
        """Rows left running by a dead service are failed (their worker is
        stopped if it is verifiably still ours); queued rows run again. One
        unreadable task never blocks startup."""
        for row in self.store.unfinished():
            if row["status"] in ("running", "cancelling"):
                try:
                    await self._recover_row(row)
                except Exception:
                    logger.exception("task %s: recovery failed; closing it without its journal", row["id"])
                    status, code = _classify(engine.RunResult(), None, row, timed_out=False,
                                             forced=None if row["status"] == "cancelling"
                                             else ("failed", "service_restart"))
                    self.store.transition(row["id"], ("running", "cancelling"), status, finished_at=now(),
                                          error=row.get("error") or "service restarted; task journal unreadable",
                                          error_code=code)
            elif row["status"] == "queued":
                self._queue.put_nowait(row["id"])
            elif row.get("exported") == 0:
                self._export_backlog[row["id"]] = Path(row["workdir"])

    async def _recover_row(self, row: dict[str, Any]) -> None:
        workdir = Path(row["workdir"])
        pid = row.get("pid")
        if pid:
            owned = await asyncio.to_thread(_is_our_worker, pid, workdir)
            if owned:
                await _kill_group_and_wait(pid)
            elif owned is None:
                logger.warning("task %s: cannot verify pid %s (no /proc or ps); not signalling it",
                               row["id"], pid)
        # A worker that outlived the old service may have finished the task;
        # the journal decides before any forced status. A cancelling row keeps
        # the reason it was being stopped for (caller, timeout, shutdown).
        done = engine.read_result(workdir).status == "completed"
        if done or row["status"] == "cancelling":
            await self._finish(row["id"], workdir, exit_code=0 if done else None)
        else:
            await self._finish(row["id"], workdir, exit_code=None, forced=("failed", "service_restart"),
                               note="service restarted while task was running")

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

    def _start_termination(self, task_id: str, *, grace: float | None = None) -> None:
        proc = self._procs.get(task_id)
        if proc is not None and task_id not in self._terminations:
            t = asyncio.create_task(self._terminate(proc, grace=grace))
            self._terminations[task_id] = t
            # Only drop our own entry: shutdown may have replaced it.
            t.add_done_callback(lambda t, k=task_id: self._terminations.get(k) is t
                                and self._terminations.pop(k))

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
        if self._stopping:
            return                      # stays queued; the next start runs it
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
        self._spawning.add(task_id)
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, cwd=str(workdir), env=env, stdin=asyncio.subprocess.DEVNULL,
                stdout=log, stderr=asyncio.subprocess.STDOUT, start_new_session=True,
            )
        finally:
            log.close()
            self._spawning.discard(task_id)
        self._procs[task_id] = proc
        try:
            if self._stopping:
                # Shutdown began while it was spawning: stop the worker before
                # it does anything; the task stays queued for the next start.
                await self._terminate(proc, grace=0)
                return
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
                # Only a still-running task times out; one already being
                # cancelled keeps its reason and its termination in progress.
                timed_out = self.store.transition(
                    task_id, ("running",), "cancelling",
                    error=f"timed out after {self.cfg.task_timeout_s}s", error_code="timeout")
                if timed_out:
                    self._start_termination(task_id)
                await proc.wait()
            finally:
                exporter.cancel()
            await self._finish(task_id, workdir, exit_code=proc.returncode, timed_out=timed_out)
        finally:
            self._procs.pop(task_id, None)
            self._interrupted.discard(proc.pid)

    async def _terminate(self, proc: asyncio.subprocess.Process, *, grace: float | None = None) -> None:
        """SIGINT (graceful: the CLI stops at a turn boundary and journals
        run.end), then SIGTERM, then SIGKILL of the whole process group."""
        first = self.cfg.cancel_grace_s if grace is None else grace
        for sig, wait in ((signal.SIGINT, first), (signal.SIGTERM, 5), (signal.SIGKILL, 2)):
            if proc.returncode is not None:
                return
            # A second SIGINT makes the worker's asyncio.run raise
            # KeyboardInterrupt mid clean-up (run.end, deliverables); a worker
            # already interrupted is only given the time to finish.
            if not (sig == signal.SIGINT and proc.pid in self._interrupted):
                if sig == signal.SIGINT:
                    self._interrupted.add(proc.pid)
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

    async def _sweep_exports(self) -> None:
        """Retry final exports that failed (Langfuse down at task end)."""
        while True:
            await asyncio.sleep(_EXPORT_RETRY_S)
            for task_id, workdir in list(self._export_backlog.items()):
                await self._export_final(task_id, workdir)

    async def _export_final(self, task_id: str, workdir: Path) -> None:
        if await _export_once(workdir, {}, final=True):
            self._export_backlog.pop(task_id, None)
            self.store.update(task_id, exported=1)
        else:
            self._export_backlog[task_id] = workdir

    async def _finish(self, task_id: str, workdir: Path, *, exit_code: int | None,
                      forced: tuple[str, str] | None = None, timed_out: bool = False,
                      note: str = "") -> None:
        await engine.recover(workdir)            # crashed worker -> synthetic run.end
        result = engine.read_result(workdir)
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
            exported=0,
        )
        logger.info("task %s finished: %s/%s (exit %s)%s", task_id, status, code, exit_code,
                    "" if done else " [already final, not overwritten]")
        # Export after the final state is written, so a slow or unreachable
        # Langfuse never delays the task's result.
        if done:
            t = asyncio.create_task(self._export_final(task_id, workdir))
            self._exports.add(t)
            t.add_done_callback(self._exports.discard)


def _classify(result: engine.RunResult, exit_code: int | None, row: dict[str, Any], *,
              forced: tuple[str, str] | None, timed_out: bool) -> tuple[str, str | None]:
    """``(status, error_code)``. A run that completed wins over a late cancel;
    a stopped run keeps the reason it was stopped for."""
    if forced:
        return forced
    if exit_code == 0 and result.status == "completed":
        return "completed", None
    if timed_out or row.get("error_code") == "timeout":
        return "timed_out", "timeout"
    if row.get("status") == "cancelling":
        return "cancelled", row.get("error_code") or "cancelled"
    if result.status == "cancelled":
        return "cancelled", "cancelled"
    if result.status == "crashed" or not result.status:
        return "failed", "worker_crashed"
    if result.status == "incomplete":
        return "failed", "incomplete"
    if result.error.startswith(("llm_error", "LLMError")):
        return "failed", "llm_error"
    return "failed", "agent_error"


async def _export_once(workdir: Path, exporters: dict[str, Any], *, final: bool) -> bool:
    """Export every run under *workdir*; False if anything is still pending."""
    from frontier_agent.telemetry import langfuse_export as lf

    if not lf.configured():
        return True
    ok = True
    try:
        runs = await asyncio.to_thread(engine.session_ids, workdir)
    except Exception as exc:
        logger.warning("Langfuse export: cannot list runs in %s: %s", workdir, exc)
        return False
    for rd, session_id, run_id in runs:
        ex = exporters.setdefault(run_id, lf.Exporter(rd, session_id, run_id))
        try:
            await ex.step(final=final, wait_lock=final)
            ok = ok and not ex.pending
        except Exception as exc:
            ok = False
            logger.warning("Langfuse export for %s deferred: %s", run_id, exc)
    return ok


def _cmdline(pid: int) -> list[str] | str | None:
    """Command line of *pid*: the exact argument vector from /proc on Linux,
    else the space-joined ``ps -ww`` string (argument boundaries are lost).
    Empty when the process is gone, ``None`` when it cannot be determined."""
    proc = Path(f"/proc/{pid}/cmdline")
    if Path("/proc/self/cmdline").exists():
        try:
            return [a for a in proc.read_bytes().decode(errors="replace").split("\0") if a]
        except OSError:
            return []
    try:
        out = subprocess.run(["ps", "-ww", "-o", "command=", "-p", str(pid)],
                             capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return None
    return out.stdout.strip() if out.returncode == 0 else ""


def _is_our_worker(pid: int, workdir: Path) -> bool | None:
    """True only if *pid* is a worker started for *workdir* (its ``--cwd``
    argument equals it exactly), so a reused pid is never signalled.
    ``None`` when identity cannot be verified on this host."""
    argv = _cmdline(pid)
    if argv is None:
        return None
    target = str(workdir)
    if isinstance(argv, str):
        # The worker always has more arguments after --cwd, so the path is
        # followed by a space; this also matches paths that contain spaces.
        return f" --cwd {target} " in f" {argv} "
    return any(a == "--cwd" and b == target for a, b in itertools.pairwise(argv))


async def _kill_group_and_wait(pid: int) -> None:
    for sig in (signal.SIGTERM, signal.SIGKILL):
        with contextlib.suppress(ProcessLookupError, PermissionError):
            # Workers lead their own group; anything else gets a plain kill.
            if os.getpgid(pid) == pid:
                os.killpg(pid, sig)
            else:
                os.kill(pid, sig)
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
