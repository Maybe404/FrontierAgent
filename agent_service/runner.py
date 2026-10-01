"""Task scheduler: one worker process per task, bounded concurrency.

Lifecycle: queued -> running -> completed | failed | cancelled
(cancelling is the transient state between a cancel request and the
worker exiting). Every transition is a compare-and-set in the task store,
so a cancel racing with completion has exactly one winner.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
from pathlib import Path
from typing import Any

from agent_service import engine
from agent_service.config import ServiceConfig
from agent_service.store import TaskStore, now

logger = logging.getLogger(__name__)


class Runner:
    def __init__(self, store: TaskStore, cfg: ServiceConfig) -> None:
        self.store = store
        self.cfg = cfg
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._procs: dict[str, asyncio.subprocess.Process] = {}
        self._workers: list[asyncio.Task[None]] = []
        self._bg: set[asyncio.Task[None]] = set()

    # -- lifecycle --------------------------------------------------------------

    async def start(self) -> None:
        await self._recover_after_restart()
        self._workers = [asyncio.create_task(self._worker(i)) for i in range(self.cfg.max_concurrency)]

    async def stop(self) -> None:
        for w in self._workers:
            w.cancel()
        for task_id in list(self._procs):
            await self.cancel(task_id, reason="service shutdown")

    async def _recover_after_restart(self) -> None:
        """Running rows whose worker died with the old service are failed;
        queued rows are re-queued."""
        for row in self.store.list(limit=10_000):
            if row["status"] in ("running", "cancelling"):
                pid = row.get("pid")
                if pid and _alive(pid):
                    # Orphaned worker from the previous service; stop it.
                    with contextlib.suppress(ProcessLookupError):
                        os.kill(pid, signal.SIGTERM)
                await engine.recover(Path(row["workdir"]))
                final = "cancelled" if row["status"] == "cancelling" else "failed"
                await self._finish(row["id"], Path(row["workdir"]), exit_code=None,
                                   forced_status=final, note="service restarted while task was running")
            elif row["status"] == "queued":
                self._queue.put_nowait(row["id"])

    # -- api ---------------------------------------------------------------------

    def submit(self, task_id: str) -> None:
        self._queue.put_nowait(task_id)

    async def cancel(self, task_id: str, *, reason: str = "cancelled by caller") -> dict[str, Any] | None:
        if self.store.transition(task_id, ("queued",), "cancelled", finished_at=now(), error=reason):
            return self.store.get(task_id)
        if self.store.transition(task_id, ("running",), "cancelling", error=reason):
            proc = self._procs.get(task_id)
            if proc is not None:
                t = asyncio.create_task(self._terminate(proc))
                self._bg.add(t)
                t.add_done_callback(self._bg.discard)
        return self.store.get(task_id)

    # -- execution ---------------------------------------------------------------

    async def _worker(self, idx: int) -> None:
        while True:
            task_id = await self._queue.get()
            try:
                await self._run(task_id)
            except Exception:
                logger.exception("task %s: runner error", task_id)
                self.store.update(task_id, status="failed", finished_at=now(), error="internal runner error")
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
        env = engine.build_env(task_id=task_id, request_id=row["request_id"])
        log = (workdir / "worker.log").open("ab")
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, cwd=str(workdir), env=env, stdin=asyncio.subprocess.DEVNULL,
                stdout=log, stderr=asyncio.subprocess.STDOUT, start_new_session=True,
            )
        finally:
            log.close()
        if not self.store.transition(task_id, ("queued",), "running", started_at=now(), pid=proc.pid):
            await self._terminate(proc)        # cancelled between dequeue and start
            await proc.wait()
            await self._finish(task_id, workdir, exit_code=proc.returncode, forced_status="cancelled")
            return
        self._procs[task_id] = proc
        try:
            await asyncio.wait_for(proc.wait(), timeout=self.cfg.task_timeout_s)
        except TimeoutError:
            self.store.transition(task_id, ("running",), "cancelling",
                                  error=f"timed out after {self.cfg.task_timeout_s}s")
            await self._terminate(proc)
            await proc.wait()
        finally:
            self._procs.pop(task_id, None)
        await self._finish(task_id, workdir, exit_code=proc.returncode)

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

    async def _finish(self, task_id: str, workdir: Path, *, exit_code: int | None,
                      forced_status: str | None = None, note: str = "") -> None:
        await engine.recover(workdir)            # crashed worker -> synthetic run.end
        result = engine.read_result(workdir)
        row = self.store.get(task_id) or {}
        if forced_status:
            status = forced_status
        elif row.get("status") == "cancelling":
            status = "cancelled"
        elif exit_code == 0 and result.status == "completed":
            status = "completed"
        else:
            status = "failed"
        error = note or row.get("error") or result.error
        if status == "failed" and not error:
            error = f"worker exited with code {exit_code}; run status {result.status or 'unknown'}"
        self.store.update(
            task_id, status=status, finished_at=now(), exit_code=exit_code,
            answer=result.answer, error=error if status != "completed" else None,
            complete=int(result.complete), deliverables=result.deliverables,
        )
        logger.info("task %s finished: %s (exit %s)", task_id, status, exit_code)


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True
