"""agent_service: submit/idempotency/auth, SSE, downloads, cancel, restart recovery.

The worker command is replaced by a tiny script that writes a real run
journal (via frontier_agent.telemetry), so no model or network is needed.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from agent_service import engine
from agent_service.api import create_app
from agent_service.config import ServiceConfig

FAKE_WORKER = r'''
import asyncio, os, signal, sys
from pathlib import Path
from frontier_agent.telemetry.scope import telemetry_run
from frontier_agent.telemetry.inject import with_telemetry
from agent_core.loop_types import AgentLoopResult, LoopConfig

workdir, task = Path(sys.argv[1]), sys.argv[2]
rd = workdir / ".apodex" / "runs" / "s1"
out = rd / "outputs"
out.mkdir(parents=True, exist_ok=True)

async def main():
    async with telemetry_run(run_dir=rd, session_id="s1", task=task, outputs_dir=out):
        obs = with_telemetry([])[0]
        await obs.on_loop_start(LoopConfig(role_id="main"))
        if task.startswith("sleep"):
            await asyncio.sleep(60)
        if task.startswith("crash"):
            os._exit(3)
        (out / "report.md").write_text("# report\n" + task, encoding="utf-8")
        await obs.on_loop_end(AgentLoopResult(messages=[], final_content="answer: " + task, stopped_by="no_tool"))

asyncio.run(main())
'''


@pytest.fixture()
def client(tmp_path: Path, monkeypatch):
    script = tmp_path / "fake_worker.py"
    script.write_text(FAKE_WORKER, encoding="utf-8")
    monkeypatch.setattr(engine, "build_command",
                        lambda *, task, mode, workdir, max_turns: [sys.executable, str(script), str(workdir), task])
    cfg = ServiceConfig(data_dir=tmp_path / "svc", api_token="t0ken", max_concurrency=2,
                        task_timeout_s=120, cancel_grace_s=10, default_mode="react", max_task_chars=1000)
    with TestClient(create_app(cfg)) as c:
        c.headers["Authorization"] = "Bearer t0ken"
        yield c


def _wait(client, task_id: str, timeout: float = 30) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        row = client.get(f"/v1/tasks/{task_id}").json()
        if row["status"] in ("completed", "failed", "cancelled", "timed_out"):
            return row
        time.sleep(0.2)
    raise AssertionError(f"task {task_id} did not finish: {row}")


def test_auth_is_required(client) -> None:
    r = client.post("/v1/tasks", json={"task": "x"}, headers={"Authorization": "Bearer wrong"})
    assert r.status_code == 401


def test_submit_runs_to_completion_with_answer_and_deliverable(client) -> None:
    r = client.post("/v1/tasks", json={"task": "hello", "request_id": "r1"})
    assert r.status_code == 202 and r.json()["created"] is True
    row = _wait(client, r.json()["id"])
    assert row["status"] == "completed", row
    assert row["answer"] == "answer: hello"
    assert [d["path"] for d in row["deliverables"]] == ["report.md"]
    assert client.get(f"/v1/tasks/{row['id']}/files/report.md").text == "# report\nhello"
    assert client.get(f"/v1/tasks/{row['id']}/files/../journal.db").status_code == 404


def test_request_id_is_idempotent(client) -> None:
    a = client.post("/v1/tasks", json={"task": "one", "request_id": "same"}).json()
    b = client.post("/v1/tasks", json={"task": "two", "request_id": "same"}).json()
    assert a["id"] == b["id"] and b["created"] is False


def test_rejects_unknown_mode_and_oversized_task(client) -> None:
    assert client.post("/v1/tasks", json={"task": "x", "mode": "nope"}).status_code == 422
    assert client.post("/v1/tasks", json={"task": "x" * 1001}).status_code == 413


def test_sse_streams_journal_then_task_end(client) -> None:
    task_id = client.post("/v1/tasks", json={"task": "stream"}).json()["id"]
    _wait(client, task_id)
    body = client.get(f"/v1/tasks/{task_id}/events").text
    kinds = [line.split(": ", 1)[1] for line in body.splitlines() if line.startswith("event: ")]
    assert kinds[0] == "run.start" and kinds[-1] == "task.end"
    assert {"loop.start", "loop.end", "deliverable", "run.end"} <= set(kinds)
    # Resume after the last journal event: only the terminal marker remains.
    ids = [int(line[4:]) for line in body.splitlines() if line.startswith("id: ")]
    tail = client.get(f"/v1/tasks/{task_id}/events", headers={"Last-Event-ID": str(ids[-1])}).text
    assert [ln for ln in tail.splitlines() if ln.startswith("event: ")] == ["event: task.end"]


def test_cancel_running_task_is_graceful(client) -> None:
    task_id = client.post("/v1/tasks", json={"task": "sleep please"}).json()["id"]
    deadline = time.monotonic() + 20
    while client.get(f"/v1/tasks/{task_id}").json()["status"] != "running":
        assert time.monotonic() < deadline
        time.sleep(0.1)
    time.sleep(1.0)                 # let the worker open its journal
    assert client.post(f"/v1/tasks/{task_id}/cancel").json()["status"] == "cancelling"
    row = _wait(client, task_id)
    assert row["status"] == "cancelled" and row["error_code"] == "cancelled"
    workdir = Path(client.app.state.store.get(task_id)["workdir"])
    assert engine.read_result(workdir).status == "cancelled"


def test_crashed_worker_is_failed_with_synthetic_run_end(client) -> None:
    task_id = client.post("/v1/tasks", json={"task": "crash now"}).json()["id"]
    row = _wait(client, task_id)
    assert row["status"] == "failed" and row["exit_code"] == 3
    assert row["error_code"] == "worker_crashed"
    workdir = Path(client.app.state.store.get(task_id)["workdir"])
    assert engine.read_result(workdir).status == "crashed"


def test_restart_fails_orphaned_running_rows(tmp_path: Path, client) -> None:
    store = client.app.state.store
    row, _ = store.create(task="ghost", mode="react", request_id="ghost", max_turns=None,
                          workdir_root=tmp_path / "svc" / "tasks")
    store.update(row["id"], status="running", pid=2 ** 22 + 4321)
    import asyncio

    asyncio.run(client.app.state.runner._recover_after_restart())
    after = store.get(row["id"])
    assert after["status"] == "failed" and after["error_code"] == "service_restart"
    assert "restarted" in after["error"]
    assert json.loads(json.dumps(after["deliverables"])) == []


def test_worker_env_is_an_allowlist() -> None:
    env = engine.build_env(task_id="t", request_id="r", source={
        "PATH": "/bin", "OPENAI_API_KEY": "k", "SYNCO_SEARCH_TOKEN": "s",
        "SERVICE_API_TOKEN": "secret", "LANGFUSE_SECRET_KEY": "lf", "AWS_SECRET_ACCESS_KEY": "aws",
        "FRONTIER_TELEMETRY": "0", "FRONTIER_AGENT_ALLOW_FAKE_IP_CIDRS": "198.18.0.0/15",
    })
    assert env["OPENAI_API_KEY"] == "k" and env["SYNCO_SEARCH_TOKEN"] == "s" and env["PATH"] == "/bin"
    for leaked in ("SERVICE_API_TOKEN", "LANGFUSE_SECRET_KEY", "AWS_SECRET_ACCESS_KEY",
                   "FRONTIER_AGENT_ALLOW_FAKE_IP_CIDRS"):
        assert leaked not in env
    assert env["FRONTIER_TELEMETRY"] == "1"       # the service needs the journal
    assert "FRONTIER_AGENT_ALLOW_FAKE_IP_CIDRS" in engine.build_env(
        task_id="t", request_id="r", allow_fake_ip=True,
        source={"FRONTIER_AGENT_ALLOW_FAKE_IP_CIDRS": "198.18.0.0/15"})


def test_fake_ip_optin_requires_loopback(monkeypatch) -> None:
    monkeypatch.setenv("SERVICE_ALLOW_FAKE_IP", "1")
    assert ServiceConfig.from_env(host="127.0.0.1").allow_fake_ip is True
    assert ServiceConfig.from_env(host="0.0.0.0").allow_fake_ip is False


def test_timeout_is_reported_as_timed_out(tmp_path: Path, monkeypatch) -> None:
    script = tmp_path / "fake_worker.py"
    script.write_text(FAKE_WORKER, encoding="utf-8")
    monkeypatch.setattr(engine, "build_command",
                        lambda *, task, mode, workdir, max_turns: [sys.executable, str(script), str(workdir), task])
    cfg = ServiceConfig(data_dir=tmp_path / "svc", api_token="", max_concurrency=1,
                        task_timeout_s=2, cancel_grace_s=5, default_mode="react", max_task_chars=1000)
    with TestClient(create_app(cfg, allow_no_auth=True)) as c:
        task_id = c.post("/v1/tasks", json={"task": "sleep long"}).json()["id"]
        row = _wait(c, task_id)
    assert row["status"] == "timed_out" and row["error_code"] == "timeout"


def test_completed_task_is_not_overwritten_by_late_cancel(client) -> None:
    task_id = client.post("/v1/tasks", json={"task": "quick"}).json()["id"]
    assert _wait(client, task_id)["status"] == "completed"
    assert client.post(f"/v1/tasks/{task_id}/cancel").json()["status"] == "completed"


def test_classify_prefers_a_completed_run_over_cancelling() -> None:
    from agent_service.runner import _classify

    done = engine.RunResult(status="completed", answer="a")
    assert _classify(done, 0, {"status": "cancelling"}, forced=None, timed_out=False) == ("completed", None)
    failed = engine.RunResult(status="failed", error="llm_error: 503")
    assert _classify(failed, 1, {"status": "running"}, forced=None, timed_out=False) == ("failed", "llm_error")
    inc = engine.RunResult(status="incomplete")
    assert _classify(inc, 1, {"status": "running"}, forced=None, timed_out=False) == ("failed", "incomplete")


def test_restart_never_signals_a_process_that_is_not_our_worker(tmp_path: Path) -> None:
    from agent_service.runner import _is_our_worker

    assert _is_our_worker(os.getpid(), tmp_path / "not-in-my-cmdline") is False


def test_missing_token_is_rejected(client) -> None:
    r = client.post("/v1/tasks", json={"task": "x"}, headers={"Authorization": ""})
    assert r.status_code == 401


def test_task_status_enum_matches_store() -> None:
    from typing import get_args

    from agent_service.schemas import TaskStatus
    from agent_service.store import ACTIVE, FINAL

    assert set(get_args(TaskStatus)) == {*ACTIVE, *FINAL}


def test_openapi_documents_every_operation_and_field(client) -> None:
    spec = client.get("/openapi.json").json()
    assert spec["info"]["description"]
    assert "HTTPBearer" in spec["components"]["securitySchemes"]
    for path, ops in spec["paths"].items():
        for method, op in ops.items():
            where = f"{method.upper()} {path}"
            assert op.get("summary") and op.get("description"), where
            assert op.get("tags"), where
            for p in op.get("parameters", []):
                assert p.get("description"), f"{where} parameter {p['name']}"
    for name in ("SubmitRequest", "SubmitResponse", "Task", "TaskList", "Deliverable", "Health", "ErrorResponse"):
        for field, schema in spec["components"]["schemas"][name]["properties"].items():
            assert schema.get("description"), f"{name}.{field}"
    ok = spec["paths"]["/v1/tasks/{task_id}"]["get"]["responses"]["200"]["content"]["application/json"]
    assert ok["schema"]["$ref"].endswith("/Task")


def test_worker_process_does_not_reload_a_parent_dotenv(tmp_path: Path) -> None:
    """The allowlist must hold inside the real worker: the CLI's .env loader
    walks up from the task dir, so put a poisoned .env above it."""
    import subprocess

    (tmp_path / ".env").write_text("SERVICE_API_TOKEN=leaked\nLANGFUSE_SECRET_KEY=leaked\n")
    workdir = tmp_path / "tasks" / "t1"
    workdir.mkdir(parents=True)
    env = engine.build_env(task_id="t1", request_id="r", source=dict(os.environ))
    probe = ("import json, os; from apodex.userenv import load_environment; load_environment(); "
             "print(json.dumps({k: os.environ.get(k) for k in ('SERVICE_API_TOKEN', 'LANGFUSE_SECRET_KEY')}))")
    out = subprocess.run([sys.executable, "-c", probe], cwd=workdir, env=env,
                         capture_output=True, text=True, timeout=60, check=True)
    assert json.loads(out.stdout.strip().splitlines()[-1]) == {
        "SERVICE_API_TOKEN": None, "LANGFUSE_SECRET_KEY": None}


def test_create_app_refuses_without_token_unless_allowed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("SERVICE_ALLOW_NO_AUTH", raising=False)
    cfg = ServiceConfig(data_dir=tmp_path, api_token="", max_concurrency=1, task_timeout_s=10,
                        cancel_grace_s=1, default_mode="react", max_task_chars=10)
    with pytest.raises(RuntimeError):
        create_app(cfg)
    create_app(cfg, allow_no_auth=True)
    # An app factory does not know its bind address: no fake-IP passthrough.
    monkeypatch.setenv("SERVICE_ALLOW_FAKE_IP", "1")
    assert ServiceConfig.from_env().allow_fake_ip is False


def test_is_our_worker_recognises_a_real_worker(tmp_path: Path) -> None:
    import subprocess

    from agent_service.runner import _is_our_worker

    workdir = tmp_path / "tasks" / "abc"
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)", "--cwd", str(workdir)])
    try:
        time.sleep(0.3)
        assert _is_our_worker(proc.pid, workdir) is True
        assert _is_our_worker(proc.pid, tmp_path / "tasks" / "ab") is False   # no prefix match
    finally:
        proc.kill()
        proc.wait()


def test_restart_keeps_a_task_the_worker_finished(tmp_path: Path, client) -> None:
    import asyncio

    task_id = client.post("/v1/tasks", json={"task": "done before restart"}).json()["id"]
    _wait(client, task_id)
    store = client.app.state.store
    store.update(task_id, status="running", pid=2 ** 22 + 99, error_code=None)   # service died mid-task
    asyncio.run(client.app.state.runner._recover_after_restart())
    assert store.get(task_id)["status"] == "completed"


# -- shutdown, restart and export ordering (runner driven directly) ------------

STUBBORN_WORKER = "import signal, time; signal.signal(signal.SIGINT, signal.SIG_IGN); time.sleep(300)"
# Counts SIGINTs and needs 1.5s of clean-up after the first, like a real
# worker journaling run.end and deliverables.
COUNTING_WORKER = r'''
import signal, sys, time
from pathlib import Path
workdir = Path(sys.argv[1])
count = 0
def on_int(*_):
    global count
    count += 1
    (workdir / "sigints").write_text(str(count))
signal.signal(signal.SIGINT, on_int)
(workdir / "ready").write_text("1")
done_at = None
while True:
    time.sleep(0.05)
    if count and done_at is None:
        done_at = time.monotonic() + 1.5
    if done_at and time.monotonic() > done_at:
        sys.exit(0)
'''


def _runner(tmp_path: Path, monkeypatch, *, shutdown_grace_s: int = 25, script: str = FAKE_WORKER):
    from agent_service.runner import Runner
    from agent_service.store import TaskStore

    worker = tmp_path / "worker.py"
    worker.write_text(script, encoding="utf-8")
    monkeypatch.setattr(engine, "build_command", lambda *, task, mode, workdir, max_turns:
                        [sys.executable, str(worker), str(workdir), task, "--cwd", str(workdir), "--"])
    cfg = ServiceConfig(data_dir=tmp_path / "svc", api_token="t", max_concurrency=1, task_timeout_s=120,
                        cancel_grace_s=60, default_mode="react", max_task_chars=1000,
                        shutdown_grace_s=shutdown_grace_s)
    store = TaskStore(cfg.db_path)
    return Runner(store, cfg), store, cfg


def _add(store, cfg, task: str) -> str:
    row, _ = store.create(task=task, mode="react", request_id=None, max_turns=None, workdir_root=cfg.tasks_root)
    return row["id"]


async def _until(store, task_id: str, status: str, timeout: float = 20) -> None:
    import asyncio

    deadline = time.monotonic() + timeout
    while store.get(task_id)["status"] != status:
        assert time.monotonic() < deadline, store.get(task_id)
        await asyncio.sleep(0.1)


async def test_shutdown_keeps_reason_and_starts_nothing_new(tmp_path: Path, monkeypatch) -> None:
    import asyncio

    from agent_service.runner import _alive

    runner, store, cfg = _runner(tmp_path, monkeypatch, shutdown_grace_s=10)
    await runner.start()
    running, waiting = _add(store, cfg, "sleep a"), _add(store, cfg, "sleep b")
    runner.submit(running)
    runner.submit(waiting)
    await _until(store, running, "running")
    await asyncio.sleep(1.0)                     # let the worker open its journal
    await runner.stop()
    await asyncio.sleep(0.5)                     # a worker would pick up the next task here
    row = store.get(running)
    assert (row["status"], row["error_code"]) == ("cancelled", "service_shutdown")
    queued = store.get(waiting)
    assert queued["status"] == "queued" and queued["pid"] is None
    assert not _alive(row["pid"])


async def test_shutdown_cuts_a_slow_cancel_short(tmp_path: Path, monkeypatch) -> None:
    from agent_service.runner import _alive

    runner, store, cfg = _runner(tmp_path, monkeypatch, shutdown_grace_s=4, script=STUBBORN_WORKER)
    await runner.start()
    task_id = _add(store, cfg, "x")
    runner.submit(task_id)
    await _until(store, task_id, "running")
    await runner.cancel(task_id)                 # 60s cancel grace; the worker ignores SIGINT
    started = time.monotonic()
    await runner.stop()
    assert time.monotonic() - started < cfg.shutdown_grace_s + 1
    assert not _alive(store.get(task_id)["pid"])


async def _wait_for_file(path: Path, timeout: float = 10) -> None:
    import asyncio

    deadline = time.monotonic() + timeout
    while not path.exists():
        assert time.monotonic() < deadline, f"{path} never appeared"
        await asyncio.sleep(0.05)


async def test_shutdown_never_interrupts_a_worker_twice(tmp_path: Path, monkeypatch) -> None:
    """A second SIGINT would make the worker's asyncio.run raise
    KeyboardInterrupt in the middle of writing run.end."""
    runner, store, cfg = _runner(tmp_path, monkeypatch, shutdown_grace_s=10, script=COUNTING_WORKER)
    await runner.start()
    task_id = _add(store, cfg, "x")
    runner.submit(task_id)
    workdir = Path(store.get(task_id)["workdir"])
    await _wait_for_file(workdir / "ready")
    await runner.cancel(task_id)
    await _wait_for_file(workdir / "sigints")    # the cancel's SIGINT arrived; clean-up runs
    await runner.stop()                          # replaces the cancel's termination
    assert (workdir / "sigints").read_text() == "1"
    row = store.get(task_id)
    assert (row["status"], row["error_code"], row["exit_code"]) == ("cancelled", "cancelled", 0)


async def test_shutdown_stops_a_worker_that_was_still_spawning(tmp_path: Path, monkeypatch) -> None:
    import asyncio
    import signal

    from agent_service.runner import _alive

    runner, store, cfg = _runner(tmp_path, monkeypatch, shutdown_grace_s=10, script=STUBBORN_WORKER)
    real_spawn = asyncio.create_subprocess_exec
    spawned: list[asyncio.subprocess.Process] = []
    in_flight = asyncio.Event()

    async def slow_spawn(*args, **kwargs):
        proc = await real_spawn(*args, **kwargs)
        spawned.append(proc)
        in_flight.set()
        await asyncio.sleep(0.5)                 # forked, not yet handed back to the runner
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", slow_spawn)
    await runner.start()
    task_id = _add(store, cfg, "x")
    runner.submit(task_id)
    await in_flight.wait()
    try:
        await runner.stop()
        assert not _alive(spawned[0].pid)
        row = store.get(task_id)
        assert (row["status"], row["pid"]) == ("queued", None)   # runs again after the restart
    finally:
        if spawned and spawned[0].returncode is None:
            os.killpg(spawned[0].pid, signal.SIGKILL)


async def test_restart_picks_up_rows_however_old(tmp_path: Path, monkeypatch) -> None:
    import sqlite3

    runner, store, cfg = _runner(tmp_path, monkeypatch)
    first, second = _add(store, cfg, "q1"), _add(store, cfg, "q2")
    unexported = _add(store, cfg, "e")
    store.update(unexported, status="completed", exported=0)
    # Many newer finished tasks in front of them.
    with sqlite3.connect(cfg.db_path) as con:
        con.executemany(
            "INSERT INTO tasks (id, request_id, mode, task, status, created_at, workdir) "
            "VALUES (?, ?, 'react', 't', 'completed', ?, '/nonexistent')",
            [(f"f{i}", f"f{i}", f"2999-01-01T00:00:00.{i:06d}") for i in range(10_001)],
        )
    await runner._recover_after_restart()
    assert [runner._queue.get_nowait(), runner._queue.get_nowait()] == [first, second]
    assert runner._queue.empty()
    assert unexported in runner._export_backlog


async def test_restart_keeps_the_reason_a_task_was_being_stopped_for(tmp_path: Path, monkeypatch) -> None:
    runner, store, cfg = _runner(tmp_path, monkeypatch)
    ids = {}
    for code in ("service_shutdown", "cancelled", "timeout"):
        ids[code] = _add(store, cfg, code)
        store.update(ids[code], status="cancelling", error_code=code, pid=2 ** 22 + 77)
    await runner._recover_after_restart()
    got = {code: (store.get(i)["status"], store.get(i)["error_code"]) for code, i in ids.items()}
    assert got == {"service_shutdown": ("cancelled", "service_shutdown"),
                   "cancelled": ("cancelled", "cancelled"), "timeout": ("timed_out", "timeout")}


async def test_restart_survives_an_unreadable_journal(tmp_path: Path, monkeypatch) -> None:
    runner, store, cfg = _runner(tmp_path, monkeypatch)
    bad, queued = _add(store, cfg, "bad"), _add(store, cfg, "next")
    rd = Path(store.get(bad)["workdir"]) / ".apodex" / "runs" / "s1"
    rd.mkdir(parents=True)
    (rd / "journal.db").write_bytes(b"not a database" * 100)
    store.update(bad, status="running", pid=2 ** 22 + 78)
    await runner._recover_after_restart()
    row = store.get(bad)
    assert (row["status"], row["error_code"]) == ("failed", "service_restart")
    assert runner._queue.get_nowait() == queued


async def test_final_state_does_not_wait_for_langfuse(tmp_path: Path, monkeypatch) -> None:
    import asyncio

    from agent_service import runner as runner_mod

    async def slow_export(workdir, exporters, *, final):
        await asyncio.sleep(30)
        return False

    monkeypatch.setattr(runner_mod, "_export_once", slow_export)
    runner, store, cfg = _runner(tmp_path, monkeypatch)
    await runner.start()
    task_id = _add(store, cfg, "quick")
    runner.submit(task_id)
    await _until(store, task_id, "completed", timeout=15)
    assert store.get(task_id)["exported"] == 0
    await runner.stop()
    # The owed export survives a restart.
    again, _, _ = _runner(tmp_path, monkeypatch)
    await again._recover_after_restart()
    assert task_id in again._export_backlog


def test_classify_keeps_the_shutdown_reason() -> None:
    from agent_service.runner import _classify

    crashed = engine.RunResult(status="cancelled")
    row = {"status": "cancelling", "error_code": "service_shutdown"}
    assert _classify(crashed, 130, row, forced=None, timed_out=False) == ("cancelled", "service_shutdown")


def test_is_our_worker_handles_paths_with_spaces(tmp_path: Path) -> None:
    import subprocess

    from agent_service.runner import _is_our_worker

    workdir = tmp_path / "data dir" / "tasks" / "abc"
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)", "--cwd", str(workdir), "--", "t"])
    try:
        time.sleep(0.3)
        assert _is_our_worker(proc.pid, workdir) is True
        assert _is_our_worker(proc.pid, tmp_path / "data dir" / "tasks" / "ab") is False
    finally:
        proc.kill()
        proc.wait()


def test_default_data_dir_needs_a_home(monkeypatch) -> None:
    from agent_service import config

    monkeypatch.delenv("XDG_DATA_HOME", raising=False)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: (_ for _ in ()).throw(RuntimeError("no home"))))
    with pytest.raises(RuntimeError, match="SERVICE_DATA_DIR"):
        config._default_data_dir()
