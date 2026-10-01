"""agent_service: submit/idempotency/auth, SSE, downloads, cancel, restart recovery.

The worker command is replaced by a tiny script that writes a real run
journal (via frontier_agent.telemetry), so no model or network is needed.
"""

from __future__ import annotations

import json
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
        if row["status"] in ("completed", "failed", "cancelled"):
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
    assert row["status"] == "cancelled"
    workdir = Path(client.app.state.store.get(task_id)["workdir"])
    assert engine.read_result(workdir).status == "cancelled"


def test_crashed_worker_is_failed_with_synthetic_run_end(client) -> None:
    task_id = client.post("/v1/tasks", json={"task": "crash now"}).json()["id"]
    row = _wait(client, task_id)
    assert row["status"] == "failed" and row["exit_code"] == 3
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
    assert after["status"] == "failed"
    assert "restarted" in after["error"]
    assert json.loads(json.dumps(after["deliverables"])) == []
