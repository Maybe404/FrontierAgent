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
    with TestClient(create_app(cfg)) as c:
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
