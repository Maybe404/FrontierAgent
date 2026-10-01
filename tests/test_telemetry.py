"""Run journal (frontier_agent.telemetry): completeness, failure isolation,
concurrency, span assembly and injection."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from agent_core.loop_types import AgentLoopResult, Intervention, LoopConfig, ToolResult, TurnContext

from frontier_agent.telemetry import langfuse_export as lf
from frontier_agent.telemetry.inject import with_telemetry
from frontier_agent.telemetry.observer import InterventionRecorder, TelemetryObserver
from frontier_agent.telemetry.run import INLINE_LIMIT, TelemetryRun, current_agent_span, current_run
from frontier_agent.telemetry.scope import telemetry_run


def _entries(run: TelemetryRun) -> list[dict]:
    return lf.read_entries(run.run_dir, run.session_id, run.run_id)


def _ctx(turn: int = 1, **kw) -> TurnContext:
    base = dict(turn=turn, max_turns=10, task_id="t", role_id="r", ai_text="hi", thinking="",
                tool_calls=[], messages=[{"role": "user", "content": "q"}], usage=None, metadata={})
    base.update(kw)
    return TurnContext(**base)


async def test_events_are_gap_free_and_large_fields_move_to_blobs(tmp_path: Path) -> None:
    run = TelemetryRun(tmp_path, session_id="s1")
    await run.start(task="task")
    await run.emit("custom", {"big": "x" * (INLINE_LIMIT + 1), "small": "y"})
    await run.end(status="completed")

    entries = _entries(run)
    assert [e["seq"] for e in entries] == [1, 2, 3]
    big = entries[1]["data"]["big"]
    assert big["bytes"] == INLINE_LIMIT + 1
    assert lf.blob_text(tmp_path, big["blob"]) == "x" * (INLINE_LIMIT + 1)
    assert entries[1]["data"]["small"] == "y"
    assert entries[-1]["data"]["complete"] is True
    assert not (tmp_path / "run.lock").exists()


async def test_write_failure_never_raises_and_marks_run_incomplete(tmp_path: Path, monkeypatch) -> None:
    run = TelemetryRun(tmp_path, session_id="s1")
    await run.start(task="task")
    real_append = run._ctx.journal.append

    async def failing(entry):
        if entry.kind == "doomed":
            raise OSError("disk full")
        return await real_append(entry)

    monkeypatch.setattr(run._ctx.journal, "append", failing)
    await run.emit("doomed", {})          # must not raise
    await run.end(status="completed")

    entries = _entries(run)
    end = entries[-1]["data"]
    assert end["complete"] is False
    assert end["lost_seqs"] == [2]
    # The gap is visible to the integrity check, not silently renumbered.
    assert [e["seq"] for e in entries] == [1, 3]


async def test_concurrent_agents_keep_one_contiguous_sequence(tmp_path: Path) -> None:
    run = TelemetryRun(tmp_path, session_id="s1")
    await run.start(task="task")

    async def agent(i: int) -> None:
        for j in range(20):
            await run.emit("tick", {"agent": i, "j": j}, agent_id=f"a{i}")

    await asyncio.gather(*(agent(i) for i in range(50)))
    await run.end(status="completed")

    seqs = [e["seq"] for e in _entries(run)]
    assert seqs == list(range(1, 50 * 20 + 3))


async def test_scope_records_error_status_and_reraises(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError):
        async with telemetry_run(run_dir=tmp_path, session_id="s1", task="t") as run:
            assert current_run.get() is run
            raise RuntimeError("boom")
    assert current_run.get() is None
    session_id, run_id = lf.list_runs(tmp_path)[0]
    end = lf.read_entries(tmp_path, session_id, run_id)[-1]
    assert end["kind"] == "run.end"
    assert end["data"]["status"] == "error"
    assert "boom" in end["data"]["error"]


async def test_nested_scope_joins_outer_run(tmp_path: Path) -> None:
    async with (
        telemetry_run(run_dir=tmp_path, session_id="s1", task="outer") as outer,
        telemetry_run(run_dir=tmp_path, session_id="s1", task="inner") as inner,
    ):
        assert inner is outer
    assert len(lf.list_runs(tmp_path)) == 1


async def test_injection_is_noop_without_a_run_and_chains_parents(tmp_path: Path) -> None:
    sentinel = object()
    assert with_telemetry([sentinel]) == [sentinel]

    async with telemetry_run(run_dir=tmp_path, session_id="s1", task="t") as run:
        main_obs = with_telemetry([])
        main = main_obs[0]
        assert isinstance(main, TelemetryObserver)
        assert main.parent_span_id == run.root_span_id
        await main.on_loop_start(LoopConfig(role_id="main"))
        # A sub-agent created while the main loop runs inherits it as parent.
        async def spawn_sub():
            return with_telemetry([])[0]

        sub = await asyncio.create_task(spawn_sub())
        assert sub.parent_span_id == main.span_id
        await main.on_loop_end(AgentLoopResult(messages=[], stopped_by="done"))
        assert current_agent_span.get() is None
        # Idempotent: an observer list that already has telemetry is untouched.
        assert with_telemetry(main_obs) is main_obs


async def test_intervention_recorder_journals_interventions(tmp_path: Path) -> None:
    class Guard:
        critical = True
        marker = "kept"

        async def on_turn_end(self, ctx):
            return Intervention(stop_reason="loop detected")

    async with telemetry_run(run_dir=tmp_path, session_id="s1", task="t"):
        _, wrapped, _tail = with_telemetry([Guard()])
        assert isinstance(wrapped, InterventionRecorder)
        assert wrapped.marker == "kept" and wrapped.critical is True
        rv = await wrapped.on_turn_end(_ctx(turn=3))
        assert rv.stop_reason == "loop detected"

    session_id, run_id = lf.list_runs(tmp_path)[0]
    ev = [e for e in lf.read_entries(tmp_path, session_id, run_id) if e["kind"] == "observer.intervention"]
    assert ev[0]["turn"] == 3
    assert ev[0]["data"]["observer"] == "Guard"
    assert ev[0]["data"]["intervention"] == {"stop_reason": "loop detected"}


async def _one_turn_run(tmp_path: Path, *, finish: bool) -> TelemetryRun:
    run = TelemetryRun(tmp_path, session_id="s1", workflow="react")
    await run.start(task="question")
    obs = TelemetryObserver(run, parent_span_id=run.root_span_id)
    await obs.on_loop_start(LoopConfig(role_id="main"))
    await obs.on_llm_input(_ctx())
    await obs.on_llm_response(_ctx(usage={"prompt_tokens": 10, "completion_tokens": 2}))
    call = {"id": "c1", "function": {"name": "web_search", "arguments": "{}"}}
    await obs.on_tool_call(_ctx(), call)
    if finish:
        await obs.on_tool_result(_ctx(), ToolResult(name="web_search", args={}, result="r",
                                                    duration_ms=5, tool_call_id="c1", is_error=False))
        await obs.on_loop_end(AgentLoopResult(messages=[], stopped_by="no_tool"))
        await run.end(status="completed")
    return run


async def test_spans_pair_start_and_end_events(tmp_path: Path) -> None:
    run = await _one_turn_run(tmp_path, finish=True)
    trace_id, spans = lf.build_spans(tmp_path, _entries(run), final=False)
    by_type = {s.attrs.get("langfuse.observation.type"): s for s in spans}
    assert set(by_type) == {"chain", "agent", "generation", "tool"}
    assert by_type["agent"].parent == by_type["chain"].span_id
    assert by_type["generation"].parent == by_type["agent"].span_id
    assert by_type["tool"].parent == by_type["agent"].span_id
    assert json.loads(by_type["generation"].attrs["langfuse.observation.usage_details"]) == {
        "input": 10, "output": 2}
    payload = lf._otlp(trace_id, spans)
    assert len(payload["resourceSpans"][0]["scopeSpans"][0]["spans"]) == 4


async def test_crashed_run_exports_open_spans_as_unfinished(tmp_path: Path) -> None:
    run = await _one_turn_run(tmp_path, finish=False)
    _, live = lf.build_spans(tmp_path, _entries(run), final=False)
    assert {s.attrs.get("langfuse.observation.type") for s in live} == {"generation"}
    _, final = lf.build_spans(tmp_path, _entries(run), final=True)
    open_ones = [s for s in final if s.attrs.get("langfuse.observation.metadata.unfinished")]
    assert {s.attrs["langfuse.observation.type"] for s in open_ones} == {"chain", "agent", "tool"}
    assert all(s.level == "WARNING" for s in open_ones)


def test_check_command_flags_missing_run_end(tmp_path: Path, capsys) -> None:
    from frontier_agent.telemetry.__main__ import check

    asyncio.run(_one_turn_run(tmp_path, finish=False))
    assert check(tmp_path) == 1
    assert "NO run.end" in capsys.readouterr().out


async def test_stale_lock_from_dead_process_gets_synthetic_run_end(tmp_path: Path) -> None:
    from frontier_agent.telemetry.run import recover_stale

    run = await _one_turn_run(tmp_path, finish=False)
    lock = json.loads((tmp_path / "run.lock").read_text())
    lock["pid"] = 2 ** 22 + 12345          # no such process
    (tmp_path / "run.lock").write_text(json.dumps(lock))

    assert await recover_stale(tmp_path) == run.run_id
    end = _entries(run)[-1]
    assert end["kind"] == "run.end"
    assert end["data"]["status"] == "crashed" and end["data"]["synthetic"] is True
    assert end["seq"] == _entries(run)[-2]["seq"] + 1
    assert not (tmp_path / "run.lock").exists()
    assert await recover_stale(tmp_path) is None


async def test_live_lock_is_left_alone(tmp_path: Path) -> None:
    from frontier_agent.telemetry.run import recover_stale

    await _one_turn_run(tmp_path, finish=False)       # lock holds our own pid
    assert await recover_stale(tmp_path) is None
    assert (tmp_path / "run.lock").exists()


async def test_deliverables_and_final_answer_are_journaled(tmp_path: Path) -> None:
    outputs = tmp_path / "outputs"
    (outputs / "sub").mkdir(parents=True)
    (outputs / "old.txt").write_text("from an earlier run", encoding="utf-8")

    async with telemetry_run(run_dir=tmp_path, session_id="s1", task="t", outputs_dir=outputs) as run:
        (outputs / "report.md").write_text("# 报告\n结论", encoding="utf-8")
        (outputs / "sub" / "data.csv").write_text("a,b\n1,2\n", encoding="utf-8")
        obs = with_telemetry([])[0]
        await obs.on_loop_start(LoopConfig(role_id="main"))
        await obs.on_loop_end(AgentLoopResult(messages=[], final_content="最终答案", stopped_by="no_tool"))

    entries = _entries(run)
    files = {e["data"]["path"]: e["data"] for e in entries if e["kind"] == "deliverable"}
    assert set(files) == {"report.md", "sub/data.csv"}
    assert lf.blob_text(tmp_path, files["report.md"]["sha256"]) == "# 报告\n结论"
    assert entries[-1]["kind"] == "run.end"
    assert entries[-1]["data"]["output"] == "最终答案"


def test_redaction_masks_secrets_and_personal_data(monkeypatch) -> None:
    from frontier_agent.telemetry import redact as rd

    monkeypatch.setenv("SOME_SERVICE_TOKEN", "tok-0123456789abcdef")
    rd._env_secrets.cache_clear()
    text = ("key sk-abcdefghijklmnop1234 auth Bearer abcdefghijklmnopqrstuv "
            "mail a.b@example.com tel 13812345678 id 11010519491231002X "
            "env tok-0123456789abcdef ok 2026")
    out = rd.redact(text)
    for leaked in ("sk-abcdefghijklmnop1234", "abcdefghijklmnopqrstuv", "a.b@example.com",
                   "13812345678", "11010519491231002X", "tok-0123456789abcdef"):
        assert leaked not in out
    assert out.endswith("ok 2026")
    rd._env_secrets.cache_clear()


def test_archive_then_prune_only_deletes_archived_closed_runs(tmp_path: Path) -> None:
    import os
    import time

    from frontier_agent.telemetry import retention as rt

    root = tmp_path / "runs"
    for name in ("old", "open", "new"):
        (root / name).mkdir(parents=True)
        (root / name / "journal.db").write_bytes(b"x" * 100)
    (root / "open" / "run.lock").write_text("{}")
    old = time.time() - 30 * 86400
    for name in ("old", "open"):
        os.utime(root / name / "journal.db", (old, old))

    store = rt.LocalArchive(tmp_path / "archive")
    runs = rt.scan(root)
    chosen = rt.select_for_prune(runs, older_than_days=7, max_total_bytes=None)
    assert [r.path.name for r in chosen] == ["old"]
    assert not rt.is_archived(chosen[0], store)
    rt.archive_run(chosen[0], store, tmp_path / "tmp")
    assert rt.is_archived(chosen[0], store)
    assert list((tmp_path / "archive").rglob("old.tar.gz"))


async def test_stress_100_agents_concurrently(tmp_path: Path) -> None:
    """100 sub-agents x 30 events through real observers: gap-free, ordered, fast enough."""
    import time

    async with telemetry_run(run_dir=tmp_path, session_id="s1", task="stress") as run:
        main = with_telemetry([])[0]
        await main.on_loop_start(LoopConfig(role_id="main"))

        async def sub_agent(i: int) -> None:
            obs = with_telemetry([])[0]
            await obs.on_loop_start(LoopConfig(role_id=f"sub{i}", task_id=f"job{i}"))
            for turn in range(1, 11):
                await obs.on_llm_input(_ctx(turn=turn, messages=[{"role": "user", "content": f"q{i}"}]))
                await obs.on_llm_response(_ctx(turn=turn))
            await obs.on_loop_end(AgentLoopResult(messages=[], stopped_by="done"))

        started = time.monotonic()
        await asyncio.gather(*(asyncio.create_task(sub_agent(i)) for i in range(100)))
        elapsed = time.monotonic() - started
        await main.on_loop_end(AgentLoopResult(messages=[], stopped_by="done"))

    entries = _entries(run)
    assert [e["seq"] for e in entries] == list(range(1, len(entries) + 1))
    assert entries[-1]["data"]["complete"] is True
    starts = [e for e in entries if e["kind"] == "loop.start"]
    assert len(starts) == 101
    assert {e["parent_span_id"] for e in starts[1:]} == {main.span_id}
    # 100 agents x 22 events; generous bound, catches lock-contention regressions.
    assert elapsed < 60, f"journaling 2200 events took {elapsed:.1f}s"


async def test_benchmark_session_journals_into_trial_dir(tmp_path: Path, monkeypatch) -> None:
    from benchmarks.public.core.kernel_adapter import BenchmarkSession

    outputs = tmp_path / "outputs"
    outputs.mkdir()
    monkeypatch.setenv("FRONTIER_AGENT_OUTPUTS_DIR", str(outputs))

    async def fake_run(self, instruction, *, meta, pipeline_id, extra_input):
        observers = with_telemetry([]) or []   # empty when no run is open
        for obs in observers:
            await obs.on_loop_start(LoopConfig(role_id="bench"))
        (outputs / "answer.txt").write_text("42")
        for obs in observers:
            await obs.on_loop_end(AgentLoopResult(messages=[], final_content="42", stopped_by="no_tool"))
        return {"final_answer": "42"}

    monkeypatch.setattr(BenchmarkSession, "_run", fake_run)
    session = BenchmarkSession.__new__(BenchmarkSession)
    state = await session.run("q?", meta={"_trial_dir": str(tmp_path / "trial"), "run_type": "bench"},
                              pipeline_id="stateful-react-agent")
    assert state == {"final_answer": "42"}
    journal = tmp_path / "trial" / "journal"
    (session_id, run_id), = lf.list_runs(journal)
    entries = lf.read_entries(journal, session_id, run_id)
    assert [e["kind"] for e in entries] == ["run.start", "loop.start", "loop.end", "deliverable", "run.end"]
    assert entries[-1]["data"]["output"] == "42"

    # Without a trial dir (and no open run) nothing is journaled.
    assert await session.run("q?", meta={}, pipeline_id="x") == {"final_answer": "42"}


async def test_reported_outcome_and_answer_win_over_defaults(tmp_path: Path) -> None:
    async with telemetry_run(run_dir=tmp_path, session_id="s1", task="t") as run:
        obs = with_telemetry([])[0]
        await obs.on_loop_start(LoopConfig(role_id="main"))
        await obs.on_loop_end(AgentLoopResult(messages=[], final_content="loop text", stopped_by="llm_error"))
        run.set_answer("workflow answer")
        run.set_outcome("failed", "llm_error: 503")
    end = _entries(run)[-1]["data"]
    assert end["status"] == "failed" and end["error"] == "llm_error: 503"
    assert end["output"] == "workflow answer"


async def test_hook_failure_is_counted_not_raised(tmp_path: Path, monkeypatch) -> None:
    async with telemetry_run(run_dir=tmp_path, session_id="s1", task="t") as run:
        obs = with_telemetry([])[0]

        async def broken_put(*a, **k):
            raise OSError("disk full")

        monkeypatch.setattr(run, "put_blob", broken_put)
        await obs.on_llm_input(_ctx())              # must not raise
    end = _entries(run)[-1]["data"]
    assert end["complete"] is False and end["telemetry_errors"] == 1


async def test_lease_decides_for_other_hosts(tmp_path: Path) -> None:
    import os

    from frontier_agent.telemetry.run import LEASE_TIMEOUT_S, recover_stale

    run = await _one_turn_run(tmp_path, finish=False)
    lock = tmp_path / "run.lock"
    info = json.loads(lock.read_text())
    info["host"] = "another-pod"
    lock.write_text(json.dumps(info))
    assert await recover_stale(tmp_path) is None          # fresh lease: still owned
    old = lock.stat().st_mtime - LEASE_TIMEOUT_S - 5
    os.utime(lock, (old, old))
    assert await recover_stale(tmp_path) == run.run_id    # lease expired


async def test_reused_pid_is_not_mistaken_for_the_owner(tmp_path: Path) -> None:
    from frontier_agent.telemetry.run import recover_stale

    run = await _one_turn_run(tmp_path, finish=False)
    lock = tmp_path / "run.lock"
    info = json.loads(lock.read_text())
    info["pid_started"] = "Thu Jan  1 00:00:00 1970"      # our pid, different process
    lock.write_text(json.dumps(info))
    assert await recover_stale(tmp_path) == run.run_id


async def test_tail_records_tool_result_as_the_model_sees_it(tmp_path: Path) -> None:
    class Truncator:
        critical = True

        async def on_tool_result(self, ctx, result):
            result.result = result.result[:3]
            return result

    async with telemetry_run(run_dir=tmp_path, session_id="s1", task="t") as run:
        head, trunc, tail = with_telemetry([Truncator()])
        await head.on_loop_start(LoopConfig(role_id="main"))
        tr = ToolResult(name="web_fetch", args={}, result="abcdef", duration_ms=1,
                        tool_call_id="c1", is_error=False)
        for o in (head, trunc, tail):            # the loop's dispatch order
            await o.on_tool_result(_ctx(), tr)
    kinds = {e["kind"]: e["data"] for e in _entries(run)}
    assert kinds["tool.result"]["result"] == "abcdef"
    assert kinds["tool.result.final"]["result"] == "abc"
    _, spans = lf.build_spans(tmp_path, _entries(run), final=True)
    tool = next(s for s in spans if s.name == "tool:web_fetch")
    assert tool.attrs["langfuse.observation.output"] == "abc"


def test_fake_ip_optin_is_limited_to_the_proxy_range(monkeypatch) -> None:
    from plugins.tools._download_runner import _local_fake_ip_networks

    monkeypatch.setenv("FRONTIER_AGENT_ALLOW_FAKE_IP_CIDRS", "0.0.0.0/0, 10.0.0.0/8, nonsense, 198.18.0.0/15")
    assert [str(n) for n in _local_fake_ip_networks()] == ["198.18.0.0/15"]


async def test_tool_span_waits_for_the_tail_before_export(tmp_path: Path) -> None:
    run = await _one_turn_run(tmp_path, finish=False)
    obs_entries = _entries(run)
    builder = lf.SpanBuilder(tmp_path)
    builder.feed(obs_entries)
    # simulate tool.result arriving; agent has not moved on yet
    agent_span = next(e["span_id"] for e in obs_entries if e["kind"] == "loop.start")
    tool_span = next(e["span_id"] for e in obs_entries if e["kind"] == "tool.call")
    base = {"trace_id": run.trace_id, "seq": 99, "agent_id": "a", "turn": 1}
    builder.feed([{**base, "entry_id": "x1", "kind": "tool.result", "ts": obs_entries[-1]["ts"],
                   "span_id": tool_span, "parent_span_id": agent_span, "data": {"result": "raw"}}])
    assert not any(s.name.startswith("tool:") for s in builder.take(set(), final=False))
    builder.feed([{**base, "entry_id": "x2", "kind": "tool.result.final", "ts": obs_entries[-1]["ts"],
                   "span_id": tool_span, "parent_span_id": agent_span, "data": {"result": "seen"}},
                  {**base, "entry_id": "x3", "kind": "turn.end", "ts": obs_entries[-1]["ts"],
                   "span_id": agent_span, "parent_span_id": run.root_span_id, "data": {}}])
    tool = next(s for s in builder.take(set(), final=False) if s.name.startswith("tool:"))
    assert tool.attrs["langfuse.observation.output"] == "seen"


async def test_unconfirmed_inflight_waits_out_the_grace_period(tmp_path: Path, monkeypatch) -> None:
    import time as _time

    ex = lf.Exporter(tmp_path, "s", "r")
    sent: set[str] = set()

    async def none_arrived(client, trace_id):
        return set()

    monkeypatch.setattr(lf, "_existing_ids", none_arrived)
    now = _time.time()
    keep = await ex._reconcile(None, sent, {"a": ["id-a", now - 5], "b": ["id-b", now - 1000]}, now)
    assert set(keep) == {"a"} and not sent          # a waits, b is released for resend

    async def failing(client, trace_id):
        raise OSError("langfuse down")

    monkeypatch.setattr(lf, "_existing_ids", failing)
    keep = await ex._reconcile(None, sent, {"a": ["id-a", now - 5]}, now)
    assert set(keep) == {"a"}                        # a failing check never blocks or drops


async def test_snapshot_failure_does_not_fail_the_task(tmp_path: Path, monkeypatch) -> None:
    def boom(_):
        raise PermissionError("denied")

    monkeypatch.setattr(TelemetryRun, "snapshot_outputs", staticmethod(boom))
    async with telemetry_run(run_dir=tmp_path, session_id="s1", task="t", outputs_dir=tmp_path / "out") as run:
        pass
    end = _entries(run)[-1]["data"]
    assert end["status"] == "completed" and end["complete"] is False
    assert run._heartbeat is None
