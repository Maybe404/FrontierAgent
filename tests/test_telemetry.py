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
        _, wrapped = with_telemetry([Guard()])
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
