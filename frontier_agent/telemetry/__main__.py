"""Inspect and export run journals.

    python -m frontier_agent.telemetry runs    <run_dir>
    python -m frontier_agent.telemetry show    <run_dir> [--run RUN_ID] [--full]
    python -m frontier_agent.telemetry check   <run_dir>
    python -m frontier_agent.telemetry export  <run_dir> [--resend]
    python -m frontier_agent.telemetry verify  <run_dir>

``<run_dir>`` is ``<cwd>/.apodex/runs/<session-id>``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

from frontier_agent.telemetry import langfuse_export as lf


def _runs(run_dir: Path, only: str | None) -> list[tuple[str, str]]:
    runs = lf.list_runs(run_dir)
    return [r for r in runs if only in (None, r[1])]


def _line(e: dict[str, Any], full: bool) -> str:
    d = e.get("data") or {}
    brief = {k: v for k, v in d.items() if k in (
        "name", "stopped_by", "status", "outcome", "duration_ms", "ttft_ms",
        "is_error", "observer", "hook", "role_id", "tool_calls", "complete",
    )}
    if full:
        brief = d
    return (f"{e['seq']:>5} {e['ts'][11:23]} t{e.get('turn') if e.get('turn') is not None else '-':<3} "
            f"{e.get('agent_id', '')[:28]:<28} {e['kind']:<22} "
            f"{json.dumps(brief, ensure_ascii=False)[:300 if not full else 100000]}")


def check(run_dir: Path) -> int:
    """Integrity: per-run seq must be gap-free and every run must have run.end."""
    bad = 0
    for session_id, run_id in _runs(run_dir, None):
        entries = lf.read_entries(run_dir, session_id, run_id)
        seqs = [e["seq"] for e in entries]
        missing = sorted(set(range(1, max(seqs) + 1)) - set(seqs)) if seqs else []
        end = next((e for e in entries if e["kind"] == "run.end"), None)
        status = "ok"
        if missing:
            status = f"MISSING seq {missing[:20]}"
        if end is None:
            status += " | NO run.end (crashed or still running)"
        elif not end["data"].get("complete"):
            status += f" | incomplete: {end['data'].get('telemetry_errors')} write errors"
        bad += status != "ok"
        print(f"{run_id}: {len(entries)} events, {status}")
    return 1 if bad else 0


async def _observation_ids(client: Any, trace_id: str) -> set[str]:
    """Unique observation ids of a trace (Langfuse v4 v2 API, cursor-paged).

    Langfuse does not deduplicate re-sent spans, so ids are compared as a set.
    """
    ids: set[str] = set()
    cursor = None
    while True:
        params = {"traceId": trace_id, "limit": 1000, "fields": "core"}
        if cursor:
            params["cursor"] = cursor
        resp = await client.get("/api/public/v2/observations", params=params)
        resp.raise_for_status()
        body = resp.json()
        ids.update(o["id"] for o in body.get("data", []))
        cursor = (body.get("meta") or {}).get("cursor")
        if not cursor:
            return ids


async def verify(run_dir: Path) -> int:
    """Reconcile: every observation the journal implies exists in Langfuse."""
    import httpx

    bad = 0
    async with lf._client() as client:
        for session_id, run_id in _runs(run_dir, None):
            trace_id, expected = lf.expected_observation_ids(run_dir, session_id, run_id)
            try:
                got = await _observation_ids(client, trace_id)
            except httpx.HTTPError as exc:
                print(f"{run_id}: cannot read trace {trace_id}: {exc}")
                bad += 1
                continue
            missing = expected - got
            print(f"{run_id}: expected {len(expected)} observations, found {len(expected) - len(missing)}"
                  + (f", MISSING {len(missing)}" if missing else ", ok"))
            bad += bool(missing)
    return 1 if bad else 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m frontier_agent.telemetry")
    p.add_argument("command", choices=["runs", "show", "check", "export", "verify"])
    p.add_argument("run_dir", type=Path)
    p.add_argument("--run", default=None, help="run id (default: all runs in the directory)")
    p.add_argument("--full", action="store_true", help="show: print full event data")
    p.add_argument("--resend", action="store_true", help="export: resend spans already sent (Langfuse keeps both copies; debugging only)")
    a = p.parse_args(argv)

    if not (a.run_dir / "journal.db").exists():
        print(f"no journal.db in {a.run_dir}", file=sys.stderr)
        return 2
    if a.command == "runs":
        for session_id, run_id in _runs(a.run_dir, None):
            n = len(lf.read_entries(a.run_dir, session_id, run_id))
            print(f"{run_id}  events={n}  spans_sent={len(lf.load_sent(a.run_dir, run_id))}")
        return 0
    if a.command == "show":
        for session_id, run_id in _runs(a.run_dir, a.run):
            for e in lf.read_entries(a.run_dir, session_id, run_id):
                print(_line(e, a.full))
        return 0
    if a.command == "check":
        return check(a.run_dir)
    if not lf.configured():
        print("set LANGFUSE_HOST, LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY", file=sys.stderr)
        return 2
    if a.command == "export":
        for session_id, run_id in _runs(a.run_dir, a.run):
            n = asyncio.run(lf.export_run(a.run_dir, session_id, run_id, final=True, resend=a.resend))
            print(f"{run_id}: sent {n} spans")
        return 0
    return asyncio.run(verify(a.run_dir))


if __name__ == "__main__":
    sys.exit(main())
