"""Adapter between the service and FrontierAgent.

The only module that knows how a task is executed (the ``frontier-agent``
CLI in native, non-interactive, auto-approve mode, one process per task) and
where its journal and deliverables land. When upstream changes the CLI,
run layout or journal format, this file changes and nothing else does.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

MODES = ("react", "agent_team", "research", "coding")


def build_command(*, task: str, mode: str, workdir: Path, max_turns: int | None) -> list[str]:
    cmd = [sys.executable, "-m", "apodex", "--native", "--print", "--yes", "--no-tui",
           "--mode", mode, "--cwd", str(workdir)]
    if max_turns:
        cmd += ["--max-turns", str(max_turns)]
    # "--" so a task starting with "-" is never parsed as an option.
    return [*cmd, "--", task]


# Workers get only what running an agent needs. Service credentials
# (SERVICE_API_TOKEN, LANGFUSE_*) never reach them: the agent auto-approves
# tools and reads untrusted web content, so its process holds as few secrets
# as possible. Export to Langfuse happens in the service process.
_ENV_PREFIXES = (
    "OPENAI_", "SUMMARY_LLM_", "SYNCO_SEARCH_", "SERPER_", "JINA_", "READDOC_",
    "APODEX_", "SANDBOX_", "BASH_", "OFFICEQA_", "LC_", "FRONTIER_AGENT_",
)
_ENV_NAMES = frozenset({
    "PATH", "HOME", "USER", "LOGNAME", "SHELL", "LANG", "TZ", "TMPDIR", "TERM",
    "PYTHONPATH", "VIRTUAL_ENV", "UV_CACHE_DIR", "UV_INDEX_URL", "UV_EXTRA_INDEX_URL",
    "PIP_INDEX_URL", "PIP_EXTRA_INDEX_URL",
    "SSL_CERT_FILE", "SSL_CERT_DIR", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE",
    "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "ALL_PROXY",
    "http_proxy", "https_proxy", "no_proxy", "all_proxy",
    "FRONTIER_TELEMETRY_REDACT",
})
# Security relaxations a worker must never inherit implicitly.
_ENV_DENY = frozenset({"FRONTIER_AGENT_ALLOW_FAKE_IP_CIDRS", "FRONTIER_AGENT_ALLOW_PRIVATE_FETCH"})
_FAKE_IP_VAR = "FRONTIER_AGENT_ALLOW_FAKE_IP_CIDRS"


def build_env(*, task_id: str, request_id: str, allow_fake_ip: bool = False,
              source: dict[str, str] | None = None) -> dict[str, str]:
    src = dict(os.environ if source is None else source)
    env = {k: v for k, v in src.items()
           if (k in _ENV_NAMES or k.startswith(_ENV_PREFIXES)) and k not in _ENV_DENY}
    if allow_fake_ip and src.get(_FAKE_IP_VAR):
        env[_FAKE_IP_VAR] = src[_FAKE_IP_VAR]
    env.update({
        "FRONTIER_TASK_ID": task_id,
        "FRONTIER_REQUEST_ID": request_id,
        # The service decides task results from the journal; never disable it.
        "FRONTIER_TELEMETRY": "1",
        # The CLI would otherwise load the nearest .env / user env file and
        # undo this allowlist.
        "APODEX_NO_DOTENV": "1",
        "APODEX_ENV_FILE": os.devnull,
        "PYTHONUNBUFFERED": "1",
        "NO_COLOR": "1",
    })
    return env


def run_dir(workdir: Path) -> Path | None:
    """The session directory the CLI created under *workdir* (one per task)."""
    runs = workdir / ".apodex" / "runs"
    if not runs.is_dir():
        return None
    dirs = sorted((d for d in runs.iterdir() if (d / "journal.db").exists()),
                  key=lambda d: d.stat().st_mtime)
    return dirs[-1] if dirs else None


def outputs_dir(workdir: Path) -> Path | None:
    rd = run_dir(workdir)
    return rd / "outputs" if rd else None


def session_ids(workdir: Path) -> list[tuple[Path, str, str]]:
    """``(run_dir, session_id, run_id)`` for each run journaled under *workdir*."""
    rd = run_dir(workdir)
    if rd is None:
        return []
    from frontier_agent.telemetry.langfuse_export import list_runs

    return [(rd, s, r) for s, r in list_runs(rd)]


@dataclass
class RunResult:
    status: str = ""            # completed | error | cancelled | crashed | "" (no run.end)
    answer: str = ""
    error: str = ""
    complete: bool = False
    deliverables: list[dict[str, Any]] = field(default_factory=list[dict[str, Any]])


def read_result(workdir: Path) -> RunResult:
    rd = run_dir(workdir)
    result = RunResult()
    if rd is None:
        return result
    with closing(sqlite3.connect(rd / "journal.db")) as con:
        rows = con.execute(
            "SELECT kind, payload_json FROM context_journal "
            "WHERE kind IN ('run.end', 'deliverable') ORDER BY sequence"
        ).fetchall()
    for kind, payload in rows:
        data = json.loads(payload).get("data") or {}
        if kind == "deliverable":
            result.deliverables.append({k: data.get(k) for k in ("path", "bytes", "sha256", "media_type", "error")})
        else:
            result.status = str(data.get("status") or "")
            answer = data.get("output")
            if isinstance(answer, dict) and "blob" in answer:
                answer = _blob(rd, answer["blob"])
            result.answer = str(answer or "")
            result.error = str(data.get("error") or "")
            result.complete = bool(data.get("complete"))
    return result


def _blob(rd: Path, digest: str) -> str:
    try:
        return (rd / "blobs" / digest[:2] / digest[2:]).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def read_events(workdir: Path, after: int = 0, limit: int = 500) -> list[dict[str, Any]]:
    """Journal events (all agents) after sequence *after*, oldest first."""
    rd = run_dir(workdir)
    if rd is None:
        return []
    with closing(sqlite3.connect(rd / "journal.db")) as con:
        rows = con.execute(
            "SELECT sequence, agent_id, kind, payload_json FROM context_journal "
            "WHERE sequence > ? ORDER BY sequence LIMIT ?", (after, limit),
        ).fetchall()
    return [{"cursor": r[0], "agent_id": r[1], "kind": r[2], **json.loads(r[3])} for r in rows]


async def recover(workdir: Path) -> None:
    """Close a run whose worker died before writing run.end."""
    rd = run_dir(workdir)
    if rd is None:
        return
    from frontier_agent.telemetry.run import recover_stale

    await recover_stale(rd)
