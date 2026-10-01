"""Service settings, read from the environment (and the repo .env)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def is_loopback(host: str) -> bool:
    return host in ("127.0.0.1", "::1", "localhost")


def _int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    return int(raw) if raw else default


@dataclass(frozen=True)
class ServiceConfig:
    data_dir: Path
    api_token: str
    max_concurrency: int
    task_timeout_s: int
    cancel_grace_s: int
    default_mode: str
    max_task_chars: int
    host: str = "127.0.0.1"
    # Pass the local-proxy fake-IP opt-in to workers. Only honoured for a
    # loopback-bound service with SERVICE_ALLOW_FAKE_IP=1 (local development).
    allow_fake_ip: bool = False
    # API reference at /docs, /openapi.json, /openapi.<locale>.json: "on" or
    # "off". A docs password puts them behind HTTP Basic auth (any user name);
    # it is separate from api_token, which can run tasks.
    docs: str = "on"
    docs_password: str = ""
    docs_try_it: bool = True

    def __post_init__(self) -> None:
        if self.docs not in ("on", "off"):
            raise ValueError(f"SERVICE_DOCS must be 'on' or 'off', got {self.docs!r}")

    @property
    def loopback(self) -> bool:
        return is_loopback(self.host)

    @property
    def tasks_root(self) -> Path:
        return self.data_dir / "tasks"

    @property
    def db_path(self) -> Path:
        return self.data_dir / "service.db"

    @classmethod
    def from_env(cls, host: str = "127.0.0.1") -> ServiceConfig:
        return cls(
            host=host,
            allow_fake_ip=is_loopback(host) and os.getenv("SERVICE_ALLOW_FAKE_IP", "") == "1",
            data_dir=Path(os.getenv("SERVICE_DATA_DIR", ".service")).expanduser().resolve(),
            api_token=os.getenv("SERVICE_API_TOKEN", "").strip(),
            max_concurrency=max(1, _int("SERVICE_MAX_CONCURRENCY", 2)),
            task_timeout_s=_int("SERVICE_TASK_TIMEOUT_S", 7200),
            cancel_grace_s=_int("SERVICE_CANCEL_GRACE_S", 60),
            default_mode=os.getenv("SERVICE_DEFAULT_MODE", "react"),
            max_task_chars=_int("SERVICE_MAX_TASK_CHARS", 20_000),
            docs=os.getenv("SERVICE_DOCS", "on").strip().lower() or "on",
            docs_password=os.getenv("SERVICE_DOCS_PASSWORD", "").strip(),
            docs_try_it=os.getenv("SERVICE_DOCS_TRY_IT", "1").strip() != "0",
        )
