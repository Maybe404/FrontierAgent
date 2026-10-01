"""Mask secrets and personal data before journal content leaves the machine.

Applied on export only: the local journal keeps the original text (it is the
source of truth and stays inside the run directory). Rules are deliberately
conservative regexes plus the literal values of configured secret env vars.
"""

from __future__ import annotations

import os
import re
from functools import lru_cache

# Env vars whose values are credentials wherever they appear in text.
_SECRET_ENV_HINTS = ("KEY", "TOKEN", "SECRET", "PASSWORD", "AUTH")
_MIN_SECRET_LEN = 8

_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("api_key", re.compile(r"\b(?:sk|pk|rk)-[A-Za-z0-9_\-]{16,}")),
    ("bearer", re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._\-~+/]{16,}=*")),
    ("jwt", re.compile(r"\beyJ[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}")),
    ("aws_key", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("private_key", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]+?-----END [A-Z ]*PRIVATE KEY-----")),
    ("email", re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")),
    # Mainland China resident ID (18 chars, last may be X) and mobile numbers.
    ("cn_id", re.compile(r"(?<!\d)\d{6}(?:19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{3}[\dXx](?!\d)")),
    ("cn_mobile", re.compile(r"(?<!\d)(?:\+?86[- ]?)?1[3-9]\d{9}(?!\d)")),
)


@lru_cache(maxsize=1)
def _env_secrets() -> tuple[str, ...]:
    values = {
        v for k, v in os.environ.items()
        if any(h in k.upper() for h in _SECRET_ENV_HINTS) and len(v) >= _MIN_SECRET_LEN
    }
    # Longest first so a secret containing another is masked whole.
    return tuple(sorted(values, key=len, reverse=True))


def enabled() -> bool:
    return os.getenv("FRONTIER_TELEMETRY_REDACT", "1").strip() not in ("0", "false", "off")


def redact(text: str) -> str:
    if not text or not enabled():
        return text
    for secret in _env_secrets():
        if secret in text:
            text = text.replace(secret, "[REDACTED:env]")
    for label, pattern in _PATTERNS:
        text = pattern.sub(f"[REDACTED:{label}]", text)
    return text
