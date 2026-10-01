"""API reference at ``/docs``: the English OpenAPI document plus translations.

The English document generated from the code is canonical. Each other
language is an OpenAPI Overlay (``openapi/<locale>.yaml``) whose actions
replace only human-readable text (``summary``, ``description``, a tag's
``x-displayName``); paths, field names, types and enum values are never
translated.

Every action carries ``x-source``: the English text it translates. When the
English changes, ``check_overlay`` reports the translation as stale, so a
translation can never silently drift from the code.

Targets use a small JSONPath subset, enough to address any node of an
OpenAPI document: ``$.a.b``, ``$.a['/x/{y}']``, ``$.a[?(@.name=='v')]``.
"""

from __future__ import annotations

import copy
import json
import re
import secrets
from collections.abc import Callable
from functools import cache
from pathlib import Path
from typing import Annotated, Any

import yaml
from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from agent_service.config import ServiceConfig

OVERLAY_DIR = Path(__file__).parent / "openapi"
LOCALES = {"zh-CN": "中文"}
TEXT_KEYS = ("summary", "description", "x-displayName")

# Pinned so a docs page never changes without a code change.
_SCALAR_JS = "https://cdn.jsdelivr.net/npm/@scalar/api-reference@1.72.3"

_TOKEN = re.compile(r"\.([A-Za-z0-9_\-]+)|\['([^']*)'\]|\[\?\(@\.([A-Za-z0-9_]+)=='([^']*)'\)\]")


def _parse(target: str) -> list[tuple[str, str, str]]:
    """``(kind, a, b)`` per step: ``("key", name, "")`` or ``("filter", field, value)``."""
    if not target.startswith("$"):
        raise ValueError(f"target must start with '$': {target}")
    steps: list[tuple[str, str, str]] = []
    pos = 1
    while pos < len(target):
        m = _TOKEN.match(target, pos)
        if m is None:
            raise ValueError(f"unsupported target syntax at {target[pos:]!r} in {target}")
        if m.group(3) is not None:
            steps.append(("filter", m.group(3), m.group(4)))
        else:
            steps.append(("key", m.group(1) if m.group(1) is not None else m.group(2), ""))
        pos = m.end()
    return steps


def resolve(doc: Any, target: str) -> Any | None:
    """The node *target* names in *doc*, or None when it does not exist."""
    node = doc
    for kind, a, b in _parse(target):
        if kind == "key":
            if not isinstance(node, dict) or a not in node:
                return None
            node = node[a]
        else:
            if not isinstance(node, list):
                return None
            hits = [x for x in node if isinstance(x, dict) and x.get(a) == b]
            if len(hits) != 1:
                return None
            node = hits[0]
    return node


def _key(k: str) -> str:
    return f".{k}" if re.fullmatch(r"[A-Za-z0-9_\-]+", k) else f"['{k}']"


def translatable(spec: dict[str, Any]) -> dict[str, dict[str, str]]:
    """``{target: {key: english}}`` for every summary/description in *spec*."""
    out: dict[str, dict[str, str]] = {}

    def add(target: str, node: Any) -> None:
        if isinstance(node, dict):
            texts = {k: node[k] for k in TEXT_KEYS if isinstance(node.get(k), str) and node[k]}
            if texts:
                out[target] = texts

    def schema(target: str, node: Any) -> None:
        add(target, node)
        if isinstance(node, dict):
            for name, prop in (node.get("properties") or {}).items():
                schema(f"{target}.properties{_key(name)}", prop)

    add("$.info", spec.get("info"))
    for tag in spec.get("tags") or []:
        add(f"$.tags[?(@.name=='{tag['name']}')]", tag)
    for path, ops in (spec.get("paths") or {}).items():
        for method, op in ops.items():
            base = f"$.paths['{path}'].{method}"
            add(base, op)
            for p in op.get("parameters") or []:
                add(f"{base}.parameters[?(@.name=='{p['name']}')]", p)
            body = ((op.get("requestBody") or {}).get("content") or {})
            for media, content in body.items():
                schema(f"{base}.requestBody.content['{media}'].schema", content.get("schema"))
            for code, resp in (op.get("responses") or {}).items():
                add(f"{base}.responses['{code}']", resp)
    components = spec.get("components") or {}
    for name, node in (components.get("schemas") or {}).items():
        schema(f"$.components.schemas{_key(name)}", node)
    for name, node in (components.get("securitySchemes") or {}).items():
        add(f"$.components.securitySchemes{_key(name)}", node)
    return out


def load_overlay(locale: str) -> dict[str, Any]:
    return yaml.safe_load((OVERLAY_DIR / f"{locale}.yaml").read_text(encoding="utf-8"))


def apply_overlay(spec: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """A translated copy of *spec*. Actions whose target is gone or whose
    English changed are skipped, so the page falls back to English there."""
    doc = copy.deepcopy(spec)
    for action in overlay.get("actions") or []:
        node, original = resolve(doc, action["target"]), resolve(spec, action["target"])
        if not isinstance(node, dict) or not isinstance(original, dict):
            continue
        if any(original.get(k) != v for k, v in (action.get("x-source") or {}).items()):
            continue
        node.update({k: v for k, v in (action.get("update") or {}).items() if v})
    return doc


def check_overlay(spec: dict[str, Any], overlay: dict[str, Any]) -> list[str]:
    """Problems that make *overlay* incomplete or stale for *spec*; empty when it is up to date."""
    problems: list[str] = []
    seen: dict[str, set[str]] = {}
    for action in overlay.get("actions") or []:
        target = action["target"]
        node = resolve(spec, target)
        if not isinstance(node, dict):
            problems.append(f"target not found (delete this action): {target}")
            continue
        update, source = action.get("update") or {}, action.get("x-source") or {}
        if set(update) != set(source):
            problems.append(f"update and x-source keys differ: {target}")
        for k, v in source.items():
            if node.get(k) != v:
                problems.append(f"stale (English changed): {target} {k}")
        seen.setdefault(target, set()).update(k for k, v in update.items() if v)
    for target, texts in translatable(spec).items():
        for k in texts:
            if k not in seen.get(target, set()):
                problems.append(f"untranslated: {target} {k}")
    return problems


class _Dumper(yaml.SafeDumper):
    pass


def _str(dumper: yaml.SafeDumper, s: str) -> yaml.Node:
    return dumper.represent_scalar("tag:yaml.org,2002:str", s, style="|" if "\n" in s else None)


_Dumper.add_representer(str, _str)


def overlay_stub(spec: dict[str, Any], overlay: dict[str, Any]) -> str:
    """Ready-to-paste actions for every target whose translation is missing or
    stale: ``x-source`` holds the current English, ``update`` keeps translations
    that are still valid and leaves the rest empty to fill in."""
    actions = {a["target"]: a for a in overlay.get("actions") or []}
    stubs = []
    for target, english in translatable(spec).items():
        action = actions.get(target) or {}
        source, update = action.get("x-source") or {}, action.get("update") or {}
        if all(source.get(k) == v and update.get(k) for k, v in english.items()):
            continue
        keep = {k: update[k] for k, v in english.items() if source.get(k) == v and update.get(k)}
        stubs.append({"target": target, "update": {k: keep.get(k, "") for k in english}, "x-source": english})
    if not stubs:
        return ""
    return yaml.dump(stubs, Dumper=_Dumper, allow_unicode=True, sort_keys=False, width=1000)


def _page(title: str, configs: list[dict[str, Any]], *, try_it: bool) -> str:
    common = {
        # Both would upload the whole document to Scalar's servers.
        "agent": {"disabled": True},
        "mcp": {"disabled": True},
        "telemetry": False,
        "withDefaultFonts": False,
        "showDeveloperTools": "never",
        "hideTestRequestButton": not try_it,
        "hideClientButton": not try_it,
    }
    data = json.dumps([{**common, **c} for c in configs], ensure_ascii=False).replace("</", "<\\/")
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title}</title>
</head>
<body>
  <div id="app"></div>
  <script src="{_SCALAR_JS}"></script>
  <script>
    // Document URLs are relative to this page, so the docs work behind a
    // gateway path prefix; resolving against origin + pathname also drops any
    // user:password@ the page was opened with, which fetch() would reject.
    const configs = {data};
    const base = location.origin + location.pathname;
    for (const c of configs) c.url = new URL(c.url, base).href;
    Scalar.createApiReference('#app', configs);
  </script>
</body>
</html>"""


# Module level: annotations are strings here, and FastAPI resolves them
# against module globals, not a function's locals.
_basic = HTTPBasic(realm="API docs")


def _password_guard(password: str) -> Callable[..., None]:
    def guard(credentials: Annotated[HTTPBasicCredentials, Depends(_basic)]) -> None:
        if not secrets.compare_digest(credentials.password.encode(), password.encode()):
            raise HTTPException(401, "invalid docs password", headers={"WWW-Authenticate": 'Basic realm="API docs"'})

    return guard


def mount(app: FastAPI, cfg: ServiceConfig) -> None:
    """Serve ``/docs``, ``/openapi.json`` and ``/openapi.<locale>.json`` as *cfg* allows.

    The app must be built with ``openapi_url=None`` so that the English
    document gets the same switch and password as the rest."""
    if cfg.docs == "off":
        return
    deps = [Depends(_password_guard(cfg.docs_password))] if cfg.docs_password else []

    def route(path: str, doc: Callable[[], dict[str, Any]]) -> None:
        app.add_api_route(path, lambda: JSONResponse(doc()), include_in_schema=False, dependencies=deps)

    route("/openapi.json", app.openapi)
    for locale in LOCALES:
        translated = cache(lambda locale=locale: apply_overlay(app.openapi(), load_overlay(locale)))
        route(f"/openapi.{locale}.json", translated)

    configs = [{"title": "English", "slug": "en", "url": "openapi.json", "localization": {"locale": "en"}}]
    configs += [{"title": label, "slug": locale.lower(), "url": f"openapi.{locale}.json",
                 "localization": {"locale": locale}} for locale, label in LOCALES.items()]
    html = _page(app.title, configs, try_it=cfg.docs_try_it)
    app.add_api_route("/docs", lambda: HTMLResponse(html), include_in_schema=False, dependencies=deps)
