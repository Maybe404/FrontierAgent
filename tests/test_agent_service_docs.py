"""agent_service.docs: translated OpenAPI documents and the /docs page."""

from __future__ import annotations

import copy
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from agent_service import docs
from agent_service.api import create_app
from agent_service.config import ServiceConfig


@pytest.fixture()
def client(tmp_path: Path):
    cfg = ServiceConfig(data_dir=tmp_path / "svc", api_token="t0ken", max_concurrency=1,
                        task_timeout_s=60, cancel_grace_s=5, default_mode="react", max_task_chars=1000)
    with TestClient(create_app(cfg)) as c:
        yield c


@pytest.fixture()
def spec(client) -> dict:
    return client.get("/openapi.json").json()


@pytest.mark.parametrize("locale", list(docs.LOCALES))
def test_overlay_is_complete_and_up_to_date(spec, locale) -> None:
    # Fails when an endpoint or field is added without a translation, when the
    # English text changes without the translation, or when a target is gone.
    assert docs.check_overlay(spec, docs.load_overlay(locale)) == []


def test_translated_document_changes_only_text(client, spec) -> None:
    zh = client.get("/openapi.zh-CN.json").json()
    assert zh["paths"]["/v1/tasks"]["post"]["summary"] == "提交任务"
    assert zh["components"]["schemas"]["Task"]["properties"]["status"]["description"] == "当前生命周期状态。"

    def strip(node):
        if isinstance(node, dict):
            return {k: strip(v) for k, v in node.items() if k not in docs.TEXT_KEYS}
        if isinstance(node, list):
            return [strip(v) for v in node]
        return node

    assert strip(zh) == strip(spec)


def test_stale_translation_falls_back_to_english_and_is_reported(spec) -> None:
    changed = copy.deepcopy(spec)
    changed["paths"]["/v1/tasks"]["post"]["summary"] = "Submit an agent task"
    overlay = docs.load_overlay("zh-CN")
    assert "stale (English changed): $.paths['/v1/tasks'].post summary" in docs.check_overlay(changed, overlay)
    assert docs.apply_overlay(changed, overlay)["paths"]["/v1/tasks"]["post"]["summary"] == "Submit an agent task"


def test_missing_target_and_untranslated_text_are_reported(spec) -> None:
    changed = copy.deepcopy(spec)
    changed["paths"]["/v1/tasks"]["post"]["responses"].pop("413")
    changed["components"]["schemas"]["Task"]["properties"]["status"]["description"] = "Changed."
    changed["components"]["schemas"]["Task"]["properties"]["new_field"] = {"description": "New."}
    problems = docs.check_overlay(changed, docs.load_overlay("zh-CN"))
    assert "target not found: $.paths['/v1/tasks'].post.responses['413']" in problems
    assert "untranslated: $.components.schemas.Task.properties.new_field description" in problems


def test_resolve_supports_keys_quoted_keys_and_filters() -> None:
    doc = {"paths": {"/a/{id}": {"get": {"parameters": [{"name": "id", "x": 1}, {"name": "q", "x": 2}]}}}}
    assert docs.resolve(doc, "$.paths['/a/{id}'].get.parameters[?(@.name=='q')]") == {"name": "q", "x": 2}
    assert docs.resolve(doc, "$.paths['/b'].get") is None
    with pytest.raises(ValueError):
        docs.resolve(doc, "$.paths[*]")


def test_docs_page_is_scalar_with_every_language_and_no_ai_upload(client) -> None:
    r = client.get("/docs")
    assert r.status_code == 200 and "text/html" in r.headers["content-type"]
    html = r.text
    assert "@scalar/api-reference@" in html
    assert '"url": "/openapi.json"' in html and '"url": "/openapi.zh-CN.json"' in html
    for off in ('"agent": {"disabled": true}', '"mcp": {"disabled": true}', '"telemetry": false'):
        assert off in html
    assert client.get("/redoc").status_code == 404
