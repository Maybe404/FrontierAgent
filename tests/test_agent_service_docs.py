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

_DOC_PATHS = ("/docs", "/openapi.json", "/openapi.zh-CN.json")


def _cfg(tmp_path: Path, **docs_settings) -> ServiceConfig:
    return ServiceConfig(data_dir=tmp_path / "svc", api_token="t0ken", max_concurrency=1, task_timeout_s=60,
                         cancel_grace_s=5, default_mode="react", max_task_chars=1000, **docs_settings)


@pytest.fixture()
def client(tmp_path: Path):
    with TestClient(create_app(_cfg(tmp_path))) as c:
        yield c


@pytest.fixture()
def spec(client) -> dict:
    return client.get("/openapi.json").json()


@pytest.mark.parametrize("locale", list(docs.LOCALES))
def test_overlay_is_complete_and_up_to_date(spec, locale) -> None:
    # Fails when an endpoint or field is added without a translation, when the
    # English text changes without the translation, or when a target is gone.
    overlay = docs.load_overlay(locale)
    problems = docs.check_overlay(spec, overlay)
    assert not problems, (
        "\n".join(problems)
        + f"\n\nFix agent_service/openapi/{locale}.yaml (see AGENTS.md). For each target below, replace its "
        "action if one exists, otherwise append it under `actions:`, then fill in every empty `update` "
        "value; keep `x-source` exactly as given.\n\n" + docs.overlay_stub(spec, overlay)
    )


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
    assert "target not found (delete this action): $.paths['/v1/tasks'].post.responses['413']" in problems
    assert "untranslated: $.components.schemas.Task.properties.new_field description" in problems


def test_stub_lists_only_what_to_fix_and_filling_it_passes(spec) -> None:
    import yaml

    overlay = docs.load_overlay("zh-CN")
    assert docs.overlay_stub(spec, overlay) == ""

    changed = copy.deepcopy(spec)
    changed["paths"]["/v1/tasks"]["post"]["summary"] = "Submit an agent task"
    changed["components"]["schemas"]["Task"]["properties"]["new_field"] = {"description": "New."}
    stubs = yaml.safe_load(docs.overlay_stub(changed, overlay))
    by_target = {s["target"]: s for s in stubs}
    assert set(by_target) == {"$.paths['/v1/tasks'].post", "$.components.schemas.Task.properties.new_field"}
    post = by_target["$.paths['/v1/tasks'].post"]
    assert post["update"]["summary"] == "" and post["x-source"]["summary"] == "Submit an agent task"
    assert post["update"]["description"].startswith("将任务加入队列")   # still-valid translation kept

    # An empty value counts as untranslated and never blanks the page.
    actions = [a for a in overlay["actions"] if a["target"] not in by_target] + stubs
    assert "untranslated: $.paths['/v1/tasks'].post summary" in docs.check_overlay(changed, {"actions": actions})
    assert docs.apply_overlay(changed, {"actions": actions})["paths"]["/v1/tasks"]["post"]["summary"] == \
        "Submit an agent task"

    for s in stubs:
        s["update"] = {k: v or "译文" for k, v in s["update"].items()}
    assert docs.check_overlay(changed, {"actions": actions}) == []


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
    # Relative, so the page also works behind a gateway path prefix.
    assert '"url": "openapi.json"' in html and '"url": "openapi.zh-CN.json"' in html
    for off in ('"agent": {"disabled": true}', '"mcp": {"disabled": true}', '"telemetry": false'):
        assert off in html
    assert client.get("/redoc").status_code == 404


def test_docs_off_removes_every_docs_route_but_not_the_api(tmp_path: Path) -> None:
    with TestClient(create_app(_cfg(tmp_path, docs="off"))) as c:
        for path in (*_DOC_PATHS, "/redoc"):
            assert c.get(path).status_code == 404, path
        assert c.get("/v1/tasks", headers={"Authorization": "Bearer t0ken"}).status_code == 200


def test_docs_password_guards_docs_only(tmp_path: Path) -> None:
    with TestClient(create_app(_cfg(tmp_path, docs_password="read-only"))) as c:
        for path in _DOC_PATHS:
            r = c.get(path)
            assert r.status_code == 401 and r.headers["www-authenticate"].startswith("Basic"), path
            assert c.get(path, auth=("anyone", "wrong")).status_code == 401, path
            assert c.get(path, auth=("t0ken", "t0ken")).status_code == 401, path   # API token is not it
            assert c.get(path, auth=("anyone", "read-only")).status_code == 200, path
        # The docs password does not unlock the API, and the API token still does.
        assert c.get("/v1/tasks", auth=("anyone", "read-only")).status_code == 401
        assert c.get("/v1/tasks", headers={"Authorization": "Bearer t0ken"}).status_code == 200


def test_try_it_can_be_hidden(tmp_path: Path) -> None:
    with TestClient(create_app(_cfg(tmp_path))) as c:
        assert '"hideTestRequestButton": false' in c.get("/docs").text
    with TestClient(create_app(_cfg(tmp_path, docs_try_it=False))) as c:
        html = c.get("/docs").text
        assert '"hideTestRequestButton": true' in html and '"hideClientButton": true' in html


def test_docs_settings_from_env(monkeypatch) -> None:
    for name in ("SERVICE_DOCS", "SERVICE_DOCS_PASSWORD", "SERVICE_DOCS_TRY_IT"):
        monkeypatch.delenv(name, raising=False)
    cfg = ServiceConfig.from_env()
    assert (cfg.docs, cfg.docs_password, cfg.docs_try_it) == ("on", "", True)
    monkeypatch.setenv("SERVICE_DOCS", "OFF")
    monkeypatch.setenv("SERVICE_DOCS_PASSWORD", " pw ")
    monkeypatch.setenv("SERVICE_DOCS_TRY_IT", "0")
    cfg = ServiceConfig.from_env()
    assert (cfg.docs, cfg.docs_password, cfg.docs_try_it) == ("off", "pw", False)
    monkeypatch.setenv("SERVICE_DOCS", "maybe")
    with pytest.raises(ValueError, match="SERVICE_DOCS"):
        ServiceConfig.from_env()


def test_translated_document_ignores_query_parameters(client) -> None:
    # Routes take no parameters, so a query string can never pick the overlay file.
    r = client.get("/openapi.zh-CN.json", params={"locale": "../../pyproject"})
    assert r.status_code == 200 and r.json()["paths"]["/v1/tasks"]["post"]["summary"] == "提交任务"
