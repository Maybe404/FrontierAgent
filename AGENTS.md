# Agent instructions

## HTTP API documentation (`agent_service/`)

The service documents itself: `/docs` renders `/openapi.json` (English) and
`/openapi.zh-CN.json` (Chinese) with Scalar. Nothing is written by hand
outside the code and one translation file.

**When you add or change an endpoint, a request/response field, or any of
their text, in the same change:**

1. Write the English in code. Fields: `Field(description=..., examples=[...])`
   in `agent_service/schemas.py`. Endpoints: `summary=`, `tags=`, `responses=`
   on the route and a docstring (it becomes the description) in
   `agent_service/api.py`. Return a model via `response_model=`, never an
   undocumented `dict`.
2. Update the Chinese in `agent_service/openapi/zh-CN.yaml`, an OpenAPI
   Overlay. Each action is
   ```yaml
   - target: $.components.schemas.Task.properties.status
     update:
       description: 当前生命周期状态。      # the Chinese
     x-source:
       description: Current lifecycle status.  # the exact English it translates
   ```
3. Run
   ```bash
   uv run --extra service --extra dev pytest tests/test_agent_service.py tests/test_agent_service_docs.py -q
   ```
   A missing, stale or orphaned translation fails the test, and the failure
   prints ready-to-paste actions with `x-source` already filled in: replace
   the action with the same `target` (or append it), fill every empty
   `update` value, and keep `x-source` exactly as printed. Delete actions
   reported as `target not found`.

Translation rules:

- Translate only `summary`, `description` and tag `x-displayName`. Never
  translate paths, field names, enum values, error codes, header names,
  environment variable names or anything in backticks.
- Terms: task 任务, deliverable 交付物, worker 工作进程, agent 智能体,
  idempotency key 幂等键, cursor 游标, run journal 运行日志（journal）,
  lifecycle status 生命周期状态, final status 终态.
- Keep Markdown structure (paragraphs, code blocks, bold labels) as in the
  English.

API documents are confidential. In `agent_service/docs.py`, keep Scalar's
`agent` and `mcp` disabled (both upload the whole document to Scalar's
servers) and `telemetry` off; do not add a `proxyUrl`.
