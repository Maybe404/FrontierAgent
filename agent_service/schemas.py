"""Request and response models of the HTTP API.

They are the source of the OpenAPI document served at ``/openapi.json`` and
rendered at ``/docs``: field descriptions written here are the API docs.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from agent_service.engine import MODES

TaskStatus = Literal["queued", "running", "cancelling", "completed", "failed", "cancelled", "timed_out"]

API_DESCRIPTION = """\
Run FrontierAgent tasks asynchronously.

Submit a task, then follow its progress over Server-Sent Events or poll its
status. When it reaches a final status, read the answer and download the
files it produced.

**Lifecycle:** `queued` → `running` → `completed` | `failed` | `cancelled` | `timed_out`.
`cancelling` is the transient state between a cancel request and the worker
exiting.

**Authentication:** when the service is configured with an API token, every
`/v1` endpoint requires `Authorization: Bearer <token>`.
"""

OPENAPI_TAGS = [
    {"name": "tasks", "description": "Submit, query and cancel agent tasks."},
    {"name": "results", "description": "Progress events and produced files."},
    {"name": "system", "description": "Service health."},
]


class SubmitRequest(BaseModel):
    task: str = Field(
        min_length=1,
        description="The task for the agent, in natural language. Length is capped by the "
        "service setting `SERVICE_MAX_TASK_CHARS` (413 when exceeded).",
        examples=["Summarise the attached quarterly report and list three risks."],
    )
    mode: str | None = Field(
        default=None,
        description=f"Agent mode, one of {', '.join(f'`{m}`' for m in MODES)}. "
        "Omit to use the service default (`SERVICE_DEFAULT_MODE`).",
        examples=["react"],
    )
    request_id: str | None = Field(
        default=None,
        max_length=200,
        description="Caller-chosen idempotency key. Submitting the same `request_id` again "
        "returns the existing task (`created: false`) instead of starting a new one. "
        "Defaults to the task id when omitted.",
        examples=["order-20261001-0001"],
    )
    max_turns: int | None = Field(
        default=None,
        ge=1,
        le=1000,
        description="Upper bound on agent turns. Omit to use the agent's own default.",
        examples=[50],
    )


class Deliverable(BaseModel):
    """A file the agent produced. Download it from `/v1/tasks/{task_id}/files/{path}`."""

    path: str | None = Field(default=None, description="Path relative to the task's outputs directory.",
                             examples=["report.docx"])
    bytes: int | None = Field(default=None, description="File size in bytes.", examples=[48213])
    sha256: str | None = Field(default=None, description="SHA-256 of the file content, hex encoded.")
    media_type: str | None = Field(
        default=None, description="MIME type of the file.",
        examples=["application/vnd.openxmlformats-officedocument.wordprocessingml.document"],
    )
    error: str | None = Field(default=None,
                              description="Why the file could not be recorded; null when it was.")


class Task(BaseModel):
    id: str = Field(description="Task id assigned by the service.")
    request_id: str = Field(description="Idempotency key: the caller's `request_id`, or the task id when none was given.")
    mode: str = Field(description="Agent mode the task runs in.", examples=["react"])
    status: TaskStatus = Field(description="Current lifecycle status.")
    created_at: str = Field(description="When the task was submitted (ISO 8601, UTC).",
                            examples=["2026-10-01T08:30:00.123456+00:00"])
    started_at: str | None = Field(default=None,
                                   description="When a worker started running it (ISO 8601, UTC); null while queued.")
    finished_at: str | None = Field(default=None,
                                    description="When it reached a final status (ISO 8601, UTC); null until then.")
    answer: str | None = Field(default=None,
                               description="The agent's final answer. Set once the task is final; "
                               "may be partial or empty for tasks that did not complete.")
    error: str | None = Field(default=None, description="Human-readable failure reason; null for completed tasks.")
    error_code: str | None = Field(
        default=None,
        description="Machine-readable failure reason for branching: `llm_error`, `incomplete`, "
        "`agent_error`, `worker_crashed`, `timeout`, `cancelled`, `service_restart`, `internal`. "
        "Null unless the task failed, timed out or was cancelled. New codes may be added.",
    )
    complete: bool | None = Field(default=None,
                                  description="Whether the agent reported the task as fully done; null until final.")
    deliverables: list[Deliverable] = Field(default_factory=list[Deliverable],
                                            description="Files the agent produced, in creation order.")
    exit_code: int | None = Field(default=None,
                                  description="Exit code of the worker process; null if it never exited normally.")


class SubmitResponse(Task):
    created: bool = Field(description="True when this call created the task; false when `request_id` "
                          "matched an existing task, which is returned unchanged.")


class TaskList(BaseModel):
    data: list[Task] = Field(description="Tasks, newest first.")


class Health(BaseModel):
    status: Literal["ok"] = Field(description="Always `ok` while the service is up.")


class ErrorResponse(BaseModel):
    detail: str = Field(description="Error message.", examples=["task not found"])
