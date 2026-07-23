from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field, TypeAdapter


class SandboxRecord(BaseModel):
    id: str
    name: str
    state: str
    labels: dict[str, str] = Field(default_factory=dict)


class SandboxListPage(BaseModel):
    items: list[SandboxRecord]
    continue_token: str | None = None


class ExecResponse(BaseModel):
    exit_code: int
    output: str = ""


class CommandRequest(BaseModel):
    command: str
    cwd: str | None = None
    timeout: float | None = None
    env_vars: dict[str, str] | None = None


class CommandOutputEvent(BaseModel):
    type: Literal["stdout", "stderr"]
    data: str


class CommandExitEvent(BaseModel):
    type: Literal["exit"]
    exit_code: int


class CommandErrorEvent(BaseModel):
    type: Literal["error"]
    code: str
    message: str
    request_id: str | None = None


CommandEvent = Annotated[
    CommandOutputEvent | CommandExitEvent | CommandErrorEvent,
    Field(discriminator="type"),
]
command_event_adapter: TypeAdapter[CommandEvent] = TypeAdapter(CommandEvent)


class EgressRequest(BaseModel):
    allowed_addresses: list[str]


class ControlErrorDetail(BaseModel):
    code: str
    message: str
    request_id: str | None = None


class ControlErrorResponse(BaseModel):
    error: ControlErrorDetail
