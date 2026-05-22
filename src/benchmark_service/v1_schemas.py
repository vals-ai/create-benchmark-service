"""Lab-facing /v1/ request and response schemas.

The internal `EvaluateResponseRequest` / `FinalScoreResponse` keep their
existing flat shape. The v1 surface wraps those with `run_id`, a payload
envelope (so the same endpoint serves text and artifact shapes), a typed
status enum mirroring the runner's `EvalStatus`, and explicit version
fields. Both surfaces share scoring handlers in `app.py`.
"""

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class V1PayloadType(StrEnum):
    TEXT = "text"
    ARTIFACT = "artifact"


class V1EvalStatus(StrEnum):
    EVALUATED = "evaluated"
    DID_NOT_COMPLETE = "did_not_complete"
    GENERATION_ERROR = "generation_error"
    ERROR = "error"


class V1Payload(BaseModel):
    type: V1PayloadType
    schema_id: str = Field(alias="schema", description="Payload schema id, e.g. fabv2.text.v1")
    data: str = Field(description="Text answer or base64-encoded artifact bytes")

    model_config = ConfigDict(populate_by_name=True)


class V1Versions(BaseModel):
    runner: str | None = None


class V1EvalRequest(BaseModel):
    run_id: str
    task_id: str
    dataset: str | None = None
    payload: V1Payload
    versions: V1Versions = Field(default_factory=V1Versions)


class V1EvalResponse(BaseModel):
    run_id: str
    task_id: str
    status: V1EvalStatus
    evaluator_version: str | None = None
    result: Any | None = None
    errors: list[str] = Field(default_factory=list)


class V1ScoreItem(BaseModel):
    status: V1EvalStatus
    result: Any | None = None
    errors: list[str] = Field(default_factory=list)


class V1ScoreRequest(BaseModel):
    run_id: str
    dataset: str | None = None
    evaluation_results: dict[str, V1ScoreItem | None]


class V1ScoreResponse(BaseModel):
    run_id: str
    tasks_evaluated: list[str]
    final_score: float
    metadata: dict[str, Any] = Field(default_factory=dict)


__all__ = [
    "V1EvalRequest",
    "V1EvalResponse",
    "V1EvalStatus",
    "V1Payload",
    "V1PayloadType",
    "V1ScoreItem",
    "V1ScoreRequest",
    "V1ScoreResponse",
    "V1Versions",
]
