"""Local grading inputs and results."""

from pydantic import BaseModel, ConfigDict, JsonValue


class GradingModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class Check(GradingModel):
    """One binary criterion with its complete judge prompt."""

    operator: str
    prompt: str
    criteria: str
    severity: float


class GradingCase(GradingModel):
    """Saved inputs needed to grade one candidate response."""

    id: str
    input_under_test: str
    context: dict[str, JsonValue]
    checks: tuple[Check, ...]


class CheckResult(GradingModel):
    operator: str
    criteria: str
    auto_eval: int
    feedback: str
    eval_error: bool

    @property
    def eval_cont(self) -> float:
        """Platform confidence value when confidence evaluation is disabled."""
        return 0 if self.eval_error else 0.5


class GradingResult(GradingModel):
    test_id: str
    grading_case: GradingCase
    check_results: tuple[CheckResult, ...]
    pass_percentage: float
    pass_percentage_with_weight: float
    low_confidence_percentage: float
