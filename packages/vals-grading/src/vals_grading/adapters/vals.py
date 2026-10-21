"""Convert loaded Vals SDK data without calling the platform."""

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict
from vals import Metadata, Suite
from vals.sdk.types import CheckResult as SDKCheckResult
from vals.sdk.types import Confidence, Test, TestResult

from vals_grading import Check, GradingCase, GradingResult, UnsupportedCheckError


class CustomOperatorPrompt(BaseModel):
    model_config = ConfigDict(extra="forbid")
    prompt: str
    rubric: list[Any] | None


def export_suite(suite: Suite, operators: Mapping[str, CustomOperatorPrompt]) -> dict[str, GradingCase]:
    """Convert a supported SDK suite into complete, serializable grading cases."""
    problems = [f"global check {check.operator!r}" for check in suite.global_checks]
    cases: dict[str, GradingCase] = {}
    for test in suite.tests:
        if not test.id:
            problems.append("test without an ID")
            continue
        if test.id in cases:
            problems.append(f"duplicate test ID {test.id!r}")
        if test.right_answer:
            problems.append(f"test {test.id}: golden output")
        checks: list[Check] = []
        for check in test.checks:
            operator = operators.get(check.operator)
            if operator is None:
                problems.append(f"test {test.id}: operator {check.operator!r} has no exported prompt")
                continue
            if operator.rubric:
                problems.append(f"test {test.id}: rubric operator {check.operator!r}")
            modifiers = check.modifiers
            if modifiers.conditional is not None:
                problems.append(f"test {test.id}: conditional modifier")
            if modifiers.extractor and modifiers.extractor.strip():
                problems.append(f"test {test.id}: extractor modifier")
            if modifiers.examples:
                problems.append(f"test {test.id}: examples modifier")
            checks.append(
                Check(
                    operator=check.operator,
                    prompt=operator.prompt,
                    criteria=check.criteria,
                    severity=modifiers.severity if modifiers.severity is not None else 1,
                )
            )
        cases[test.id] = GradingCase(
            id=test.id, input_under_test=test.input_under_test, context=test.context, checks=tuple(checks)
        )
    if problems:
        raise UnsupportedCheckError("Cannot export suite for local grading: " + "; ".join(problems))
    return cases


def to_test_result(
    result: GradingResult,
    test: Test,
    llm_output: str,
    output_context: dict[str, Any],
    metadata: Metadata | None = None,
) -> TestResult:
    """Build an SDK result for existing scoring or upload code."""
    expected = [(check.operator, check.criteria) for check in test.checks]
    actual = [(check.operator, check.criteria) for check in result.check_results]
    if result.test_id != test.id or expected != actual:
        raise ValueError(
            "The grading result belongs to a different test or check list. Use the original exported test."
        )
    if "eval_error" not in SDKCheckResult.model_fields:
        raise RuntimeError("Upgrade valsai to a version that preserves grading errors in CheckResult.eval_error.")
    return TestResult(
        test=test,
        input_under_test=test.input_under_test,
        context=test.context,
        output_context=output_context,
        llm_output=llm_output,
        check_results=[
            SDKCheckResult(
                operator=check.operator,
                criteria=check.criteria,
                modifiers=check.modifiers,
                is_global=False,
                auto_eval=outcome.auto_eval,
                feedback=outcome.feedback,
                eval_error=outcome.eval_error,
                confidence=Confidence(outcome.eval_cont),
            )
            for check, outcome in zip(test.checks, result.check_results, strict=True)
        ],
        pass_percentage=result.pass_percentage,
        pass_percentage_with_weight=result.pass_percentage_with_weight,
        low_confidence_percentage=result.low_confidence_percentage,
        metadata=metadata,
    )
