from typing import Any

import pytest
from conftest import RecordedModel
from vals import Suite
from vals.sdk.types import Check as SDKCheck
from vals.sdk.types import CheckModifiers, Confidence, Test

from vals_grading import CheckJudge, GradingCase, UnsupportedCheckError
from vals_grading.adapters.vals import CustomOperatorPrompt, export_suite, to_test_result

OPERATORS = {"custom": CustomOperatorPrompt(prompt="{{output}} {{criteria}}", rubric=None)}


@pytest.mark.parametrize("rubric", [None, []])
async def test_export_grade_and_convert_without_platform(rubric: list[Any] | None) -> None:
    test = Test(id="saved-test", input_under_test="Question", checks=[SDKCheck(operator="custom", criteria="Rule")])
    suite = Suite(title="saved suite", tests=[test])
    exported = export_suite(suite, {"custom": CustomOperatorPrompt(prompt="{{output}} {{criteria}}", rubric=rubric)})
    saved = GradingCase.model_validate_json(exported["saved-test"].model_dump_json())
    result = await CheckJudge(RecordedModel(['{"result":1,"explanation":"ok"}'])).grade_test(saved, "Answer", {})
    restored = to_test_result(result, test, "Answer", {})
    assert restored.pass_percentage_with_weight == 1
    assert restored.check_results[0].confidence == Confidence.CONFIDENCE_NOT_RUN
    assert restored.check_results[0].auto_eval == 1
    assert not restored.check_results[0].eval_error
    with pytest.raises(ValueError, match="different test"):
        to_test_result(result, test.model_copy(update={"id": "another-test"}), "Answer", {})


@pytest.mark.parametrize(
    ("modifiers", "problem"),
    [
        ({"conditional": {"operator": "custom", "criteria": "x"}}, "conditional"),
        ({"extractor": "amounts"}, "extractor"),
        ({"examples": [{"type": "positive", "text": "x"}]}, "examples"),
    ],
)
def test_rejects_unsupported_modifiers(modifiers: dict[str, Any], problem: str) -> None:
    test = Test(
        id="test", input_under_test="Q", checks=[SDKCheck(operator="custom", modifiers=CheckModifiers(**modifiers))]
    )
    with pytest.raises(UnsupportedCheckError, match=problem):
        export_suite(Suite(title="suite", tests=[test]), OPERATORS)


@pytest.mark.parametrize("kind", ["global", "golden", "built-in", "rubric", "duplicate"])
def test_rejects_unsupported_suite(kind: str) -> None:
    check = SDKCheck(operator="includes" if kind == "built-in" else "custom")
    test = Test(id="test", input_under_test="Q", checks=[check], right_answer="A" if kind == "golden" else "")
    suite = Suite(
        title="suite",
        tests=[test, test] if kind == "duplicate" else [test],
        global_checks=[check] if kind == "global" else [],
    )
    operators = {"custom": CustomOperatorPrompt(prompt="P", rubric=[{"score": 1}] if kind == "rubric" else None)}
    with pytest.raises(UnsupportedCheckError):
        export_suite(suite, operators)
