"""In-service check grading, ported from the platform's custom-operator path."""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import pytest
from model_library.base import LLMConfig
from vals import Suite
from vals.sdk.types import Check, CheckModifiers, CheckResult, ConditionalCheck, Confidence, Example, Test, TestResult

from benchmark_service.vals import check_judge
from benchmark_service.vals.check_judge import CheckJudge, CustomOperatorPrompt, UnsupportedCheckError

OPERATOR = "snap_satisfies_criteria"
PASS_REPLY = '```json\n{"explanation": "meets it", "result": 1}\n```'
FAIL_REPLY = '```json\n{"explanation": "misses it", "result": 0}\n```'


class _CharEncoding:
    """One token per character, so the chunk threshold is easy to reach."""

    def encode(self, text: str) -> list[str]:
        return list(text)


class _Reply:
    def __init__(self, text: str) -> None:
        self.output_text_str = text


class FakeLLM:
    """Answers queries from `replies` in order; a reply that is an exception is raised instead."""

    def __init__(self, replies: list[str | Exception]) -> None:
        self.replies: Iterator[str | Exception] = iter(replies)
        self.queries: list[str] = []
        self.constructed_with: tuple[str, LLMConfig | None] | None = None

    async def query(self, prompt: str) -> _Reply:
        self.queries.append(prompt)
        reply = next(self.replies)
        if isinstance(reply, Exception):
            raise reply
        return _Reply(reply)

    async def get_encoding(self) -> _CharEncoding:
        return _CharEncoding()


@pytest.fixture(autouse=True)
def gateway_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MODEL_GATEWAY_URL", "https://gateway.test")
    monkeypatch.setenv("MODEL_GATEWAY_API_KEY", "test-key")


def make_judge(
    monkeypatch: pytest.MonkeyPatch,
    replies: list[str | Exception],
    prompt: str = "Criteria: {{criteria}}",
    rubric: list[Any] | None = None,
) -> tuple[CheckJudge, FakeLLM]:
    llm = FakeLLM(replies)

    def fake_get_registry_model(model: str, override_config: LLMConfig | None = None) -> Any:
        llm.constructed_with = (model, override_config)
        return llm

    monkeypatch.setattr(check_judge, "get_registry_model", fake_get_registry_model)
    return CheckJudge({OPERATOR: CustomOperatorPrompt(prompt=prompt, rubric=rubric)}), llm


def make_test(*checks: Check, context: dict[str, Any] | None = None, right_answer: str = "") -> Test:
    return Test(
        id="test-1",
        input_under_test="Am I eligible?",
        checks=list(checks),
        context=context or {},
        right_answer=right_answer,
    )


def check(criteria: str = "Mentions income limits", **modifiers: Any) -> Check:
    return Check(operator=OPERATOR, criteria=criteria, modifiers=CheckModifiers(**modifiers))


async def grade(judge: CheckJudge, test: Test, output_context: dict[str, Any] | None = None) -> TestResult:
    await judge.require_gradeable(Suite(title="suite", tests=[test]))
    return await judge.grade_test(test, "the transcript", output_context or {})


async def test_renders_platform_placeholders(monkeypatch: pytest.MonkeyPatch) -> None:
    prompt = (
        "Q={{input}} A={{output}} C={{criteria}} IN={{context.input}} OUT={{context.output}} "
        "STATE={{context.input.case.state}} AUDIT={{context.output.auditor.status}}"
    )
    judge, llm = make_judge(monkeypatch, [PASS_REPLY], prompt=prompt)
    context = {"case": {"state": "CA"}, "household": 3}
    output_context: dict[str, Any] = {
        "auditor": {"status": "evaluated"},
        "turns": (1, 2),
        "trajectory_atif": {"steps": []},
    }

    result = await grade(judge, make_test(check(), context=context), output_context)

    rendered = (
        "Q=Am I eligible? A=the transcript C=Mentions income limits "
        "IN={'case': {'state': 'CA'}, 'household': 3} "
        "OUT={'auditor': {'status': 'evaluated'}, 'turns': [1, 2]} "
        "STATE={CA} AUDIT={evaluated}"
    )
    assert llm.queries == [f"<PROMPT>{rendered}{check_judge.DEFAULT_FORMATTING_PROMPT}</PROMPT>"]
    assert llm.constructed_with == ("anthropic/claude-opus-4-7", check_judge.JUDGE_CONFIG)
    assert result.output_context == output_context


@pytest.mark.parametrize(
    ("replies", "auto_eval", "feedback", "eval_error"),
    [
        pytest.param([PASS_REPLY], True, "meets it", False, id="json"),
        pytest.param(['{"explanation": "bare", "result": "1"}'], True, "bare", False, id="unfenced-string-result"),
        pytest.param(["no json here", "Answer: 1"], True, "no json here", False, id="fallback-pass"),
        pytest.param(["no json here", "0"], False, "no json here", False, id="fallback-fail"),
        pytest.param(["no json here", "no digits", "1"], True, "no json here", False, id="requery-digit"),
        pytest.param(["no json here", "7", "maybe"], False, "no json here", False, id="requery-final-zero"),
        pytest.param(
            ["no json here", "no digits", "  "],
            False,
            "Error evaluating  check: string index out of range",
            True,
            id="requery-empty-reply-errors",
        ),
    ],
)
async def test_parse_fallback_requery_chain(
    monkeypatch: pytest.MonkeyPatch,
    replies: list[str | Exception],
    auto_eval: bool,
    feedback: str,
    eval_error: bool,
) -> None:
    judge, llm = make_judge(monkeypatch, replies)

    result = await grade(judge, make_test(check()))

    [check_result] = result.check_results
    assert (check_result.auto_eval, check_result.feedback, check_result.eval_error) == (auto_eval, feedback, eval_error)
    assert len(llm.queries) == len(replies)
    if len(replies) >= 2:
        assert llm.queries[1] == check_judge.AUTO_EVAL_FEEDBACK_PROMPT.format(
            prompt=llm.queries[0],
            feedback_eval="Use the following explanation to inform your answer:\n\nno json here",
        )
    if len(replies) == 3:
        assert llm.queries[2] == check_judge.REQUERY_PROMPT.format(
            initial_output="" if replies[1] == "no digits" else "7", initial_prompt=llm.queries[1]
        )


async def test_missing_context_field_fails_the_check(monkeypatch: pytest.MonkeyPatch) -> None:
    judge, llm = make_judge(monkeypatch, [], prompt="{{context.input.missing}}")

    result = await grade(judge, make_test(check()))

    [check_result] = result.check_results
    assert llm.queries == []
    assert check_result.eval_error and check_result.auto_eval is False
    assert check_result.feedback == (
        "Error evaluating  check: Could not find requested field 'missing' inside context. "
        "Please check the input context and make sure that the field exists."
    )


@pytest.mark.parametrize(
    ("test", "global_checks", "rubric", "prompt", "problem"),
    [
        pytest.param(make_test(Check(operator="includes")), [], None, "", "has no stored", id="built-in-operator"),
        pytest.param(
            make_test(check(conditional=ConditionalCheck(operator=OPERATOR, criteria="x"))),
            [],
            None,
            "",
            "conditional",
            id="conditional",
        ),
        pytest.param(make_test(check(extractor="the dollar amounts")), [], None, "", "extractor", id="extractor"),
        pytest.param(
            make_test(check(examples=[Example(type="positive", text="x")])), [], None, "", "examples", id="examples"
        ),
        pytest.param(make_test(check(), right_answer="Yes"), [], None, "", "golden output", id="golden-output"),
        pytest.param(make_test(check()), [check()], None, "", "global check", id="global-check"),
        pytest.param(make_test(check()), [], [{"score": 1}], "", "rubric", id="rubric-operator"),
        pytest.param(make_test(check()), [], None, "{{files}}", "test files", id="files-prompt"),
    ],
)
async def test_refuses_unsupported_checks_before_any_model_call(
    monkeypatch: pytest.MonkeyPatch,
    test: Test,
    global_checks: list[Check],
    rubric: list[Any] | None,
    prompt: str,
    problem: str,
) -> None:
    judge, llm = make_judge(monkeypatch, [], prompt=prompt, rubric=rubric)

    with pytest.raises(UnsupportedCheckError, match=problem):
        await judge.require_gradeable(Suite(title="suite", tests=[test], global_checks=global_checks))
    with pytest.raises(UnsupportedCheckError, match="require_gradeable"):
        await judge.grade_test(test, "transcript", {})
    assert llm.queries == []


@pytest.mark.parametrize(
    ("prompt", "context_tokens", "refused"),
    [
        pytest.param("{{context.input}}", check_judge.CHUNK_TOKENS + 1, True, id="over-limit"),
        pytest.param("{{context.input}}", check_judge.CHUNK_TOKENS, False, id="at-limit-is-one-chunk"),
        pytest.param("{{criteria}}", check_judge.CHUNK_TOKENS + 1, False, id="over-limit-without-placeholder"),
    ],
)
async def test_refuses_only_contexts_the_platform_would_chunk(
    monkeypatch: pytest.MonkeyPatch, prompt: str, context_tokens: int, refused: bool
) -> None:
    judge, llm = make_judge(monkeypatch, [PASS_REPLY], prompt=prompt)
    test = make_test(check(), context={"f": "x" * (context_tokens - 9)})
    assert len(str(test.context)) == context_tokens

    if refused:
        with pytest.raises(UnsupportedCheckError, match="grades in chunks"):
            await grade(judge, test)
        assert llm.queries == []
    else:
        result = await grade(judge, test)
        assert result.check_results[0].auto_eval is True


async def test_grades_only_the_registered_test_objects(monkeypatch: pytest.MonkeyPatch) -> None:
    judge, llm = make_judge(monkeypatch, [])
    test = make_test(check())
    await judge.require_gradeable(Suite(title="suite", tests=[test]))

    with pytest.raises(UnsupportedCheckError, match="require_gradeable"):
        await judge.grade_test(test.model_copy(), "transcript", {})
    assert llm.queries == []


def test_requires_gateway(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MODEL_GATEWAY_API_KEY")
    with pytest.raises(RuntimeError, match="MODEL_GATEWAY_API_KEY"):
        CheckJudge({})


@pytest.mark.parametrize(
    ("checks", "replies", "percentages"),
    [
        pytest.param([], [], (1.0, 0.0, 0.0), id="no-checks-vacuous-pass"),
        pytest.param([check(severity=2), check(severity=1)], [PASS_REPLY, FAIL_REPLY], (0.5, 2 / 3, 0.0), id="weights"),
        pytest.param(
            [check(severity=2), check(severity=-1)], [PASS_REPLY, FAIL_REPLY], (0.5, 0.5, 0.0), id="negative-weight"
        ),
        pytest.param([check(), check()], [PASS_REPLY, RuntimeError("boom")], (0.5, 0.5, 1.0), id="errored-fails"),
    ],
)
async def test_pass_percentages(
    monkeypatch: pytest.MonkeyPatch,
    checks: list[Check],
    replies: list[str | Exception],
    percentages: tuple[float, float, float],
) -> None:
    judge, _ = make_judge(monkeypatch, replies)

    result = await grade(judge, make_test(*checks))

    assert (result.pass_percentage, result.pass_percentage_with_weight, result.low_confidence_percentage) == percentages


async def test_check_results_match_what_the_sdk_builds_from_the_platform(monkeypatch: pytest.MonkeyPatch) -> None:
    """The SDK's `Test.evaluate` builds each CheckResult from a `resultJson` element like these."""
    judge, _ = make_judge(monkeypatch, [PASS_REPLY, RuntimeError("boom")])

    result = await grade(judge, make_test(check(criteria=""), check(severity=2)))

    modifiers: dict[str, Any] = {
        "optional": False,
        "severity": None,
        "display_metrics": False,
        "examples": [],
        "extractor": None,
        "conditional": None,
        "category": None,
    }
    platform_elements: list[dict[str, Any]] = [
        {
            "operator": OPERATOR,
            "criteria": "",
            "modifiers": modifiers,
            "auto_eval": 1.0,
            "feedback": "meets it",
            "eval_cont": 0.5,
            "is_global": False,
            "eval_error": False,
        },
        {
            "operator": OPERATOR,
            "criteria": "Mentions income limits",
            "modifiers": {**modifiers, "severity": 2.0},
            "auto_eval": 0.0,
            "feedback": "Error evaluating  check: boom",
            "eval_cont": 0.0,
            "is_global": False,
            "eval_error": True,
        },
    ]
    assert result.check_results == [
        CheckResult.model_validate(element)  # pyright: ignore[reportUnknownMemberType]  # SDK types obj as bare dict
        for element in platform_elements
    ]
    assert [r.confidence for r in result.check_results] == [Confidence.CONFIDENCE_NOT_RUN, Confidence.LOW]


# Copied verbatim from vals-ai/platform at d63fdbd so an edit to the port cannot drift from it
# unnoticed. Re-copy these from the platform source when re-porting check_judge.py.

# backend/app/operators/library/custom_operator.py:16-26
PLATFORM_DEFAULT_FORMATTING_PROMPT = """
Start by explaining your reasoning, then output 1 for a CORRECT answer or 0 for an INCORRECT answer.
Strictly follow this JSON format:

```json
{
    "explanation": "..."
    "result": 1 | 0
}
```
"""

# backend/app/operators/library/prompts.py:11-14
PLATFORM_AUTO_EVAL_FEEDBACK_PROMPT_V1 = """Give your answer as only 0 or 1, with no additional text with 1 meaning affirmative and 0 meaning negative. I will give you a text:
{prompt}
{feedback_eval}
Answer: """


def platform_requery_prompt(initial_prompt: str, initial_output: str) -> str:
    # backend/app/operators/operator_base.py:119-123
    reprompt = (
        f"I will give you a prompt, for which the model produced an invalid output. Correct this mistake and return either 0 or 1 with no additional context. The invalid output was {initial_output}. The initial prompt was:"
        + "\n\n"
        + f"'{initial_prompt}'"
    )
    return reprompt


def platform_check_error_feedback(result: Exception, is_global: bool) -> str:
    # backend/app/runs/evaluation_pipeline.py:448
    return f"Error evaluating {'global' if is_global else ''} check: {result}"


# backend/app/operators/operator_base.py:71-75, with EVALUATOR_MAX_TOKENS from custom_operator.py:193
PLATFORM_EVALUATOR_CONFIG = LLMConfig(temperature=0, top_p=1.0, max_tokens=10_000)


def test_ported_constants_match_platform() -> None:
    assert check_judge.DEFAULT_FORMATTING_PROMPT == PLATFORM_DEFAULT_FORMATTING_PROMPT
    assert check_judge.AUTO_EVAL_FEEDBACK_PROMPT == PLATFORM_AUTO_EVAL_FEEDBACK_PROMPT_V1
    assert check_judge.REQUERY_PROMPT.format(initial_prompt="P {x}", initial_output="7") == platform_requery_prompt(
        "P {x}", "7"
    )
    error = ValueError("boom")
    assert check_judge.CHECK_ERROR_FEEDBACK.format(error=error) == platform_check_error_feedback(error, False)
    assert json.loads(check_judge.JUDGE_CONFIG.model_dump_json(exclude_unset=True)) == json.loads(
        PLATFORM_EVALUATOR_CONFIG.model_dump_json(exclude_unset=True)
    )
    # backend/app/runs/mutations/run_results_mutations.py:208, operator_utils.py:83, runs/model_config.py:18
    assert (check_judge.CHECK_CONCURRENCY, check_judge.CHUNK_TOKENS) == (10, 50000)
    assert check_judge.TRAJECTORY_OUTPUT_CONTEXT_KEY == "trajectory_atif"
