import asyncio
import json
from pathlib import Path

import pytest
from conftest import RecordedModel

from vals_grading import Check, CheckJudge, GradingCase, UnsupportedCheckError
from vals_grading.judge import AUTO_EVAL_FEEDBACK_PROMPT, CHUNK_TOKENS, REQUERY_PROMPT

PASS = '{"explanation":"meets it","result":1}'
FAIL = '{"explanation":"misses it","result":0}'


def case(prompt: str = "{{criteria}}", weights: tuple[float, ...] = (1,)) -> GradingCase:
    return GradingCase(
        id="test-1",
        input_under_test="Question",
        context={"case": {"state": "CA"}},
        checks=tuple(Check(operator="criterion", prompt=prompt, criteria="Rule", severity=w) for w in weights),
    )


async def test_saved_case_and_prompt_compatibility() -> None:
    original = case(
        "Q={{input}} A={{output}} C={{criteria}} IN={{context.input}} OUT={{context.output}} STATE={{context.input.case.state}} AUDIT={{context.output.auditor.status}}"
    )
    loaded = GradingCase.model_validate_json(original.model_dump_json())
    model = RecordedModel([PASS])
    result = await CheckJudge(model).grade_test(
        loaded, "Answer", {"auditor": {"status": "done"}, "trajectory_atif": {"steps": []}}
    )
    assert model.prompts == [
        "<PROMPT>Q=Question A=Answer C=Rule IN={'case': {'state': 'CA'}} OUT={'auditor': {'status': 'done'}} STATE={CA} AUDIT={done}\n"
        "Start by explaining your reasoning, then output 1 for a CORRECT answer or 0 for an INCORRECT answer.\n"
        'Strictly follow this JSON format:\n\n```json\n{\n    "explanation": "..."\n    "result": 1 | 0\n}\n```\n</PROMPT>'
    ]
    assert result.test_id == original.id
    assert result.pass_percentage_with_weight == 1
    assert result.check_results[0].feedback == "meets it"


@pytest.mark.parametrize(
    ("replies", "score", "error"),
    [
        ([PASS], 1, False),
        ([f"Reasoning\n```json\n{FAIL}\n```"], 0, False),
        (["no json", "Answer: 1"], 1, False),
        (["no json", "0"], 0, False),
        (["no json", "no digits", "1"], 1, False),
        (["no json", "7", "maybe"], 0, False),
        (["no json", "no digits", "  "], 0, True),
        ([RuntimeError("unavailable")], 0, True),
        (["null"], 0, True),
    ],
)
async def test_parse_fallback_and_failures(replies: list[str | BaseException], score: int, error: bool) -> None:
    model = RecordedModel(replies)
    result = await CheckJudge(model).grade_test(case(), "Answer", {})
    outcome = result.check_results[0]
    assert (outcome.auto_eval, outcome.eval_error) == (score, error)
    assert len(model.prompts) == len(replies)
    if len(replies) > 1:
        assert model.prompts[1] == AUTO_EVAL_FEEDBACK_PROMPT.format(
            prompt=model.prompts[0], feedback_eval="Use the following explanation to inform your answer:\n\nno json"
        )
    if len(replies) > 2:
        assert model.prompts[2] == REQUERY_PROMPT.format(
            initial_prompt=model.prompts[1], initial_output="7" if replies[1] == "7" else ""
        )


@pytest.mark.parametrize(
    ("weights", "replies", "percentages"),
    [
        ((), [], (1, 0, 0)),
        ((2, 1), [PASS, FAIL], (0.5, 2 / 3, 0)),
        ((2, -1), [PASS, FAIL], (0.5, 0.5, 0)),
        ((1, 1), [PASS, RuntimeError("boom")], (0.5, 0.5, 1)),
    ],
)
async def test_weighted_scoring(
    weights: tuple[float, ...], replies: list[str | BaseException], percentages: tuple[float, ...]
) -> None:
    result = await CheckJudge(RecordedModel(replies)).grade_test(case(weights=weights), "Answer", {})
    assert (result.pass_percentage, result.pass_percentage_with_weight, result.low_confidence_percentage) == percentages


@pytest.mark.parametrize(
    ("prompt", "length", "refused"),
    [
        ("{{files}}", 10, True),
        ("{{context.input}}", CHUNK_TOKENS, False),
        ("{{context.input}}", CHUNK_TOKENS + 1, True),
        ("{{criteria}}", CHUNK_TOKENS + 1, False),
    ],
)
async def test_unsupported_input_fails_before_generation(prompt: str, length: int, refused: bool) -> None:
    test = case(prompt).model_copy(update={"context": {"f": "x" * (length - 9)}})
    model = RecordedModel([PASS])
    if refused:
        with pytest.raises(UnsupportedCheckError):
            await CheckJudge(model).grade_test(test, "Answer", {})
        assert model.prompts == []
    else:
        assert (await CheckJudge(model).grade_test(test, "Answer", {})).pass_percentage == 1


async def test_missing_context_is_a_check_error() -> None:
    model = RecordedModel([])
    result = await CheckJudge(model).grade_test(case("{{context.input.missing}}"), "Answer", {})
    assert model.prompts == []
    assert result.check_results[0].eval_error
    assert "Could not find requested field 'missing'" in result.check_results[0].feedback


async def test_concurrency_and_parent_cancellation() -> None:
    started = asyncio.Event()
    stopped = asyncio.Event()
    active = 0
    peak = 0

    class BlockedModel(RecordedModel):
        async def generate(self, prompt: str, system: str = "") -> str:
            nonlocal active, peak
            if not self.prompts:
                self.prompts.append(prompt)
                return PASS
            active += 1
            peak = max(peak, active)
            if active == 10:
                started.set()
            try:
                await asyncio.Event().wait()
                return PASS
            finally:
                active -= 1
                if active == 0:
                    stopped.set()

    task = asyncio.create_task(CheckJudge(BlockedModel([])).grade_test(case(weights=(1,) * 16), "Answer", {}))
    await asyncio.wait_for(started.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.wait_for(stopped.wait(), 2)
    assert active == 0 and peak == 10


async def test_checks_share_the_rendered_prefix_as_system() -> None:
    calls: list[tuple[str, str]] = []

    class SystemModel(RecordedModel):
        async def generate(self, prompt: str, system: str = "") -> str:
            calls.append((system, prompt))
            return PASS

    test = case("A={{output}}\nC={{criteria}}", weights=(1, 1))
    test = test.model_copy(update={"checks": (test.checks[0], test.checks[1].model_copy(update={"criteria": "Other"}))})
    await CheckJudge(SystemModel([])).grade_test(test, "Answer", {})
    assert [system for system, _ in calls] == ["<PROMPT>A=Answer\n"] * 2
    assert [prompt.split("\n")[0] for _, prompt in calls] == ["C=Rule", "C=Other"]


async def test_model_cancellation_does_not_become_a_score() -> None:
    with pytest.raises(asyncio.CancelledError):
        await CheckJudge(RecordedModel([asyncio.CancelledError()])).grade_test(case(), "Answer", {})


async def test_runnable_example_inputs() -> None:
    directory = Path(__file__).parents[1] / "examples"
    saved = GradingCase.model_validate_json((directory / "case.json").read_text())
    response = json.loads((directory / "response.json").read_text())
    result = await CheckJudge(RecordedModel([PASS])).grade_test(
        saved, response["llm_output"], response["output_context"]
    )
    assert result.pass_percentage_with_weight == 1


@pytest.mark.parametrize("source", ["criteria", "input", "output", "nested", "output_context"])
@pytest.mark.parametrize("placeholder", ["{{files}}", "{{context.input}}"])
async def test_inserted_unsupported_placeholders_fail_before_queries(source: str, placeholder: str) -> None:
    if source == "output_context" and placeholder == "{{context.input}}":
        return  # This replacement is earlier in the documented order and stays literal.
    test = case().model_copy(update={"context": {"large": "x" * CHUNK_TOKENS, "nested": placeholder}})
    prompt = {
        "criteria": "{{criteria}}",
        "input": "{{input}}",
        "output": "{{output}}",
        "nested": "{{context.input.nested}}",
        "output_context": "{{context.output}}",
    }[source]
    test = test.model_copy(
        update={
            "input_under_test": placeholder,
            "checks": (test.checks[0].model_copy(update={"prompt": prompt, "criteria": placeholder}),),
        }
    )
    model = RecordedModel([PASS])
    with pytest.raises(UnsupportedCheckError):
        await CheckJudge(model).grade_test(test, placeholder, {"value": placeholder})
    assert not model.prompts


@pytest.mark.parametrize("invalid", [2, -1, 0.5, "2"])
async def test_nonbinary_json_uses_fallback(invalid: object) -> None:
    model = RecordedModel([json.dumps({"result": invalid, "explanation": "invalid"}), "0"])
    result = await CheckJudge(model).grade_test(case(weights=(-1,)), "A", {})
    assert result.check_results[0].auto_eval == 0
    assert len(model.prompts) == 2


async def test_nonbinary_requery_cannot_escape_negative_penalty() -> None:
    model = RecordedModel([PASS, "no json", "2", "2"])
    result = await CheckJudge(model).grade_test(case(weights=(2, -1)), "A", {})
    assert result.pass_percentage_with_weight == 0.5
