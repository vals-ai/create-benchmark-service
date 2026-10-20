"""Grade Vals suite checks in the service, as a port of the platform's custom-operator grading.

Ported from vals-ai/platform at d63fdbd, `backend/app`: the `evaluateTestChecks` mutation, the
evaluation pipeline, `CustomOperator` and `BaseOperator`. Each function names the code it ports.
The platform's confidence call is not ported: checks report CONFIDENCE_NOT_RUN, and a failure of that
call cannot fail a check here.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from model_library.base import LLM, LLMConfig
from model_library.registry_utils import get_registry_model
from pydantic import BaseModel
from pydantic_core import to_jsonable_python
from vals import Metadata, Suite
from vals.graphql_client.input_types import TestEvalInputType
from vals.sdk.types import Check, CheckResult, Confidence, Test, TestResult

logger = logging.getLogger(__name__)

GATEWAY_ENV_VARS = ("MODEL_GATEWAY_URL", "MODEL_GATEWAY_API_KEY")

JUDGE_MODEL = "anthropic/claude-opus-4-7"
# operator_base.py BaseOperator.evaluator_config with CustomOperator.EVALUATOR_MAX_TOKENS.
JUDGE_CONFIG = LLMConfig(temperature=0, top_p=1.0, max_tokens=10_000)

# run_results_mutations.py EvaluateTestChecksMutation: concurrency_limit per test.
CHECK_CONCURRENCY = 10
# operator_utils.py split_string_by_tokens: above this, the platform grades the test context in chunks.
CHUNK_TOKENS = 50_000
# runs/model_config.py: the platform drops this key before rendering `{{context.output}}`.
TRAJECTORY_OUTPUT_CONTEXT_KEY = "trajectory_atif"

# custom_operator.py
DEFAULT_FORMATTING_PROMPT = """
Start by explaining your reasoning, then output 1 for a CORRECT answer or 0 for an INCORRECT answer.
Strictly follow this JSON format:

```json
{
    "explanation": "..."
    "result": 1 | 0
}
```
"""

# library/prompts.py AUTO_EVAL_FEEDBACK_PROMPT_V1
AUTO_EVAL_FEEDBACK_PROMPT = """Give your answer as only 0 or 1, with no additional text with 1 meaning affirmative and 0 meaning negative. I will give you a text:
{prompt}
{feedback_eval}
Answer: """

# operator_base.py BaseOperator.requery
REQUERY_PROMPT = (
    "I will give you a prompt, for which the model produced an invalid output. Correct this mistake and return "
    "either 0 or 1 with no additional context. The invalid output was {initial_output}. The initial prompt was:"
    "\n\n'{initial_prompt}'"
)

# evaluation_pipeline.py evaluate_checks, for a non-global check.
CHECK_ERROR_FEEDBACK = "Error evaluating  check: {error}"


class UnsupportedCheckError(ValueError):
    """A suite or test uses a platform grading feature this port does not implement."""


class CustomOperatorPrompt(BaseModel):
    """A custom operator as exported from the platform. Only operators without a rubric can be graded."""

    prompt: str
    rubric: list[Any] | None


@dataclass(frozen=True)
class _CheckOutcome:
    auto_eval: int
    feedback: str
    eval_error: bool

    @property
    def eval_cont(self) -> float:
        """0 for an errored check, as on the platform; otherwise 0.5, the platform's value when confidence is off."""
        return 0 if self.eval_error else Confidence.CONFIDENCE_NOT_RUN.value


class CheckJudge:
    """Grades suite tests the way the platform's `evaluateTestChecks` grades custom-operator checks."""

    def __init__(self, operators: Mapping[str, CustomOperatorPrompt]) -> None:
        """`operators` maps each custom operator name to its platform export."""
        missing = [name for name in GATEWAY_ENV_VARS if not os.environ.get(name)]
        if missing:
            raise RuntimeError(
                f"Check grading cannot reach its judge model: set {' and '.join(missing)} before starting the service."
            )
        if "eval_error" not in CheckResult.model_fields:
            raise RuntimeError(
                "Check grading needs a Vals SDK (valsai) whose CheckResult has eval_error: upgrade valsai."
            )
        self._operators = dict(operators)
        self._gradeable_tests: dict[int, Test] = {}
        self._model: LLM = get_registry_model(JUDGE_MODEL, override_config=JUDGE_CONFIG)

    async def require_gradeable(self, suite: Suite) -> None:
        """Raise `UnsupportedCheckError` naming every check in `suite` this judge cannot grade exactly.

        `grade_test` only grades the `Test` objects of a suite that passed this check.
        """
        encoding = None
        problem_test_ids: dict[str, list[str]] = {
            f"global check {check.operator!r}": [] for check in suite.global_checks
        }
        for test in suite.tests:
            problems = self._test_problems(test)
            if any(
                "{{context.input}}" in operator.prompt
                for check in test.checks
                if (operator := self._operators.get(check.operator))
            ):
                encoding = encoding or await self._model.get_encoding()
                if len(encoding.encode(str(test.context))) > CHUNK_TOKENS:
                    problems.append(f"context over {CHUNK_TOKENS} tokens, which the platform grades in chunks")
            for problem in problems:
                problem_test_ids.setdefault(problem, []).append(str(test.id))
        if problem_test_ids:
            summary = "; ".join(
                f"{problem} (tests affected: {len(ids)}, e.g. {', '.join(ids[:3])})" if ids else problem
                for problem, ids in problem_test_ids.items()
            )
            raise UnsupportedCheckError(f"Suite {suite.id} cannot be graded in the service: {summary}")
        self._gradeable_tests.update((id(test), test) for test in suite.tests)

    async def grade_test(
        self,
        test: Test,
        llm_output: str,
        output_context: dict[str, Any],
        metadata: Metadata | None = None,
    ) -> TestResult:
        """Grade `test`'s checks against `llm_output`, returning what `Test.evaluate` returns.

        A judge call that raises fails its check with `eval_error`, as on the platform.
        """
        if self._gradeable_tests.get(id(test)) is not test:
            raise UnsupportedCheckError(f"Test {test.id} is not from a suite that passed require_gradeable.")

        evaluation_context = _output_context_for_evaluation(output_context)
        semaphore = asyncio.Semaphore(CHECK_CONCURRENCY)

        async def grade(check: Check) -> _CheckOutcome:
            async with semaphore:
                return await self._grade_check(test, check, llm_output, evaluation_context)

        results = await asyncio.gather(*(grade(check) for check in test.checks), return_exceptions=True)
        outcomes: list[_CheckOutcome] = []
        for check, result in zip(test.checks, results, strict=True):
            if isinstance(result, _CheckOutcome):
                outcomes.append(result)
            elif isinstance(result, (Exception, asyncio.CancelledError)):
                logger.warning("Judge failed on check %r of test %s: %s", check.operator, test.id, result)
                outcomes.append(_CheckOutcome(0, CHECK_ERROR_FEEDBACK.format(error=result), eval_error=True))
            else:
                raise result

        pass_percentage, pass_percentage_with_weight, low_confidence_percentage = _compute_pass_percentages(
            test.checks, outcomes
        )
        return TestResult(
            test=test,
            input_under_test=test.input_under_test,
            context=test.context,
            output_context=output_context,
            llm_output=llm_output,
            check_results=[
                CheckResult(
                    operator=check.operator,
                    criteria=check.criteria,
                    modifiers=check.modifiers,
                    is_global=False,
                    auto_eval=outcome.auto_eval,
                    feedback=outcome.feedback,
                    eval_error=outcome.eval_error,
                    confidence=Confidence(outcome.eval_cont),
                )
                for check, outcome in zip(test.checks, outcomes, strict=True)
            ],
            pass_percentage=pass_percentage,
            pass_percentage_with_weight=pass_percentage_with_weight,
            low_confidence_percentage=low_confidence_percentage,
            metadata=metadata,
        )

    def _test_problems(self, test: Test) -> list[str]:
        problems: list[str] = []
        if test.right_answer:
            problems.append("golden output")
        for check in test.checks:
            operator = self._operators.get(check.operator)
            if operator is None:
                problems.append(f"operator {check.operator!r} has no stored custom-operator prompt")
            else:
                if operator.rubric is not None:
                    problems.append(f"{check.operator!r} is a rubric operator")
                if "{{files}}" in operator.prompt:
                    problems.append(f"{check.operator!r} grades against test files")
            modifiers = check.modifiers
            if modifiers.conditional is not None:
                problems.append(f"{check.operator!r} conditional modifier")
            if modifiers.extractor is not None and modifiers.extractor.strip() != "":
                problems.append(f"{check.operator!r} extractor modifier")
            if modifiers.examples:
                problems.append(f"{check.operator!r} examples modifier")
        return problems

    async def _grade_check(
        self, test: Test, check: Check, llm_output: str, output_context: dict[str, Any]
    ) -> _CheckOutcome:
        """CustomOperator.run_eval -> BaseOperator.process_prompt -> parse_results."""
        prompt = _format_prompt(
            self._operators[check.operator].prompt,
            test=test,
            criteria=check.criteria,
            llm_output=llm_output,
            output_context=output_context,
        )
        query_to_llm = f"<PROMPT>{prompt}</PROMPT>"
        output = (await self._model.query(query_to_llm)).output_text_str

        start_token = "```json"
        start = output.find(start_token)
        end = output.rfind("```")
        if start != -1 and end != -1 and end > start:
            output = output[start + len(start_token) : end]

        result_json = _parse_json(output)
        if result_json == 0:
            logger.warning(
                "Judge output for check %r of test %s was not valid JSON; asking for 0 or 1", check.operator, test.id
            )
            return _CheckOutcome(await self._auto_eval_fallback(query_to_llm, output), output, eval_error=False)
        return _CheckOutcome(int(result_json["result"]), str(result_json["explanation"]), eval_error=False)

    async def _auto_eval_fallback(self, prompt: str, feedback_eval: str) -> int:
        """BaseOperator.auto_eval_fallback: re-ask for a bare 0 or 1, seeded with the unparsable output."""
        feedback_eval_str = (
            f"Use the following explanation to inform your answer:\n\n{feedback_eval}" if feedback_eval else ""
        )
        query_to_llm = AUTO_EVAL_FEEDBACK_PROMPT.format(prompt=prompt, feedback_eval=feedback_eval_str)
        output = await self._model.query(query_to_llm)
        raw_output = re.sub(r"[^0-9]", "", output.output_text_str)
        if raw_output == "":
            return await self._requery(query_to_llm, raw_output)
        if raw_output[0] in ("0", "1"):
            return int(raw_output[0])
        return await self._requery(query_to_llm, raw_output)

    async def _requery(self, initial_prompt: str, initial_output: str) -> int:
        """BaseOperator.requery: the last attempt, which scores 0 unless the reply starts with a digit.

        An empty reply raises `IndexError`, which fails the check with `eval_error` as on the platform.
        """
        reprompt = REQUERY_PROMPT.format(initial_output=initial_output, initial_prompt=initial_prompt)
        output = await self._model.query(reprompt)
        raw_output = output.output_text_str.strip()[0]
        if raw_output.isdigit():
            return int(raw_output)
        logger.warning("Judge gave no 0 or 1 after a requery; scoring the check 0")
        return 0


def _format_prompt(template: str, *, test: Test, criteria: str, llm_output: str, output_context: dict[str, Any]) -> str:
    """custom_operator.format_prompts for a prompt without `{{files}}` whose test context fits one chunk.

    Replacements run in the platform's order, so a placeholder inside a substituted value is also
    replaced. `{{context.input.X}}` keeps its outer braces (`{value}`), as the platform renders it.
    """
    prompt = template
    input_instances, output_instances = _find_context_placeholders(prompt)
    for instance in input_instances:
        prompt = prompt.replace(f"{{context.input.{instance}}}", _get_nested_value(test.context, instance))
    for instance in output_instances:
        prompt = prompt.replace(f"{{context.output.{instance}}}", _get_nested_value(output_context, instance))

    matching_placeholders = {
        "{{input}}": test.input_under_test,
        "{{output}}": llm_output,
        "{{criteria}}": criteria,
        "{{context.input}}": str(test.context),
        "{{context.output}}": str(output_context),
        "{{files}}": "",
    }
    prompt += DEFAULT_FORMATTING_PROMPT
    for placeholder, value in matching_placeholders.items():
        prompt = prompt.replace(placeholder, value)
    return prompt


def _find_context_placeholders(prompt: str) -> tuple[list[str], list[str]]:
    """custom_operator.find_context_placeholders."""
    return re.findall(r"{{context\.input\.(.*?)}}", prompt), re.findall(r"{{context\.output\.(.*?)}}", prompt)


def _get_nested_value(data: dict[str, Any], path: str) -> str:
    """custom_operator.get_nested_value: `str()` of the value at dotted `path`."""
    current: Any = data
    try:
        for part in path.split("."):
            current = current[part]
        return str(current)
    except (KeyError, TypeError):
        raise ValueError(
            f"Could not find requested field '{path}' inside context. "
            "Please check the input context and make sure that the field exists."
        ) from None


def _parse_json(llm_output: str) -> Any:
    """operator_utils.parse_json: the parsed reply, or 0 when it lacks `explanation` and `result`."""
    if llm_output.startswith("```json") and llm_output.endswith("```"):
        llm_output = llm_output[7:-3]
    try:
        parsed_output = json.loads(llm_output)
        if "explanation" in parsed_output and "result" in parsed_output:
            return parsed_output
    except json.JSONDecodeError:
        pass
    return 0


def _compute_pass_percentages(checks: list[Check], outcomes: list[_CheckOutcome]) -> tuple[float, float, float]:
    """human_review/shared.py compute_pass_percentages, for binary checks only."""
    checks_passing = 0
    checks_passing_total = 0
    checks_passing_with_weight = 0.0
    total_weight = 0.0
    low_confidence_checks = 0
    low_confidence_total = 0

    for check, outcome in zip(checks, outcomes, strict=True):
        severity = check.modifiers.severity if check.modifiers.severity is not None else 1
        checks_passing_total += 1
        if outcome.auto_eval == 1:
            checks_passing += 1

        if severity > 0:
            total_weight += severity
            if outcome.auto_eval == 1:
                checks_passing_with_weight += severity
        elif severity < 0:
            if outcome.auto_eval == 0:
                checks_passing_with_weight += severity

        if outcome.eval_cont in (0, 1):
            if outcome.eval_cont == 0:
                low_confidence_checks += 1
            low_confidence_total += 1

    pass_percentage = 1 if checks_passing_total == 0 else checks_passing / checks_passing_total
    pass_percentage_with_weight = checks_passing_with_weight / total_weight if total_weight > 0 else 0
    low_confidence_percentage = low_confidence_checks / low_confidence_total if low_confidence_total > 0 else 0
    return pass_percentage, pass_percentage_with_weight, low_confidence_percentage


def _output_context_for_evaluation(output_context: dict[str, Any]) -> dict[str, Any]:
    """The output context as the platform receives it from `Test.evaluate`, minus the trajectory."""
    sent = TestEvalInputType(testId="", llmOutput="", outputContext=output_context).model_dump(
        by_alias=True, exclude_unset=True
    )["outputContext"]
    received: dict[str, Any] = json.loads(json.dumps(sent, default=to_jsonable_python))
    return {key: value for key, value in received.items() if key != TRAJECTORY_OUTPUT_CONTEXT_KEY}
