"""Binary custom-check grading with platform-compatible prompt and score behavior."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any

from pydantic import JsonValue

from vals_grading.models import Check, CheckResult, GradingCase, GradingResult
from vals_grading.protocols import JudgeModel

logger = logging.getLogger(__name__)
CHECK_CONCURRENCY = 10
CHUNK_TOKENS = 50_000
TRAJECTORY_OUTPUT_CONTEXT_KEY = "trajectory_atif"

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

AUTO_EVAL_FEEDBACK_PROMPT = """Give your answer as only 0 or 1, with no additional text with 1 meaning affirmative and 0 meaning negative. I will give you a text:
{prompt}
{feedback_eval}
Answer: """

REQUERY_PROMPT = (
    "I will give you a prompt, for which the model produced an invalid output. Correct this mistake and return "
    "either 0 or 1 with no additional context. The invalid output was {initial_output}. The initial prompt was:"
    "\n\n'{initial_prompt}'"
)

CHECK_ERROR_FEEDBACK = "Error evaluating  check: {error}"


class UnsupportedCheckError(ValueError):
    """The grading input requires an unsupported feature."""


class CheckJudge:
    """Grade local custom checks with the supplied model."""

    def __init__(self, model: JudgeModel) -> None:
        self._model = model

    async def require_gradeable(
        self, test: GradingCase, llm_output: str = "", output_context: dict[str, JsonValue] | None = None
    ) -> None:
        """Refuse unsupported expansions before any grading query."""
        expanded: set[str] = set()
        for check in test.checks:
            try:
                _format_prompt(
                    check.prompt,
                    test=test,
                    criteria=check.criteria,
                    llm_output=llm_output,
                    output_context=output_context or {},
                    expanded=expanded,
                )
            except UnsupportedCheckError:
                raise
            except ValueError:
                # Missing context remains a per-check error during grading.
                continue
        if "{{context.input}}" in expanded:
            if await self._model.count_tokens(str(test.context)) > CHUNK_TOKENS:
                raise UnsupportedCheckError(
                    f"Test {test.id} has context over {CHUNK_TOKENS} tokens, which requires chunked grading."
                )

    async def grade_test(
        self, test: GradingCase, llm_output: str, output_context: dict[str, JsonValue]
    ) -> GradingResult:
        """Grade a candidate response and return local scores without uploading results."""
        # Caller-owned dictionaries can change while model requests are in flight.
        test = test.model_copy(deep=True)
        evaluation_context = json.loads(json.dumps(output_context))
        evaluation_context.pop(TRAJECTORY_OUTPUT_CONTEXT_KEY, None)
        await self.require_gradeable(test, llm_output, evaluation_context)
        semaphore = asyncio.Semaphore(CHECK_CONCURRENCY)

        async def grade(check: Check) -> CheckResult:
            async with semaphore:
                return await self._grade_check(test, check, llm_output, evaluation_context)

        results = await asyncio.gather(*(grade(check) for check in test.checks), return_exceptions=True)
        outcomes: list[CheckResult] = []
        for check, result in zip(test.checks, results, strict=True):
            if isinstance(result, CheckResult):
                outcomes.append(result)
            elif isinstance(result, asyncio.CancelledError):
                raise result
            elif isinstance(result, Exception):
                logger.warning("Judge failed on check %r of test %s: %s", check.operator, test.id, result)
                outcomes.append(
                    CheckResult(
                        operator=check.operator,
                        criteria=check.criteria,
                        auto_eval=0,
                        feedback=CHECK_ERROR_FEEDBACK.format(error=result),
                        eval_error=True,
                    )
                )
            else:
                raise result

        pass_percentage, weighted, low_confidence = _compute_pass_percentages(test.checks, outcomes)
        return GradingResult(
            test_id=test.id,
            grading_case=test,
            check_results=tuple(outcomes),
            pass_percentage=pass_percentage,
            pass_percentage_with_weight=weighted,
            low_confidence_percentage=low_confidence,
        )

    async def _grade_check(
        self, test: GradingCase, check: Check, llm_output: str, output_context: dict[str, JsonValue]
    ) -> CheckResult:
        """Render one check and parse its judge response."""
        prompt = _format_prompt(
            check.prompt,
            test=test,
            criteria=check.criteria,
            llm_output=llm_output,
            output_context=output_context,
        )
        query_to_llm = f"<PROMPT>{prompt}</PROMPT>"
        output = await self._model.generate(query_to_llm)

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
            return CheckResult(
                operator=check.operator,
                criteria=check.criteria,
                auto_eval=await self._auto_eval_fallback(query_to_llm, output),
                feedback=output,
                eval_error=False,
            )
        return CheckResult(
            operator=check.operator,
            criteria=check.criteria,
            auto_eval=int(result_json["result"]),
            feedback=str(result_json["explanation"]),
            eval_error=False,
        )

    async def _auto_eval_fallback(self, prompt: str, feedback_eval: str) -> int:
        """Ask for a bare score using the unparsed response as feedback."""
        feedback_eval_str = (
            f"Use the following explanation to inform your answer:\n\n{feedback_eval}" if feedback_eval else ""
        )
        query_to_llm = AUTO_EVAL_FEEDBACK_PROMPT.format(prompt=prompt, feedback_eval=feedback_eval_str)
        output = await self._model.generate(query_to_llm)
        raw_output = re.sub(r"[^0-9]", "", output)
        if raw_output == "":
            return await self._requery(query_to_llm, raw_output)
        if raw_output[0] in ("0", "1"):
            return int(raw_output[0])
        return await self._requery(query_to_llm, raw_output)

    async def _requery(self, initial_prompt: str, initial_output: str) -> int:
        """Make the last score request.

        An empty reply raises `IndexError`, which fails the check with `eval_error` as on the platform.
        """
        reprompt = REQUERY_PROMPT.format(initial_output=initial_output, initial_prompt=initial_prompt)
        output = await self._model.generate(reprompt)
        raw_output = output.strip()[0]
        if raw_output in ("0", "1"):
            return int(raw_output)
        logger.warning("Judge gave no 0 or 1 after a requery; scoring the check 0")
        return 0


def _format_prompt(
    template: str,
    *,
    test: GradingCase,
    criteria: str,
    llm_output: str,
    output_context: dict[str, JsonValue],
    expanded: set[str] | None = None,
) -> str:
    """Render a binary check prompt.

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
        if placeholder in prompt:
            if placeholder == "{{files}}":
                raise UnsupportedCheckError(f"Test {test.id} requires file grading, which is not supported.")
            if expanded is not None:
                expanded.add(placeholder)
        prompt = prompt.replace(placeholder, value)
    return prompt


def _find_context_placeholders(prompt: str) -> tuple[list[str], list[str]]:
    """Find dotted input and output context paths."""
    return re.findall(r"{{context\.input\.(.*?)}}", prompt), re.findall(r"{{context\.output\.(.*?)}}", prompt)


def _get_nested_value(data: dict[str, JsonValue], path: str) -> str:
    """Return the string value at a dotted context path."""
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
    """Parse a judge reply, returning 0 when the required fields are absent."""
    if llm_output.startswith("```json") and llm_output.endswith("```"):
        llm_output = llm_output[7:-3]
    try:
        parsed_output = json.loads(llm_output)
        if "explanation" in parsed_output and "result" in parsed_output:
            if parsed_output["result"] in (0, 1, "0", "1"):
                return parsed_output
    except json.JSONDecodeError:
        pass
    return 0


def _compute_pass_percentages(checks: tuple[Check, ...], outcomes: list[CheckResult]) -> tuple[float, float, float]:
    """Compute binary pass percentages with severity weights."""
    checks_passing = 0
    checks_passing_total = 0
    checks_passing_with_weight = 0.0
    total_weight = 0.0
    low_confidence_checks = 0
    low_confidence_total = 0

    for check, outcome in zip(checks, outcomes, strict=True):
        severity = check.severity
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
