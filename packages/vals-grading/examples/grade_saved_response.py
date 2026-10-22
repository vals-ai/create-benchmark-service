"""Grade the synthetic local example through the configured Gateway."""

import asyncio
import json
from pathlib import Path

from vals_grading import CheckJudge, GradingCase
from vals_grading.gateway import GatewayJudgeModel


async def main() -> None:
    directory = Path(__file__).parent
    case = GradingCase.model_validate_json((directory / "case.json").read_text())
    response = json.loads((directory / "response.json").read_text())
    judge = CheckJudge(GatewayJudgeModel("anthropic/claude-opus-4-7"))
    result = await judge.grade_test(case, response["llm_output"], response["output_context"])
    print(result.model_dump_json(indent=2))


if __name__ == "__main__":
    asyncio.run(main())
