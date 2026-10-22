"""Exercise installed core grading with no optional dependencies."""

import asyncio
import importlib.util
from pathlib import Path

from vals_grading import CheckJudge, GradingCase


class RecordedModel:
    async def generate(self, prompt: str) -> str:
        return '{"result":1,"explanation":"recorded response"}'

    async def count_tokens(self, text: str) -> int:
        return len(text)


async def main() -> None:
    for module in ("vals", "benchmark_service", "model_library"):
        assert importlib.util.find_spec(module) is None, f"Unexpected dependency: {module}"
    path = Path(__file__).parents[1] / "examples" / "case.json"
    case = GradingCase.model_validate_json(path.read_text())
    result = await CheckJudge(RecordedModel()).grade_test(case, "saved response", {})
    assert result.pass_percentage_with_weight == 1
    print("Standalone grading passed without valsai, CBS, or model-library installed")


if __name__ == "__main__":
    asyncio.run(main())
