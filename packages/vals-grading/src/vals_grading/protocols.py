"""Model contract used by the grading engine."""

from typing import Protocol


class JudgeModel(Protocol):
    async def generate(self, prompt: str) -> str:
        """Send one user message and return the model text, or raise on failure."""
        ...

    async def count_tokens(self, text: str) -> int:
        """Count tokens using the judge model's encoding."""
        ...
