"""Example benchmark service implementation."""

from collections.abc import AsyncGenerator
from typing import Any

from daytona import AsyncSandbox

from benchmark_service import BenchmarkService
from benchmark_service.schemas import (
    EvaluateResponseRequest,
    FinalScoreResult,
    Resources,
    RetrieveTaskResponse,
    StreamChunk,
    StreamErrorChunk,
    StreamMessageChunk,
    StreamResultChunk,
)


class ExampleBenchmark(BenchmarkService):
    """TODO: Replace this example with your benchmark implementation.

    This example shows a simple text-based Q&A benchmark.
    Modify it to load your own dataset and implement your evaluation logic.
    """

    async def load_datasets(self) -> dict[str, dict[str, Any]]:
        """Load the benchmark datasets."""
        return {
            "default": {
                "example-task-1": {
                    "problem": "Write a function that returns 'Hello, World!'",
                    "answer": "Hello, World!",
                },
                "example-task-2": {
                    "problem": "What is 2 + 2?",
                    "answer": "4",
                },
            },
        }

    async def retrieve_task(self, task_id: str, skip_validation: bool = False, dataset: str | None = None) -> RetrieveTaskResponse:
        """Retrieve task metadata."""
        if not skip_validation:
            await self.validate_task_ids([task_id], dataset=dataset)
        return RetrieveTaskResponse(
            docker_image="python:3.12-slim",
            problem_path="/tmp/problem_statement.txt",
            request_setup=True,
            cwd="/workspace",
            resources=Resources(vcpu=2, memory=4, disk=10),
        )

    async def setup_task(self, task_id: str, sandbox: AsyncSandbox, dataset: str | None = None) -> AsyncGenerator[StreamChunk, None]:
        """Setup task in sandbox — writes the problem statement to problem_path."""
        yield StreamMessageChunk(type="message", data=f"Setting up task {task_id}...")

        task = self.get_dataset(dataset)[task_id]
        problem_statement = task["problem"]
        await sandbox.fs.upload_file(problem_statement.encode(), "/tmp/problem_statement.txt")

        yield StreamMessageChunk(type="message", data="Problem statement written to sandbox")
        yield StreamResultChunk(type="result", data={"status": "ok"})

    async def evaluate_response(self, request: EvaluateResponseRequest, dataset: str | None = None) -> Any:
        """Evaluate a text response."""
        task = self.get_dataset(dataset)[request.task_id]

        # Simple string comparison
        is_correct = request.response.strip() == task["answer"]

        # Return evaluation result as a dict (you can use any structure)
        return {
            "task_id": request.task_id,
            "resolved": is_correct,
            "score": 1.0 if is_correct else 0.0,
            "expected": task["answer"],
            "received": request.response.strip(),
        }

    async def evaluate_instance(self, task_id: str, sandbox: AsyncSandbox, dataset: str | None = None) -> AsyncGenerator[StreamChunk, None]:
        """Evaluate in sandbox (not implemented for this example)."""
        yield StreamMessageChunk(type="message", data=f"Evaluating task {task_id}...")
        yield StreamErrorChunk(
            type="error",
            data="Sandbox evaluation not implemented. Use /evaluate-response/ endpoint instead.",
        )

    async def calculate_final_score(self, evaluation_results: dict[str, Any], dataset: str | None = None) -> FinalScoreResult:
        """Calculate final score across all evaluations."""
        total = len(evaluation_results)

        # Count resolved tasks based on result structure
        resolved = sum(1 for r in evaluation_results.values() if r and r.get("resolved", False))

        score = (resolved / total * 100) if total > 0 else 0.0

        metadata = {
            "total_tasks": total,
            "resolved_tasks": resolved,
            "unresolved_tasks": total - resolved,
        }

        return FinalScoreResult(score=score, metadata=metadata)
