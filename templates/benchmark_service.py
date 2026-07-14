"""Example benchmark service implementation."""

from collections.abc import AsyncGenerator
from typing import Any

from benchmark_service import BenchmarkService, ImageSource, Resources, Sandbox
from benchmark_service.schemas import (
    EvaluateResponseRequest,
    FinalScoreResult,
    RetrieveTaskResponse,
    StreamChunk,
    StreamErrorChunk,
    StreamMessageChunk,
    StreamResultChunk,
)
from benchmark_service.v1_schemas import V1Task


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

    async def list_tasks(self, dataset: str | None = None) -> list[V1Task]:
        """Expose public task inputs for benchmark runners."""
        return [V1Task(id=task_id, question=task["problem"]) for task_id, task in self.get_dataset(dataset).items()]

    async def retrieve_task(
        self, task_id: str, skip_validation: bool = False, dataset: str | None = None
    ) -> RetrieveTaskResponse:
        """Retrieve task metadata."""
        if not skip_validation:
            await self.validate_task_ids([task_id], dataset=dataset)
        return RetrieveTaskResponse(
            source=ImageSource(image="python:3.12-slim"),
            problem_path="/tmp/problem_statement.txt",
            cwd="/workspace",
            agent_timeout=60.0,
            resources=Resources(vcpu=2, memory=4, disk=10),
        )

    async def setup_task(
        self, task_id: str, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        """Setup task in sandbox — writes the problem statement to problem_path."""
        yield StreamMessageChunk(type="message", data=f"Setting up task {task_id}...")

        task = self.get_dataset(dataset)[task_id]
        problem_statement = task["problem"]
        await sandbox.upload_file("/tmp/problem_statement.txt", problem_statement.encode())

        yield StreamMessageChunk(type="message", data="Problem statement written to sandbox")
        yield StreamResultChunk(type="result", data={"status": "ok"})

    # Text benchmarks grade in-process (the default). To grade in a fresh,
    # network-isolated sandbox instead: add these imports, declare the mode and
    # accepted payload schemas on the class, then implement the hook.
    #
    # from benchmark_service import ArtifactGradingSubmission, EvalMode, GradingSubmission
    # from benchmark_service.v1_schemas import V1PayloadType
    #
    # eval_mode = EvalMode.SANDBOX
    # accepted_submission_schemas = {
    #     V1PayloadType.ARTIFACT: frozenset({"benchmark.artifact.v1"}),
    # }
    #
    # async def prepare_grading_sandbox(
    #     self, sandbox: Sandbox, submission: GradingSubmission, dataset: str | None = None
    # ) -> None:
    #     if not isinstance(submission, ArtifactGradingSubmission):
    #         raise ValueError("This benchmark requires an artifact submission")
    #     await sandbox.exec(f"tar -xf {submission.sandbox_path} -C /workspace")

    async def evaluate_response(self, request: EvaluateResponseRequest, dataset: str | None = None) -> Any:
        """Evaluate a text response."""
        if request.response is None:
            raise ValueError("This benchmark only supports text response evaluation")
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

    async def evaluate_instance(
        self, task_id: str, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        """Evaluate in sandbox (not implemented for this example)."""
        yield StreamMessageChunk(type="message", data=f"Evaluating task {task_id}...")
        yield StreamErrorChunk(
            type="error",
            data="Sandbox evaluation not implemented. Use /evaluate-response/ endpoint instead.",
        )

    async def calculate_final_score(
        self, evaluation_results: dict[str, Any], dataset: str | None = None
    ) -> FinalScoreResult:
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
