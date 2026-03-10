"""Base class for benchmark service implementations.

To create your own benchmark service, subclass BenchmarkService and implement
all the abstract methods. Then use the factory function in benchmark_service.app to create
a FastAPI app with your implementation.
"""

from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from typing import Any, Self

from daytona import AsyncSandbox

from benchmark_service.schemas import (
    EvaluateResponseRequest,
    FinalScoreResult,
    RetrieveTaskResponse,
    StreamChunk,
    TaskFilter,
)


class BenchmarkService(ABC):
    """Abstract base class for benchmark implementations.

    Implement all abstract methods to create a working benchmark service.
    The FastAPI endpoints are already implemented and will call these methods.
    """

    datasets: dict[str, dict[str, Any]]

    @classmethod
    async def create(cls) -> Self:
        """Factory method to create and initialize a benchmark service."""
        instance = cls.__new__(cls)
        instance.datasets = await instance.load_datasets()
        return instance

    @abstractmethod
    async def load_datasets(self) -> dict[str, dict[str, Any]]:
        """Load the complete benchmark datasets.

        Implement dataset loading logic:
        - Load all tasks from your benchmark source (files, database, etc.)
        - Return a mapping of dataset names to task dictionaries
        - Each task dictionary maps task IDs to your benchmark-specific task objects
        - The task objects can be any structure (dataclass, Pydantic model, dict, etc.)

        Returns:
            Dictionary mapping dataset names to dictionaries of task IDs to task objects
        """
        ...

    async def check_auth(self, headers: dict[str, str]) -> bool:
        """Validate request authorization. Override to enforce authentication.

        Called on every HTTP request except /health. Return False to reject
        the request with 401 Unauthorized.

        Args:
            headers: Request headers (keys are lowercase per HTTP convention).

        Returns:
            True to allow the request (default), False to reject with 401.
        """
        return True

    def get_dataset(self, dataset: str | None = None) -> dict[str, Any]:
        """Get a specific dataset by name. Defaults to 'default'."""
        key = dataset or "default"
        if key not in self.datasets:
            raise ValueError(f"Dataset '{key}' not found. Available datasets: {', '.join(self.datasets.keys())}")
        return self.datasets[key]

    async def filter_tasks(self, task_filter: TaskFilter, dataset: str | None = None) -> list[str]:
        """Filter tasks based on provided criteria.

        Args:
            task_filter: Filter criteria for selecting tasks
            dataset: Name of the dataset to filter tasks from. Defaults to 'default'.

        Returns:
            List of task IDs that match the filter criteria
        """
        all_task_ids = list(self.get_dataset(dataset).keys())

        if task_filter.task_ids:
            return await self.validate_task_ids(task_filter.task_ids, dataset=dataset)

        if task_filter.slice_str:
            slice_obj = task_filter.parse_slice()
            return all_task_ids[slice_obj]

        return all_task_ids

    async def validate_task_ids(self, task_ids: list[str], dataset: str | None = None) -> list[str]:
        """Validate that task IDs exist in your benchmark dataset.

        Args:
            task_ids: List of task IDs to validate
            dataset: Name of the dataset to validate against. Defaults to 'default'.

        Returns:
            The same list of task IDs if all are valid

        Raises:
            ValueError: If any task ID is invalid
        """
        ds = self.get_dataset(dataset)
        for task_id in task_ids:
            if task_id not in ds:
                raise ValueError(f"Task ID not found: {task_id}")
        return task_ids

    @abstractmethod
    async def retrieve_task(
        self, task_id: str, skip_validation: bool = False, dataset: str | None = None
    ) -> RetrieveTaskResponse:
        """Retrieve task metadata including environment specification and problem path.

        Implement metadata retrieval:
        - Validate task_id exists (unless skip_validation=True)
        - Load task information from your dataset
        - Return docker image, problem path, and resource requirements

        The problem_path is the path inside the sandbox where setup_task will
        write the problem statement file.

        Args:
            task_id: The task to retrieve metadata for
            skip_validation: Skip validation of task existence
            dataset: Name of the dataset to retrieve from. Defaults to 'default'.

        Returns:
            RetrieveTaskResponse with task metadata
        """
        ...

    @abstractmethod
    def setup_task(
        self, task_id: str, sandbox: AsyncSandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        """Setup a task in a sandbox environment.

        The sandbox is already connected and ready to use. Interact with it to
        upload files, execute commands, etc. Yield StreamChunk objects to stream progress.

        Implement setup logic:
        1. Upload any setup scripts or data to the sandbox
        2. Execute setup commands using sandbox.process
        3. Yield progress messages: yield StreamMessageChunk(type="message", data="log line")
        4. Yield error messages: yield StreamErrorChunk(type="error", data="error message")
        5. Yield final result: yield StreamResultChunk(type="result", data={"status": "ok"})

        Args:
            task_id: The task identifier
            sandbox: Connected Daytona sandbox instance
            dataset: Name of the dataset. Defaults to 'default'.

        Yields:
            StreamChunk - one of StreamMessageChunk, StreamResultChunk, or StreamErrorChunk
        """
        ...

    @abstractmethod
    async def evaluate_response(self, request: EvaluateResponseRequest, dataset: str | None = None) -> Any:
        """Evaluate a text response directly (without sandbox execution).

        Use this for benchmarks where you can evaluate responses without
        executing code or running tests.

        Implement evaluation logic:
        1. Load the expected answer/criteria for the task
        2. Compare the response against expected output
        3. Calculate scores or metrics
        4. Return your benchmark-specific evaluation result

        Args:
            request: Evaluation request with task_id and response text
            dataset: Name of the dataset. Defaults to 'default'.

        Returns:
            Your benchmark-specific evaluation result (dict, Pydantic model, etc.)
        """
        ...

    @abstractmethod
    def evaluate_instance(
        self, task_id: str, sandbox: AsyncSandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        """Evaluate a solution in a sandbox environment.

        The sandbox is already connected and ready to use. Interact with it to
        execute tests, run evaluation scripts, etc. Yield StreamChunk objects to stream progress.

        Implement evaluation logic:
        1. Execute tests or evaluation scripts using sandbox.process
        2. Parse test output and grade results
        3. Yield progress logs: yield StreamMessageChunk(type="message", data="log line")
        4. Yield error messages: yield StreamErrorChunk(type="error", data="error message")
        5. Yield final result: yield StreamResultChunk(type="result", data=evaluation_result)

        Args:
            task_id: The task identifier
            sandbox: Connected Daytona sandbox instance
            dataset: Name of the dataset. Defaults to 'default'.

        Yields:
            StreamChunk - one of StreamMessageChunk, StreamResultChunk, or StreamErrorChunk
        """
        ...

    @abstractmethod
    async def calculate_final_score(
        self, evaluation_results: dict[str, Any], dataset: str | None = None
    ) -> FinalScoreResult:
        """Calculate final aggregate score from all evaluation results.

        Implement scoring logic:
        - Define how to aggregate individual task results
        - Common approach: percentage of resolved tasks
        - You may want weighted scoring or other metrics
        - Return any metadata you want to include

        Args:
            evaluation_results: Dictionary mapping task_id to your evaluation result objects
            dataset: Name of the dataset. Defaults to 'default'.

        Returns:
            FinalScoreResult with score and metadata
        """
        ...
