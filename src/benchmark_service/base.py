"""Base class for benchmark service implementations.

To create your own benchmark service, subclass BenchmarkService and implement
all the abstract methods. Then use the factory function in benchmark_service.app to create
a FastAPI app with your implementation.
"""

from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any, ClassVar, Self

from fastapi.encoders import jsonable_encoder

from benchmark_service.auth import (
    LEGACY_TENANT_SENTINEL,
    check_benchmark_service_auth,
    get_tenant_config,
    resolve_caller_tenant,
)
from benchmark_service.dataset_versioning import DatasetVersionEntry, load_dataset_versions
from benchmark_service.sandbox import Sandbox
from benchmark_service.schemas import (
    EvalMode,
    EvaluateResponseRequest,
    FinalScoreResult,
    GradingSubmission,
    RetrieveTaskResponse,
    StreamChunk,
    StreamResultChunk,
    TaskFilter,
)
from benchmark_service.v1_schemas import V1PayloadType, V1Task


class BenchmarkService(ABC):
    """Abstract base class for benchmark implementations.

    Implement all abstract methods to create a working benchmark service.
    The FastAPI endpoints are already implemented and will call these methods.
    """

    datasets: dict[str, dict[str, Any]]

    # When set, the declared dataset versions are loaded at startup and served
    # by get_dataset_version(). Version label only — content is not verified.
    dataset_versions_file: ClassVar[Path | None] = None
    dataset_versions: dict[str, DatasetVersionEntry]

    # How /v1/evaluate grades this benchmark. The framework reads this at boot
    # and per dispatch, so it is class state, not a per-instance value.
    eval_mode: ClassVar[EvalMode] = EvalMode.TEXT
    accepted_submission_schemas: ClassVar[dict[V1PayloadType, frozenset[str]]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if (
            cls.eval_mode == EvalMode.SANDBOX
            and cls.prepare_grading_sandbox is BenchmarkService.prepare_grading_sandbox
        ):
            raise TypeError(
                f"{cls.__name__} declares eval_mode = EvalMode.SANDBOX and must implement prepare_grading_sandbox"
            )
        if cls.eval_mode == EvalMode.SANDBOX and not cls.accepted_submission_schemas:
            raise TypeError(
                f"{cls.__name__} declares eval_mode = EvalMode.SANDBOX and must declare accepted_submission_schemas"
            )
        if cls.eval_mode == EvalMode.IN_PROCESS_ARTIFACT:
            if cls.evaluate_artifact is BenchmarkService.evaluate_artifact:
                raise TypeError(
                    f"{cls.__name__} declares eval_mode = EvalMode.IN_PROCESS_ARTIFACT "
                    "and must implement evaluate_artifact"
                )
            artifact_schemas = cls.accepted_submission_schemas.get(V1PayloadType.ARTIFACT)
            if not artifact_schemas or set(cls.accepted_submission_schemas) != {V1PayloadType.ARTIFACT}:
                raise TypeError(
                    f"{cls.__name__} declares eval_mode = EvalMode.IN_PROCESS_ARTIFACT and must declare "
                    "only non-empty artifact accepted_submission_schemas"
                )

    @classmethod
    async def create(cls) -> Self:
        """Factory method to create and initialize a benchmark service."""
        instance = cls.__new__(cls)
        instance.dataset_versions = (
            load_dataset_versions(cls.dataset_versions_file)
            if cls.dataset_versions_file is not None
            else {}
        )
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
        """Validate request authorization.

        Defaults to the env-driven behavior in `benchmark_service.auth`.
        Override to implement custom authentication.

        Args:
            headers: Request headers (keys are lowercase per HTTP convention).

        Returns:
            True to allow the request, False to reject with 401.
        """
        return await check_benchmark_service_auth(headers)

    async def resolve_tenant(self, headers: dict[str, str]) -> str | None:
        """Authenticate the caller and return their tenant id, or None to reject.

        Subclasses with a legacy `check_auth` override keep their existing boolean
        behavior. A successful legacy check returns the "_legacy" sentinel, which
        skips dataset-level allowlist enforcement.
        """
        if type(self).check_auth is not BenchmarkService.check_auth:
            ok = await self.check_auth(headers)
            return LEGACY_TENANT_SENTINEL if ok else None
        return await resolve_caller_tenant(headers)

    async def check_dataset_access(self, tenant: str, dataset: str | None) -> bool:
        """Return True if `tenant` may use `dataset` on this service."""
        if tenant == LEGACY_TENANT_SENTINEL:
            return True
        entry = get_tenant_config(tenant)
        if entry is None:
            return False
        return (dataset or "default") in entry.datasets

    def get_service_version(self) -> str | None:
        """Return a benchmark-owned service version override, if available.

        The app falls back to the installed Python distribution version when
        this returns None.
        """
        return None

    def get_dataset_version(self, dataset: str | None = None) -> str | None:
        """Return the version for `dataset`, if this benchmark tracks one."""
        entry = self.dataset_versions.get(dataset or "default")
        return entry.version if entry is not None else None

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

    async def list_tasks(self, dataset: str | None = None) -> list[V1Task]:
        """Return tasks for `dataset` as the lab-facing /v1/ surface sees them.

        Benchmark task objects often contain evaluator-only fields (rubrics,
        expected answers, grader config, etc.) that must not leak to external
        labs. The framework therefore does not infer a public representation
        from arbitrary internal task objects. Benchmarks exposing the endpoint
        must override this method and explicitly return V1Task(id, question,
        timeout).

        Raises:
            NotImplementedError: if the benchmark has not opted into task listing.
        """
        raise NotImplementedError(
            f"{type(self).__name__}.list_tasks must explicitly map internal tasks to V1Task"
        )

    def project_trial_result(self, result: Any) -> Any:
        """Trial-safe projection of a per-task eval result.

        For `trial_mode` tenants, /v1/evaluate responses are reduced to what this
        returns, and that projection is all a trial caller can resubmit to
        /v1/score. So it must include both the score fields a prospect may see
        AND any field `calculate_final_score` needs to aggregate -- anything
        dropped here is gone from the final score too.

        Like `list_tasks`, the default raises so trial mode requires an explicit,
        audited projection rather than leaking rubric / judge data by omission.

        Raises:
            NotImplementedError: if the benchmark has not opted into trial mode.
        """
        raise NotImplementedError(
            f"{type(self).__name__}.project_trial_result must be implemented for trial_mode tenants"
        )

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
        - Return sandbox source, problem path, and resource requirements

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
        self, task_id: str, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        """Setup a task in a sandbox environment.

        The sandbox is already connected and ready to use. Interact with it to
        upload files, execute commands, etc. Yield StreamChunk objects to stream progress.

        Implement setup logic:
        1. Upload any setup scripts or data to the sandbox
        2. Execute setup commands using sandbox.exec
        3. Yield progress messages: yield StreamMessageChunk(type="message", data="log line")
        4. Yield error messages: yield StreamErrorChunk(type="error", data="error message")
        5. Yield final result: yield StreamResultChunk(type="result", data={"status": "ok"})

        Args:
            task_id: The task identifier
            sandbox: Connected sandbox instance
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

    async def stream_evaluate_response(
        self, request: EvaluateResponseRequest, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        """Evaluate without a sandbox and stream the result.

        evaluate_response may return any JSON-encodable object; the chunk
        carries its JSON form.
        """
        result = await self.evaluate_response(request, dataset=dataset)
        yield StreamResultChunk(type="result", data=jsonable_encoder(result))

    def evaluate_artifact(
        self,
        run_id: str,
        task_id: str,
        schema_id: str,
        artifact: bytes,
        dataset: str | None = None,
    ) -> AsyncGenerator[StreamChunk, None]:
        """Grade framework-admitted artifact bytes for one run in the service process."""
        raise NotImplementedError(
            f"{type(self).__name__}.evaluate_artifact must be implemented for "
            "eval_mode == EvalMode.IN_PROCESS_ARTIFACT"
        )

    @abstractmethod
    def evaluate_instance(
        self, task_id: str, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        """Evaluate a solution in a sandbox environment.

        The sandbox is already connected and ready to use. Interact with it to
        execute tests, run evaluation scripts, etc. Yield StreamChunk objects to stream progress.

        Implement evaluation logic:
        1. Execute tests or evaluation scripts using sandbox.exec
        2. Parse test output and grade results
        3. Yield progress logs: yield StreamMessageChunk(type="message", data="log line")
        4. Yield error messages: yield StreamErrorChunk(type="error", data="error message")
        5. Yield final result: yield StreamResultChunk(type="result", data=evaluation_result)

        Generator cleanup must remain cancellation-cooperative. Finalizers must
        not catch and suppress asyncio.CancelledError indefinitely.

        Args:
            task_id: The task identifier
            sandbox: Connected sandbox instance
            dataset: Name of the dataset. Defaults to 'default'.

        Yields:
            StreamChunk - one of StreamMessageChunk, StreamResultChunk, or StreamErrorChunk
        """
        ...

    async def prepare_grading_sandbox(
        self,
        sandbox: Sandbox,
        submission: GradingSubmission,
        dataset: str | None = None,
    ) -> None:
        """Place a typed submission where evaluate_instance expects it.

        Artifact submissions have already been downloaded to
        submission.sandbox_path. Their artifact_reference is the immutable
        key, size, and ETag captured during preflight and used for that exact
        download. Evaluator secrets must come from private deployment config
        or server-side benchmark code, never task metadata.
        """
        raise NotImplementedError(
            f"{type(self).__name__}.prepare_grading_sandbox must be implemented for eval_mode == EvalMode.SANDBOX"
        )

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
