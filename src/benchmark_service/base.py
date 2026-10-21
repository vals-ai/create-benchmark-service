"""Base class for benchmark service implementations.

To create your own benchmark service, subclass BenchmarkService and implement
all the abstract methods. Then use the factory function in benchmark_service.app to create
a FastAPI app with your implementation.
"""

import asyncio
import hashlib
import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Mapping
from contextlib import AbstractAsyncContextManager, aclosing, asynccontextmanager
from functools import cached_property
from pathlib import Path
from typing import Any, ClassVar, Self
from uuid import uuid4

from fastapi.encoders import jsonable_encoder

from benchmark_service.auth import (
    UNAUTHENTICATED_TENANT_SENTINEL,
    check_benchmark_service_auth,
    resolve_caller_tenant,
)
from benchmark_service import eval_resume
from benchmark_service.context import current_sandbox_provider
from benchmark_service.dataset_versioning import DatasetVersionEntry, load_dataset_versions
from benchmark_service.sandbox import Sandbox, SandboxCreateRequest, SandboxProvider
from benchmark_service.schemas import (
    DatasetVersion,
    EvalMode,
    EvaluateResponseRequest,
    FinalScoreResult,
    GradingSubmission,
    MaterializedSubmissionArtifact,
    RetrieveTaskResponse,
    StreamChunk,
    StreamEvalResumeStateChunk,
    StreamResultChunk,
    TaskFilter,
)
from benchmark_service.v1_schemas import V1PayloadType, V1Task


def _prioritize_dataset(datasets: dict[str, dict[str, Any]], priority_dataset: str) -> dict[str, dict[str, Any]]:
    if priority_dataset not in datasets:
        raise ValueError(f"priority_dataset '{priority_dataset}' not found. Available datasets: {', '.join(datasets)}")
    priority_ids = datasets[priority_dataset].keys()
    return {
        name: dict(sorted(tasks.items(), key=lambda item: item[0] not in priority_ids))
        for name, tasks in datasets.items()
    }


_GRADERS = ("grade_input", "grade_in_sandbox")
_GRADING_HOOKS = ("collect_grading_input", "grader_identity", *_GRADERS)

EVAL_RESUME_SANDBOX_AUTO_STOP_MINUTES = 30
EVAL_RESUME_SANDBOX_CREATE_TIMEOUT_SECONDS = 600

logger = logging.getLogger(__name__)


async def _delete_owned_sandbox(provider: SandboxProvider, sandbox_id: str) -> None:
    """Delete a retry sandbox even when the stream that owns it is being cancelled."""
    cleanup = asyncio.create_task(provider.delete_sandbox(sandbox_id))
    try:
        await asyncio.shield(cleanup)
    except asyncio.CancelledError:
        await asyncio.shield(cleanup)
        raise
    except Exception:
        logger.exception("Failed to delete eval-resume sandbox %s", sandbox_id)


async def _create_owned_sandbox(provider: SandboxProvider, request: SandboxCreateRequest) -> Sandbox:
    """Create a retry sandbox; a cancellation mid-create still deletes what was created."""
    creation = asyncio.create_task(provider.create_sandbox(request))
    try:
        return await asyncio.shield(creation)
    except asyncio.CancelledError:

        async def delete_when_created() -> None:
            try:
                sandbox = await creation
            except Exception:
                return
            await _delete_owned_sandbox(provider, sandbox.id)

        cleanup = asyncio.create_task(delete_when_created())
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            pass
        raise


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

    # How /v1/evaluate grades this benchmark. The Vals app reads this at boot
    # and per dispatch, so it is class state, not a per-instance value.
    eval_mode: ClassVar[EvalMode] = EvalMode.TEXT
    accepted_submission_schemas: ClassVar[dict[V1PayloadType, frozenset[str]]] = {}

    # Task order within a dataset is dispatch order (the tracker creates task
    # rows in the order `filter_tasks` returns them). When set, every other
    # dataset is stably reordered so the tasks it shares with this dataset
    # come first, e.g. so a run of the full split finishes the index subset
    # before the long tail.
    priority_dataset: ClassVar[str | None] = None

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        # Checked here rather than in __init_subclass__ so an intermediate base class may
        # leave evaluate_instance and any of the three hooks to a subclass, as abstract methods allow.
        implemented = cls._implemented_grading_hooks()
        if all(grader in implemented for grader in _GRADERS):
            raise TypeError(f"{cls.__name__} must implement either grade_input or grade_in_sandbox, not both")
        if implemented and not cls._grades_input():
            raise TypeError(
                f"{cls.__name__} implements {', '.join(implemented)}; the framework's checkpointed grading "
                "needs collect_grading_input, grader_identity, and grade_input or grade_in_sandbox"
            )
        if cls.evaluate_instance is BenchmarkService.evaluate_instance and not implemented:
            raise TypeError(
                f"{cls.__name__} must implement evaluate_instance, or collect_grading_input, "
                "grader_identity, and grade_input or grade_in_sandbox"
            )
        return super().__new__(cls)

    @classmethod
    def _implemented_grading_hooks(cls) -> list[str]:
        return [hook for hook in _GRADING_HOOKS if getattr(cls, hook) is not getattr(BenchmarkService, hook)]

    @classmethod
    def _grades_input(cls) -> bool:
        implemented = set(cls._implemented_grading_hooks())
        return {"collect_grading_input", "grader_identity"} <= implemented and len(implemented & set(_GRADERS)) == 1

    @classmethod
    def _grades_in_sandbox(cls) -> bool:
        return cls.grade_in_sandbox is not BenchmarkService.grade_in_sandbox

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
        if cls.eval_mode == EvalMode.IN_PROCESS_MATERIALIZED_ARTIFACT:
            if cls.evaluate_materialized_artifact is BenchmarkService.evaluate_materialized_artifact:
                raise TypeError(
                    f"{cls.__name__} declares eval_mode = EvalMode.IN_PROCESS_MATERIALIZED_ARTIFACT "
                    "and must implement evaluate_materialized_artifact"
                )
        if cls.eval_mode in {EvalMode.IN_PROCESS_ARTIFACT, EvalMode.IN_PROCESS_MATERIALIZED_ARTIFACT}:
            artifact_schemas = cls.accepted_submission_schemas.get(V1PayloadType.ARTIFACT)
            if not artifact_schemas or set(cls.accepted_submission_schemas) != {V1PayloadType.ARTIFACT}:
                raise TypeError(
                    f"{cls.__name__} declares eval_mode = EvalMode.{cls.eval_mode.name} and must declare only "
                    "non-empty artifact accepted_submission_schemas"
                )

    @classmethod
    async def create(cls) -> Self:
        """Factory method to create and initialize a benchmark service."""
        instance = cls.__new__(cls)
        instance.dataset_versions = (
            load_dataset_versions(cls.dataset_versions_file) if cls.dataset_versions_file is not None else {}
        )
        instance.datasets = await instance.load_datasets()
        if cls.priority_dataset is not None:
            instance.datasets = _prioritize_dataset(instance.datasets, cls.priority_dataset)
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

        A boolean ``check_auth`` override authenticates without a tenant identity.
        Override this method to support tenant-aware dataset access.
        """
        if type(self).check_auth is not BenchmarkService.check_auth:
            ok = await self.check_auth(headers)
            return UNAUTHENTICATED_TENANT_SENTINEL if ok else None
        return await resolve_caller_tenant(headers)

    async def check_dataset_access(self, tenant: str, dataset: str | None) -> bool:
        """Return True if `tenant` may use `dataset` on this service."""
        return True

    async def list_datasets(self) -> list[str]:
        """Return available dataset names without loading tasks or selecting a version.

        Override for datasets discovered outside ``self.datasets``. The HTTP
        endpoint filters these names through ``check_dataset_access``.
        """
        return list(self.datasets)

    def supports_dataset_version_selection(self, dataset: str) -> bool:
        """Whether this dataset can honor immutable version IDs on every operation."""
        return False

    def open_dataset_version(self, dataset: str, version: str | None) -> AbstractAsyncContextManager[DatasetVersion]:
        """Bind a version to request-local state until the operation finishes.

        A null selector uses the configured default. Implementations own resource
        cleanup, including waiting for blocking preparation workers on cancellation.
        """
        raise NotImplementedError("Dataset version selection is not supported")

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

        The framework does not infer a public representation from arbitrary
        internal task objects: every field on this surface is one the
        benchmark put there deliberately. Held-out test-set content must
        never appear here in any form; rubrics and scoring detail for
        approved splits may be shared with labs. Benchmarks exposing the
        endpoint must override this method and explicitly return
        V1Task(id, question, timeout).

        Raises:
            NotImplementedError: if the benchmark has not opted into task listing.
        """
        raise NotImplementedError(f"{type(self).__name__}.list_tasks must explicitly map internal tasks to V1Task")

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
        carries its JSON form. A retry carrying ``eval_resume_state`` to a service
        that implements the grading hooks loads the saved grading input, checks it
        was saved under the current ``grader_identity``, and streams ``grade_input``.
        """
        if request.eval_resume_state is not None and self._grades_input():
            checkpoint, content, binding = await self._load_checkpoint(request, dataset)
            async with self._retry_sandbox(request, checkpoint, dataset) as sandbox:
                grading = self._grade(request.task_id, content, sandbox, dataset)
                async with aclosing(self._saving(grading, checkpoint.benchmark_id, request.task_id, binding)) as chunks:
                    async for chunk in chunks:
                        yield chunk
            return
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
            f"{type(self).__name__}.evaluate_artifact must be implemented for eval_mode == EvalMode.IN_PROCESS_ARTIFACT"
        )

    def evaluate_materialized_artifact(
        self,
        *,
        tenant: str,
        run_id: str,
        task_id: str,
        schema_id: str,
        artifact: MaterializedSubmissionArtifact,
        dataset: str | None = None,
    ) -> AsyncGenerator[StreamChunk, None]:
        """Grade a framework-owned local artifact for one authenticated tenant."""
        raise NotImplementedError(
            f"{type(self).__name__}.evaluate_materialized_artifact must be implemented for "
            "eval_mode == EvalMode.IN_PROCESS_MATERIALIZED_ARTIFACT"
        )

    async def collect_grading_input(self, task_id: str, sandbox: Sandbox, dataset: str | None = None) -> bytes:
        """Read the bytes ``grade_input`` grades from the finished sandbox, e.g. a result file.

        A grading hook. A service that implements ``collect_grading_input``,
        ``grader_identity``, and ``grade_input`` or ``grade_in_sandbox`` leaves
        ``evaluate_instance`` to the framework, which saves these bytes as an eval-resume
        checkpoint before grading them, so a grading failure is retried without rerunning the agent.
        """
        raise NotImplementedError(f"{type(self).__name__}.collect_grading_input is not implemented")

    def grader_identity(
        self, task_id: str, dataset: str | None = None
    ) -> Mapping[str, Any] | Awaitable[Mapping[str, Any]]:
        """Everything besides the grading input that decides this task's grade.

        A judge model, a test or rubric revision, a grader image digest, a hash of the
        grading code. A retry refuses a checkpoint saved under any other value. May be
        ``async`` when naming the grader needs I/O, such as resolving an image digest.
        """
        raise NotImplementedError(f"{type(self).__name__}.grader_identity is not implemented")

    def grade_input(
        self,
        task_id: str,
        content: bytes,
        dataset: str | None = None,
    ) -> AsyncGenerator[StreamChunk | eval_resume.GradingInput, None]:
        """Grade the saved bytes, on the first attempt and on retry; yield ``GradingInput`` to save progress."""
        raise NotImplementedError(f"{type(self).__name__}.grade_input is not implemented")

    def grade_in_sandbox(
        self,
        task_id: str,
        content: bytes,
        sandbox: Sandbox,
        dataset: str | None = None,
    ) -> AsyncGenerator[StreamChunk | eval_resume.GradingInput, None]:
        """Like ``grade_input``, inside the agent's sandbox or, on retry, a borrowed or fresh one."""
        raise NotImplementedError(f"{type(self).__name__}.grade_in_sandbox is not implemented")

    async def grading_input_saved(self, task_id: str, sandbox: Sandbox, dataset: str | None = None) -> None:
        """Release state kept only for collecting, once the first checkpoint is saved."""

    async def load_grading_input(self, request: EvaluateResponseRequest, dataset: str | None = None) -> bytes:
        """The grading input ``request.eval_resume_state`` points at, after the checks a retry makes."""
        _, content, _ = await self._load_checkpoint(request, dataset)
        return content

    async def _load_checkpoint(
        self, request: EvaluateResponseRequest, dataset: str | None
    ) -> tuple[eval_resume.EvalResumeCheckpoint, bytes, dict[str, str]]:
        if request.eval_resume_state is None:
            raise ValueError(f"task {request.task_id} has no eval_resume_state to load")
        await self.validate_task_ids([request.task_id], dataset=dataset)
        binding = await self._checkpoint_binding(request.task_id, dataset)
        checkpoint = self.eval_resume_store.parse(request.eval_resume_state)
        content = await self.eval_resume_store.load(checkpoint, task_id=request.task_id, **binding)
        return checkpoint, content, binding

    async def _checkpoint_binding(self, task_id: str, dataset: str | None) -> dict[str, str]:
        """The dataset and grader digest a checkpoint is saved under; save and load must agree on both."""
        parts = self.grader_identity(task_id, dataset=dataset)
        if isinstance(parts, Awaitable):
            parts = await parts
        return {"dataset": dataset or "default", "grader_identity": eval_resume.grader_identity(parts)}

    async def _save(
        self, benchmark_id: str, task_id: str, content: bytes, binding: dict[str, str]
    ) -> StreamEvalResumeStateChunk:
        checkpoint = await self.eval_resume_store.save(
            benchmark_id=benchmark_id, task_id=task_id, content=content, **binding
        )
        return StreamEvalResumeStateChunk(type="eval_resume_state", data=checkpoint.model_dump(mode="json"))

    def _grade(
        self, task_id: str, content: bytes, sandbox: Sandbox | None, dataset: str | None
    ) -> AsyncGenerator[StreamChunk | eval_resume.GradingInput, None]:
        if sandbox is not None and self._grades_in_sandbox():
            return self.grade_in_sandbox(task_id, content, sandbox, dataset=dataset)
        return self.grade_input(task_id, content, dataset=dataset)

    async def _saving(
        self,
        grading: AsyncGenerator[StreamChunk | eval_resume.GradingInput, None],
        benchmark_id: str,
        task_id: str,
        binding: dict[str, str],
    ) -> AsyncGenerator[StreamChunk, None]:
        """Stream ``grading``, saving each ``GradingInput`` it yields before asking it for more."""
        async with aclosing(grading) as items:
            async for item in items:
                if isinstance(item, eval_resume.GradingInput):
                    yield await self._save(benchmark_id, task_id, item.content, binding)
                else:
                    yield item

    @asynccontextmanager
    async def _retry_sandbox(
        self, request: EvaluateResponseRequest, checkpoint: eval_resume.EvalResumeCheckpoint, dataset: str | None
    ) -> AsyncIterator[Sandbox | None]:
        """The sandbox a ``grade_in_sandbox`` retry grades in; None for ``grade_input``."""
        if not self._grades_in_sandbox():
            yield None
            return
        provider = current_sandbox_provider()
        if provider is None:
            raise ValueError(f"{type(self).__name__} grades in a sandbox; a retry needs the request's sandbox_provider")
        if request.instance_id is not None:
            yield await provider.get_sandbox(request.instance_id)
            return
        task = await self.retrieve_task(request.task_id, skip_validation=True, dataset=dataset)
        spec = task.resume_sandbox
        digest = hashlib.sha256(f"{checkpoint.benchmark_id}:{checkpoint.task_id}:{checkpoint.sha256}".encode())
        sandbox = await _create_owned_sandbox(
            provider,
            SandboxCreateRequest(
                source=(spec.source if spec is not None and spec.source is not None else task.source),
                resources=(spec.resources if spec is not None and spec.resources is not None else task.resources),
                name=f"eval-resume-{digest.hexdigest()[:8]}-{uuid4().hex[:8]}",
                labels={"Id": checkpoint.benchmark_id, "Task": checkpoint.task_id, "EvalResume": "true"},
                env_vars={},
                auto_stop_interval=EVAL_RESUME_SANDBOX_AUTO_STOP_MINUTES,
                create_timeout=EVAL_RESUME_SANDBOX_CREATE_TIMEOUT_SECONDS,
                network_block_all=spec.network_block_all if spec is not None else False,
            ),
        )
        try:
            yield sandbox
        finally:
            await _delete_owned_sandbox(provider, sandbox.id)

    @cached_property
    def eval_resume_store(self) -> eval_resume.EvalResumeStore:
        """Where the default ``evaluate_instance`` saves grading input.

        Defaults to ``$EVAL_RESUME_BUCKET`` and ``$EVAL_RESUME_PREFIX``, which
        benchmark-services-registry sets for every service with eval-resume storage.
        """
        return eval_resume.EvalResumeStore()

    async def evaluate_instance(
        self, task_id: str, sandbox: Sandbox, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        """Evaluate a solution in a sandbox environment.

        The sandbox is already connected and ready to use. Interact with it to
        execute tests, run evaluation scripts, etc. Yield StreamChunk objects to stream progress.

        A service that implements the grading hooks can keep this default: it collects the
        grading input, saves it, yields the checkpoint as ``eval_resume_state``, calls
        ``grading_input_saved``, and then grades it. Otherwise override it:
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
        content = await self.collect_grading_input(task_id, sandbox, dataset=dataset)
        benchmark_id = eval_resume.benchmark_id_from_sandbox(sandbox)
        binding = await self._checkpoint_binding(task_id, dataset)
        saved = await self._save(benchmark_id, task_id, content, binding)
        await self.grading_input_saved(task_id, sandbox, dataset=dataset)
        yield saved
        grading = self._grade(task_id, content, sandbox, dataset)
        async with aclosing(self._saving(grading, benchmark_id, task_id, binding)) as chunks:
            async for chunk in chunks:
                yield chunk

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

        Each value is the payload your evaluation produced, or None for a task that reached
        no verdict -- both scoring surfaces deliver that shape.

        Args:
            evaluation_results: Dictionary mapping task_id to your evaluation result objects
            dataset: Name of the dataset. Defaults to 'default'.

        Returns:
            FinalScoreResult with score and metadata
        """
        ...
