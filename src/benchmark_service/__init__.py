"""Benchmark service framework for creating evaluation APIs."""

from benchmark_service._version import __version__
from benchmark_service.allowlist import CatalogAllowlistClient
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.base import BenchmarkService
from benchmark_service.context import current_sandbox_provider, sandbox_provider_scope
from benchmark_service.client import (
    BenchmarkServiceClient,
    BenchmarkServiceError,
    BenchmarkServiceStreamError,
    BenchmarkServiceStreamClosedError,
    BenchmarkServiceStreamIdleError,
    BenchmarkServiceUnauthenticatedError,
    SandboxRecoveryAttempt,
)
from benchmark_service.inflight import InflightMiddleware
from benchmark_service.sandbox import (
    ComposeSource,
    ComposeSandbox,
    DaytonaProviderConfig,
    ExecResult,
    ImageSource,
    ModalProviderConfig,
    Resources,
    Sandbox,
    SandboxCommandError,
    SandboxConnectionError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxProviderConfig,
    SandboxQuery,
    SandboxSource,
    SnapshotSource,
    TargetedSnapshotSource,
    VolumeMount,
    sandbox_provider_config_from_mapping,
)
from benchmark_service.schemas import (
    ArtifactGradingSubmission,
    EvalMode,
    GradingSubmission,
    SandboxRecoveryPolicy,
    TextGradingSubmission,
)
from benchmark_service.submission_artifacts import MaterializedSubmissionArtifact, SubmissionArtifactReference

__all__ = [
    "BenchmarkServiceApp",
    "CatalogAllowlistClient",
    "BenchmarkService",
    "current_sandbox_provider",
    "BenchmarkServiceUnauthenticatedError",
    "BenchmarkServiceClient",
    "BenchmarkServiceError",
    "BenchmarkServiceStreamError",
    "BenchmarkServiceStreamClosedError",
    "BenchmarkServiceStreamIdleError",
    "SandboxRecoveryAttempt",
    "ArtifactGradingSubmission",
    "ComposeSource",
    "ComposeSandbox",
    "DaytonaProviderConfig",
    "EvalMode",
    "GradingSubmission",
    "ExecResult",
    "ImageSource",
    "InflightMiddleware",
    "ModalProviderConfig",
    "MaterializedSubmissionArtifact",
    "Resources",
    "Sandbox",
    "SandboxCommandError",
    "SandboxConnectionError",
    "SandboxCreateRequest",
    "SandboxError",
    "SandboxNotFoundError",
    "SandboxProvider",
    "SandboxProviderConfig",
    "sandbox_provider_scope",
    "SandboxQuery",
    "SandboxRecoveryPolicy",
    "SandboxSource",
    "SnapshotSource",
    "SubmissionArtifactReference",
    "TargetedSnapshotSource",
    "TextGradingSubmission",
    "VolumeMount",
    "sandbox_provider_config_from_mapping",
    "__version__",
]
