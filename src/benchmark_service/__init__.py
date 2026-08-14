"""Benchmark service framework for creating evaluation APIs."""

from benchmark_service._version import __version__
from benchmark_service.allowlist import CatalogAllowlistClient
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.base import BenchmarkService
from benchmark_service.client import (
    BenchmarkServiceClient,
    BenchmarkServiceError,
    BenchmarkServiceStreamClosedError,
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
    "BenchmarkServiceUnauthenticatedError",
    "BenchmarkServiceClient",
    "BenchmarkServiceError",
    "BenchmarkServiceStreamClosedError",
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
