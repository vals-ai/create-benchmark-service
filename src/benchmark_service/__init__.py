"""Benchmark service framework for creating evaluation APIs."""

from benchmark_service._version import __version__
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.auth import AuthFailure, AuthResult
from benchmark_service.base import BenchmarkService
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError, BenchmarkServiceUnauthenticatedError
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
    sandbox_provider_config_from_mapping,
)

__all__ = [
    "AuthFailure",
    "AuthResult",
    "BenchmarkServiceApp",
    "BenchmarkService",
    "BenchmarkServiceUnauthenticatedError",
    "BenchmarkServiceClient",
    "BenchmarkServiceError",
    "ComposeSource",
    "ComposeSandbox",
    "DaytonaProviderConfig",
    "ExecResult",
    "ImageSource",
    "InflightMiddleware",
    "ModalProviderConfig",
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
    "SandboxSource",
    "SnapshotSource",
    "sandbox_provider_config_from_mapping",
    "__version__",
]
