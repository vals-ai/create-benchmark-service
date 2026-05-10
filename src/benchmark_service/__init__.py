"""Benchmark service framework for creating evaluation APIs."""

from benchmark_service._version import __version__
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.base import BenchmarkService
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError
from benchmark_service.inflight import InflightMiddleware

__all__ = [
    "BenchmarkServiceApp",
    "BenchmarkService",
    "BenchmarkServiceClient",
    "BenchmarkServiceError",
    "InflightMiddleware",
    "__version__",
]
