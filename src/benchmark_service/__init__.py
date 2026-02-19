"""Benchmark service framework for creating evaluation APIs."""

from benchmark_service.app import create_app
from benchmark_service.base import BenchmarkService
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError

__all__ = [
    "BenchmarkService",
    "BenchmarkServiceClient",
    "BenchmarkServiceError",
    "create_app",
]
