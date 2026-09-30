"""Shared Vals authentication, access policy, and provider routes."""

from benchmark_service.vals.app import ValsBenchmarkServiceApp
from benchmark_service.vals.base import ValsBenchmarkService

__all__ = ["ValsBenchmarkService", "ValsBenchmarkServiceApp"]
