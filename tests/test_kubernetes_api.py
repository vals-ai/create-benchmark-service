"""Tests for Kubernetes client error normalization.

Run: uv run pytest tests/test_kubernetes_api.py
"""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import AsyncMock

import aiohttp
import pytest

from benchmark_service.sandbox.kubernetes.control.api import KubernetesApiError, KubernetesAsyncioApi


async def test_maps_transport_failures_to_retryable_api_errors() -> None:
    """Keep client-library connection details behind the provider boundary.

    Test cases:
    - An aiohttp connection failure receives synthetic status zero.
    - The original failure remains available as the exception cause.
    """
    api = cast(Any, object.__new__(KubernetesAsyncioApi))
    connection_error = aiohttp.ClientConnectionError("connection reset")
    api._batch = type("BatchApi", (), {"create_namespaced_job": AsyncMock(side_effect=connection_error)})()

    with pytest.raises(KubernetesApiError) as error_info:
        await api.create_job("benchmark-sandboxes", {})

    assert error_info.value.status == 0
    assert error_info.value.__cause__ is connection_error
