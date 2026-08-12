"""Tests for BenchmarkServiceClient."""

import asyncio
import json
import os
import socket
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import httpx
import pytest
from pydantic import ValidationError
from websockets.exceptions import ConnectionClosedError
from websockets.frames import Close

from benchmark_service import (
    BenchmarkServiceStreamClosedError,
    BenchmarkServiceStreamIdleError,
    SandboxNotFoundError,
    SandboxRecoveryPolicy,
)
from benchmark_service.client import (
    _SERVER_PING_INTERVAL_S,  # pyright: ignore[reportPrivateUsage]
    _SERVER_PING_TIMEOUT_S,  # pyright: ignore[reportPrivateUsage]
    BenchmarkServiceClient,
    BenchmarkServiceError,
    SandboxRecoveryAttempt,
)
from benchmark_service.sandbox.daytona import DaytonaProviderConfig
from benchmark_service.sandbox.modal import ModalProviderConfig
from benchmark_service.schemas import HealthCheckResponse, RetrieveTaskResponse
from benchmark_service.v1_schemas import (
    V1DatasetTasksResponse,
    V1EvalResponse,
    V1EvalStatus,
    V1PayloadType,
    V1ScoreItem,
    V1ScoreResponse,
    V1UploadUrlResponse,
    V1Versions,
)

BASE_URL = "http://localhost:8000"
HEADERS = {"Authorization": "Bearer token"}
DAYTONA_CONFIG = DaytonaProviderConfig(DAYTONA_API_KEY="key", DAYTONA_API_URL="url", DAYTONA_TARGET="target")


class RetryableSetupError(Exception):
    """Test-only error representing a fresh-sandbox setup retry."""


def _task_response(max_sandbox_attempts: int | None = None) -> RetrieveTaskResponse:
    policy = {"max_sandbox_attempts": max_sandbox_attempts} if max_sandbox_attempts is not None else None
    return RetrieveTaskResponse.model_validate(
        {
            "source": {"type": "image", "image": "python:3.12"},
            "problem_path": "/tmp/problem_statement.txt",
            "cwd": "/work",
            "resources": {"vcpu": 2, "memory": 4, "disk": 10},
            "sandbox_recovery": policy,
        }
    )


def _mock_response(status_code: int = 200, json_data: Any = None, text: str = "error") -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.text = text
    return resp


@pytest.mark.parametrize(
    ("method", "args", "expected_path", "json_data"),
    [
        (
            "health_check",
            [],
            "/health",
            {"status": "ok"},
        ),
        (
            "version",
            [],
            "/version",
            {
                "framework_version": "0.7.4",
                "service_name": "legal-research-benchmark-service",
                "service_version": "1.2.3",
                "dataset_version": "3.0.0",
                "eval_mode": "text",
            },
        ),
        (
            "verify_task_ids",
            [["t1", "t2"], None],
            "/verify-task-ids",
            {"task_ids": ["t1", "t2"]},
        ),
        (
            "retrieve_task",
            ["task-1"],
            "/retrieve-task/",
            {
                "source": {"type": "image", "image": "python:3.12"},
                "docker_image": "python:3.12",
                "problem_path": "/tmp/problem_statement.txt",
                "cwd": "/work",
                "resources": {"vcpu": 2, "memory": 4, "disk": 10, "gpu": 0, "gpu_type": None},
                "agent_timeout": None,
                "eval_sandbox": None,
                "sandbox_recovery": None,
                "sandbox_secrets": {},
                "volumes": [],
            },
        ),
        (
            "final_score",
            [{"t1": {"resolved": True}}],
            "/final-score/",
            {"tasks_evaluated": ["t1"], "final_score": 100.0, "metadata": {}},
        ),
    ],
    ids=["health_check", "version", "verify_task_ids", "retrieve_task", "final_score"],
)
async def test_http_happy_path(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
    method: str,
    args: list[Any],
    expected_path: str,
    json_data: dict[str, Any],
) -> None:
    client, mock_http = benchmark_client
    mock_resp = _mock_response(json_data=json_data)
    mock_http.get = AsyncMock(return_value=mock_resp)
    mock_http.post = AsyncMock(return_value=mock_resp)

    result = await getattr(client, method)(*args)

    assert result.model_dump() == json_data


async def test_retrieve_task_accepts_legacy_shape(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
) -> None:
    client, mock_http = benchmark_client
    mock_http.get = AsyncMock(
        return_value=_mock_response(
            json_data={
                "docker_image": "python:3.12",
                "problem_path": "/tmp/problem_statement.txt",
                "cwd": "/work",
                "resources": {"vcpu": 2, "memory": 4, "disk": 10},
                "agent_timeout": None,
            }
        )
    )

    result = await client.retrieve_task("task-1")

    assert result.source.model_dump() == {"type": "image", "image": "python:3.12"}
    assert result.model_dump()["docker_image"] == "python:3.12"
    assert result.resources.model_dump() == {"vcpu": 2, "memory": 4, "disk": 10, "gpu": 0, "gpu_type": None}


async def test_retrieve_task_accepts_sandbox_recovery_policy(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
) -> None:
    client, mock_http = benchmark_client
    mock_http.get = AsyncMock(
        return_value=_mock_response(
            json_data={
                "source": {"type": "image", "image": "python:3.12"},
                "problem_path": "/tmp/problem_statement.txt",
                "cwd": "/work",
                "resources": {"vcpu": 2, "memory": 4, "disk": 10},
                "agent_timeout": 432_000,
                "sandbox_recovery": {"max_sandbox_attempts": 10},
            }
        )
    )

    result = await client.retrieve_task("task-1")

    assert result.sandbox_recovery is not None
    assert result.sandbox_recovery.max_sandbox_attempts == 10


@pytest.mark.parametrize("max_sandbox_attempts", [1, 21])
def test_sandbox_recovery_policy_rejects_out_of_range_attempts(max_sandbox_attempts: int) -> None:
    with pytest.raises(ValidationError):
        SandboxRecoveryPolicy(max_sandbox_attempts=max_sandbox_attempts)


async def test_client_keeps_task_loading_lazy_when_initial_operation_completes(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _mock_http = benchmark_client
    retrieve_task = AsyncMock(return_value=_task_response(3))
    monkeypatch.setattr(client, "retrieve_task", retrieve_task)

    async def operation(attempt: SandboxRecoveryAttempt) -> int:
        return attempt.number

    result = await client.run_with_sandbox_recovery(
        "task-1",
        "run-1",
        operation,
        retry_delay_s=0,
    )

    assert result == 1
    retrieve_task.assert_not_awaited()


@pytest.mark.parametrize(
    ("task_id", "run_id", "message"),
    [
        ("", "run-1", "task_id must be non-empty"),
        ("task-1", "", "run_id must be non-empty"),
    ],
)
async def test_client_rejects_empty_recovery_identity(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
    task_id: str,
    run_id: str,
    message: str,
) -> None:
    client, _mock_http = benchmark_client

    with pytest.raises(ValueError, match=message):
        await client.run_with_sandbox_recovery(
            task_id,
            run_id,
            AsyncMock(),
            retry_delay_s=0,
        )


async def test_client_rejects_sandbox_loss_in_caller_retry_types(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
) -> None:
    client, _mock_http = benchmark_client

    with pytest.raises(ValueError, match="controlled only by SandboxRecoveryPolicy"):
        await client.run_with_sandbox_recovery(
            "task-1",
            "run-1",
            AsyncMock(),
            retryable_attempt_errors=(SandboxNotFoundError,),
            retry_delay_s=0,
        )


async def test_client_awaits_async_retry_callback(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _mock_http = benchmark_client
    monkeypatch.setattr(client, "retrieve_task", AsyncMock(return_value=_task_response(2)))
    on_retry = AsyncMock()

    async def operation(attempt: SandboxRecoveryAttempt) -> str:
        if attempt.number == 1:
            raise SandboxNotFoundError("sandbox disappeared")
        return "finished"

    result = await client.run_with_sandbox_recovery(
        "task-1",
        "run-1",
        operation,
        retry_delay_s=0,
        on_retry=on_retry,
    )

    assert result == "finished"
    on_retry.assert_awaited_once()


async def test_client_outage_ids_are_unique_across_invocations(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _mock_http = benchmark_client
    monkeypatch.setattr(client, "retrieve_task", AsyncMock(return_value=_task_response(2)))
    identifiers = iter((UUID(int=1), UUID(int=2)))
    monkeypatch.setattr("benchmark_service.client.uuid4", lambda: next(identifiers))

    async def operation(attempt: SandboxRecoveryAttempt) -> str:
        if attempt.number == 1:
            raise SandboxNotFoundError("sandbox disappeared")
        assert attempt.outage_id is not None
        return attempt.outage_id

    first = await client.run_with_sandbox_recovery("task-1", "run-1", operation, retry_delay_s=0)
    second = await client.run_with_sandbox_recovery("task-1", "run-1", operation, retry_delay_s=0)

    assert first != second
    assert first.endswith("00000000000000000000000000000001")
    assert second.endswith("00000000000000000000000000000002")


async def test_client_recovers_consecutive_losses_with_distinct_outage_identity(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _mock_http = benchmark_client
    monkeypatch.setattr(client, "retrieve_task", AsyncMock(return_value=_task_response(3)))
    monkeypatch.setattr("benchmark_service.client.time.time", lambda: 1_234.5)
    monkeypatch.setattr("benchmark_service.client.uuid4", lambda: UUID(int=1))

    attempts: list[SandboxRecoveryAttempt] = []
    retry_errors: list[Exception] = []

    async def operation(
        attempt: SandboxRecoveryAttempt,
    ) -> str:
        attempts.append(attempt)
        attempt.mark_replacement_ready()
        if attempt.number < 3:
            raise SandboxNotFoundError("sandbox disappeared")
        return "finished"

    result = await client.run_with_sandbox_recovery(
        "task-1",
        "run-1",
        operation,
        retry_delay_s=0,
        on_retry=lambda _attempt, error: retry_errors.append(error),
    )

    assert result == "finished"
    assert [attempt.number for attempt in attempts] == [1, 2, 3]
    assert [attempt.max_attempts for attempt in attempts] == [3, 3, 3]
    assert [attempt.sandbox_loss_retry_available for attempt in attempts] == [True, True, False]
    assert [attempt.environment for attempt in attempts] == [
        {},
        {
            "VALKYRIE_SANDBOX_OUTAGE_ID": "run-1:task-1:1:00000000000000000000000000000001",
            "VALKYRIE_SANDBOX_OUTAGE_STARTED_EPOCH": "1234.5",
        },
        {
            "VALKYRIE_SANDBOX_OUTAGE_ID": "run-1:task-1:2:00000000000000000000000000000001",
            "VALKYRIE_SANDBOX_OUTAGE_STARTED_EPOCH": "1234.5",
        },
    ]
    assert len(retry_errors) == 2


async def test_client_preserves_outage_identity_when_replacement_setup_retries(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _mock_http = benchmark_client
    monkeypatch.setattr(client, "retrieve_task", AsyncMock(return_value=_task_response(3)))
    monkeypatch.setattr("benchmark_service.client.time.time", lambda: 1_234.5)
    monkeypatch.setattr("benchmark_service.client.uuid4", lambda: UUID(int=1))

    environments: list[dict[str, str]] = []

    async def operation(
        attempt: SandboxRecoveryAttempt,
    ) -> str:
        environments.append(attempt.environment)
        if attempt.number == 1:
            raise SandboxNotFoundError("sandbox disappeared")
        if attempt.number == 2:
            raise RetryableSetupError("replacement setup failed")
        attempt.mark_replacement_ready()
        return "finished"

    result = await client.run_with_sandbox_recovery(
        "task-1",
        "run-1",
        operation,
        retryable_attempt_errors=(RetryableSetupError,),
        default_max_attempts=2,
        retry_delay_s=0,
    )

    assert result == "finished"
    assert environments == [
        {},
        {
            "VALKYRIE_SANDBOX_OUTAGE_ID": "run-1:task-1:1:00000000000000000000000000000001",
            "VALKYRIE_SANDBOX_OUTAGE_STARTED_EPOCH": "1234.5",
        },
        {
            "VALKYRIE_SANDBOX_OUTAGE_ID": "run-1:task-1:1:00000000000000000000000000000001",
            "VALKYRIE_SANDBOX_OUTAGE_STARTED_EPOCH": "1234.5",
        },
    ]


async def test_client_does_not_recover_lost_sandbox_without_policy(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _mock_http = benchmark_client
    monkeypatch.setattr(client, "retrieve_task", AsyncMock(return_value=_task_response()))
    calls = 0
    attempts: list[SandboxRecoveryAttempt] = []

    async def operation(
        attempt: SandboxRecoveryAttempt,
    ) -> None:
        nonlocal calls
        calls += 1
        attempts.append(attempt)
        raise SandboxNotFoundError("sandbox disappeared")

    with pytest.raises(SandboxNotFoundError, match="sandbox disappeared"):
        await client.run_with_sandbox_recovery(
            "task-1",
            "run-1",
            operation,
            default_max_attempts=2,
            retry_delay_s=0,
        )

    assert calls == 1
    assert attempts[0].sandbox_loss_retry_available is False


async def test_client_applies_default_attempt_cap_to_caller_setup_errors(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _mock_http = benchmark_client
    monkeypatch.setattr(client, "retrieve_task", AsyncMock(return_value=_task_response()))
    calls = 0

    async def operation(
        attempt: SandboxRecoveryAttempt,
    ) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RetryableSetupError("setup failed")
        return attempt.number

    result = await client.run_with_sandbox_recovery(
        "task-1",
        "run-1",
        operation,
        retryable_attempt_errors=(RetryableSetupError,),
        default_max_attempts=2,
        retry_delay_s=0,
    )

    assert result == 2
    assert calls == 2


async def test_client_keeps_setup_retry_cap_with_larger_recovery_policy(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _mock_http = benchmark_client
    monkeypatch.setattr(client, "retrieve_task", AsyncMock(return_value=_task_response(10)))
    attempts: list[int] = []

    async def operation(
        attempt: SandboxRecoveryAttempt,
    ) -> None:
        await attempt.retrieve_task()
        attempts.append(attempt.number)
        raise RetryableSetupError("setup failed")

    with pytest.raises(RetryableSetupError, match="setup failed"):
        await client.run_with_sandbox_recovery(
            "task-1",
            "run-1",
            operation,
            retryable_attempt_errors=(RetryableSetupError,),
            default_max_attempts=2,
            retry_delay_s=0,
        )

    assert attempts == [1, 2]


async def test_client_reraises_provider_error_when_policy_cap_is_exhausted(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _mock_http = benchmark_client
    monkeypatch.setattr(client, "retrieve_task", AsyncMock(return_value=_task_response(2)))
    attempts: list[int] = []

    async def operation(
        attempt: SandboxRecoveryAttempt,
    ) -> None:
        attempts.append(attempt.number)
        attempt.mark_replacement_ready()
        raise SandboxNotFoundError(f"loss-{attempt.number}")

    with pytest.raises(SandboxNotFoundError, match="loss-2"):
        await client.run_with_sandbox_recovery(
            "task-1",
            "run-1",
            operation,
            retry_delay_s=0,
        )

    assert attempts == [1, 2]


async def test_retrieve_task_tolerates_legacy_enable_docker_field(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
) -> None:
    # Older benchmark services still send `enable_docker`; the field was removed
    # (providers grant nested-Docker capability unconditionally) and must be
    # ignored rather than rejected.
    client, mock_http = benchmark_client
    mock_http.get = AsyncMock(
        return_value=_mock_response(
            json_data={
                "source": {"type": "image", "image": "docker:27-dind"},
                "problem_path": "/tmp/problem_statement.txt",
                "cwd": "/work",
                "resources": {"vcpu": 2, "memory": 4, "disk": 10, "enable_docker": True},
                "agent_timeout": None,
            }
        )
    )

    result = await client.retrieve_task("task-1")

    assert result.resources.model_dump() == {"vcpu": 2, "memory": 4, "disk": 10, "gpu": 0, "gpu_type": None}


async def test_retrieve_task_serializes_snapshot_source_for_legacy_clients(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
) -> None:
    client, mock_http = benchmark_client
    mock_http.get = AsyncMock(
        return_value=_mock_response(
            json_data={
                "source": {"type": "snapshot", "snapshot": "vcb1-openhands-abc123"},
                "problem_path": "/tmp/problem_statement.txt",
                "cwd": "/work",
                "resources": {"vcpu": 2, "memory": 4, "disk": 10},
                "agent_timeout": None,
            }
        )
    )

    result = await client.retrieve_task("task-1")

    assert result.model_dump()["docker_image"] == "snapshot:vcb1-openhands-abc123"


async def test_retrieve_task_serializes_targeted_snapshot_as_invalid_legacy_image(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
) -> None:
    client, mock_http = benchmark_client
    mock_http.get = AsyncMock(
        return_value=_mock_response(
            json_data={
                "source": {
                    "type": "targeted_snapshot",
                    "snapshot": "programbench-masscan",
                    "target": "us-west-3",
                },
                "problem_path": "/tmp/problem_statement.txt",
                "cwd": "/work",
                "resources": {"vcpu": 4, "memory": 16, "disk": 30},
                "agent_timeout": None,
            }
        )
    )

    result = await client.retrieve_task("task-1")

    assert result.model_dump()["docker_image"] == "targeted-snapshot+source-required"
    assert result.source.model_dump() == {
        "type": "targeted_snapshot",
        "snapshot": "programbench-masscan",
        "target": "us-west-3",
    }


async def test_retrieve_task_serializes_compose_source_as_invalid_legacy_image(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
) -> None:
    """Compose-backed tasks should not advertise the outer image to older clients.

    Test cases:
    - Compose source reports an intentionally invalid legacy image.
    - Compose source metadata is preserved.
    """
    client, mock_http = benchmark_client
    mock_http.get = AsyncMock(
        return_value=_mock_response(
            json_data={
                "source": {
                    "type": "compose",
                    "outer": {"type": "image", "image": "docker:28.3.3-dind"},
                    "service": "main",
                    "compose_command": "docker compose -f /harbor/compose.yaml",
                },
                "problem_path": "/tmp/problem_statement.txt",
                "cwd": "/work",
                "resources": {"vcpu": 2, "memory": 4, "disk": 10},
                "agent_timeout": None,
            }
        )
    )

    result = await client.retrieve_task("task-1")

    assert result.model_dump()["docker_image"] == "compose+source-required"
    assert result.source.model_dump()["type"] == "compose"


@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("health_check", []),
        ("verify_task_ids", [None, None]),
        ("retrieve_task", ["task-1"]),
        ("final_score", [{"t1": {"resolved": True}}]),
    ],
    ids=["health_check", "verify_task_ids", "retrieve_task", "final_score"],
)
async def test_http_error(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
    method: str,
    args: list[Any],
) -> None:
    client, mock_http = benchmark_client
    mock_resp = _mock_response(status_code=500)
    mock_http.get = AsyncMock(return_value=mock_resp)
    mock_http.post = AsyncMock(return_value=mock_resp)

    with pytest.raises(BenchmarkServiceError) as exc_info:
        await getattr(client, method)(*args)

    assert exc_info.value.status_code == 500


@pytest.mark.parametrize(
    ("task_ids", "slice_str", "expected_params"),
    [
        (None, None, {}),
        (["a", "b"], None, {"task_ids": ["a", "b"]}),
        (None, "0:5", {"slice": "0:5"}),
        (["a"], "0:5", {"task_ids": ["a"], "slice": "0:5"}),
    ],
    ids=["none-none", "ids-only", "slice-only", "both"],
)
async def test_verify_task_ids_params(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
    task_ids: list[str] | None,
    slice_str: str | None,
    expected_params: dict[str, Any],
) -> None:
    client, mock_http = benchmark_client
    mock_resp = _mock_response(json_data={"task_ids": ["a"]})
    mock_http.get = AsyncMock(return_value=mock_resp)

    await client.verify_task_ids(task_ids, slice_str)

    mock_http.get.assert_called_once_with(f"{BASE_URL}/verify-task-ids", params=expected_params)


async def test_verify_task_ids_with_dataset(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
) -> None:
    client, mock_http = benchmark_client
    mock_resp = _mock_response(json_data={"task_ids": ["a"]})
    mock_http.get = AsyncMock(return_value=mock_resp)

    await client.verify_task_ids(["a"], None, dataset="mydata")

    mock_http.get.assert_called_once_with(
        f"{BASE_URL}/verify-task-ids", params={"task_ids": ["a"], "dataset": "mydata"}
    )


async def test_retrieve_task_with_dataset(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
) -> None:
    client, mock_http = benchmark_client
    mock_resp = _mock_response(
        json_data={
            "source": {"type": "image", "image": "python:3.12"},
            "problem_path": "/tmp/problem_statement.txt",
            "cwd": "/work",
            "resources": {"vcpu": 2, "memory": 4, "disk": 10},
        }
    )
    mock_http.get = AsyncMock(return_value=mock_resp)

    await client.retrieve_task("task-1", dataset="mydata")

    mock_http.get.assert_called_once_with(
        f"{BASE_URL}/retrieve-task/", params={"task_id": "task-1", "skip_validation": False, "dataset": "mydata"}
    )


async def test_final_score_with_dataset(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
) -> None:
    client, mock_http = benchmark_client
    mock_resp = _mock_response(json_data={"tasks_evaluated": ["t1"], "final_score": 100.0, "metadata": {}})
    mock_http.post = AsyncMock(return_value=mock_resp)

    await client.final_score({"t1": {"resolved": True}}, dataset="mydata")

    mock_http.post.assert_called_once_with(
        f"{BASE_URL}/final-score/",
        json={"evaluation_results": {"t1": {"resolved": True}}, "dataset": "mydata"},
    )


async def test_resume_evaluation_with_eval_resume_state(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
) -> None:
    client, _mock_http = benchmark_client
    state = {"artifact_prefix": "s3://bucket/run"}
    sandbox_provider = DAYTONA_CONFIG
    messages = [
        json.dumps({"type": "eval_resume_state", "data": state}),
        json.dumps({"type": "result", "data": {"score": 1.0}}),
    ]
    mock_connect = _ws_mock(messages)
    on_eval_resume_state = MagicMock()

    with patch("benchmark_service.client.websockets.connect", return_value=mock_connect):
        result = await client.resume_evaluation(
            "task-1",
            eval_resume_state=state,
            dataset="mydata",
            on_eval_resume_state=on_eval_resume_state,
            sandbox_provider=sandbox_provider,
        )

    ws = mock_connect.__aenter__.return_value
    assert json.loads(ws.send.call_args.args[0]) == {
        "task_id": "task-1",
        "response": None,
        "eval_resume_state": state,
        "sandbox_provider": {
            "type": "daytona",
            "DAYTONA_API_KEY": "key",
            "DAYTONA_API_URL": "url",
            "DAYTONA_TARGET": "target",
        },
        "dataset": "mydata",
    }
    assert result == {"score": 1.0}
    on_eval_resume_state.assert_called_once_with(state)


async def test_evaluate_response_includes_optional_provider_config(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
) -> None:
    client, mock_http = benchmark_client
    mock_http.post = AsyncMock(return_value=_mock_response(json_data={"score": 1.0}))

    result = await client.evaluate_response(
        "task-1",
        "answer",
        dataset="mydata",
        sandbox_provider=ModalProviderConfig(MODAL_TOKEN_ID="id", MODAL_TOKEN_SECRET="secret"),
    )

    assert result == {"score": 1.0}
    mock_http.post.assert_called_once_with(
        f"{BASE_URL}/evaluate-response/",
        json={
            "task_id": "task-1",
            "response": "answer",
            "sandbox_provider": {"type": "modal", "MODAL_TOKEN_ID": "id", "MODAL_TOKEN_SECRET": "secret"},
            "dataset": "mydata",
        },
        timeout=10,
    )


async def test_evaluate_response_omits_unspecified_provider_config(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
) -> None:
    client, mock_http = benchmark_client
    mock_http.post = AsyncMock(return_value=_mock_response(json_data={"score": 1.0}))

    await client.evaluate_response("task-1", "answer")

    mock_http.post.assert_called_once_with(
        f"{BASE_URL}/evaluate-response/",
        json={"task_id": "task-1", "response": "answer"},
        timeout=10,
    )


async def test_websocket_request_includes_provider_config() -> None:
    """Client websocket helpers should include provider config selected by callers.

    Test cases:
    - setup_task serializes Daytona provider config.
    - evaluate_instance serializes the same provider config.
    """
    sandbox_provider = DAYTONA_CONFIG
    for method in ("setup_task", "evaluate_instance"):
        messages = [json.dumps({"type": "result", "data": {"status": "ok"}})]
        mock_connect = _ws_mock(messages)

        client = _make_client()
        with patch("benchmark_service.client.websockets.connect", return_value=mock_connect):
            await getattr(client, method)("task-1", "inst-1", sandbox_provider)

        ws = mock_connect.__aenter__.return_value
        assert json.loads(ws.send.call_args.args[0]) == {
            "task_id": "task-1",
            "instance_id": "inst-1",
            "sandbox_provider": {
                "type": "daytona",
                "DAYTONA_API_KEY": "key",
                "DAYTONA_API_URL": "url",
                "DAYTONA_TARGET": "target",
            },
            "dataset": None,
        }


def test_get_sandbox_provider_uses_each_provider_config() -> None:
    """Provider lookup should not reuse a stale provider for a different config.

    Test cases:
    - Repeated calls with the same config reuse the cached provider.
    - A later call with a different provider config creates a separate provider.
    """
    client = _make_client()
    daytona_provider = client.get_sandbox_provider(DAYTONA_CONFIG)
    same_daytona_provider = client.get_sandbox_provider(DAYTONA_CONFIG)
    modal_provider = client.get_sandbox_provider(ModalProviderConfig(MODAL_TOKEN_ID="id", MODAL_TOKEN_SECRET="secret"))

    assert same_daytona_provider is daytona_provider
    assert modal_provider is not daytona_provider


async def test_verify_task_ids_no_dataset_omitted(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
) -> None:
    """Verify dataset param is NOT included when None."""
    client, mock_http = benchmark_client
    mock_resp = _mock_response(json_data={"task_ids": ["a"]})
    mock_http.get = AsyncMock(return_value=mock_resp)

    await client.verify_task_ids(["a"], None)

    mock_http.get.assert_called_once_with(f"{BASE_URL}/verify-task-ids", params={"task_ids": ["a"]})


class _AsyncIterator:
    """Async iterator over a list of strings."""

    def __init__(self, items: list[str], terminal_error: Exception | None = None) -> None:
        self._items = iter(items)
        self._terminal_error = terminal_error
        # Post-close attributes the client reads when the stream ends without a terminal chunk.
        self.close_code = 1000
        self.close_reason = ""

    def __aiter__(self) -> "_AsyncIterator":
        return self

    async def __anext__(self) -> str:
        try:
            return next(self._items)
        except StopIteration:
            if self._terminal_error is not None:
                error = self._terminal_error
                self._terminal_error = None
                raise error
            raise StopAsyncIteration


def _ws_mock(messages: list[str], terminal_error: Exception | None = None) -> AsyncMock:
    """Create a mock websockets.connect context manager yielding messages."""
    ws = _AsyncIterator(messages, terminal_error)
    ws.send = AsyncMock()  # type: ignore[attr-defined]

    mock_connect = AsyncMock()
    mock_connect.__aenter__ = AsyncMock(return_value=ws)
    mock_connect.__aexit__ = AsyncMock(return_value=False)
    return mock_connect


def _make_client(url: str = BASE_URL) -> BenchmarkServiceClient:
    return BenchmarkServiceClient(url=url, headers=HEADERS, timeout=10)


@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("setup_task", ["task-1", "inst-1", DAYTONA_CONFIG]),
        (
            "evaluate_instance",
            ["task-1", "inst-1", DAYTONA_CONFIG],
        ),
    ],
    ids=["setup_task", "evaluate_instance"],
)
async def test_ws_result_chunk(method: str, args: list[str]) -> None:
    result_data = {"status": "ok"} if method == "setup_task" else {"score": 1.0}
    messages = [json.dumps({"type": "result", "data": result_data})]
    mock_connect = _ws_mock(messages)

    client = _make_client()
    with patch("benchmark_service.client.websockets.connect", return_value=mock_connect):
        result = await getattr(client, method)(*args)

    if method == "setup_task":
        assert result.status == "ok"
    else:
        assert result == {"score": 1.0}


@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("setup_task", ["task-1", "inst-1", DAYTONA_CONFIG]),
        (
            "evaluate_instance",
            ["task-1", "inst-1", DAYTONA_CONFIG],
        ),
    ],
    ids=["setup_task", "evaluate_instance"],
)
async def test_ws_error_chunk(method: str, args: list[str]) -> None:
    messages = [json.dumps({"type": "error", "data": "something went wrong"})]
    mock_connect = _ws_mock(messages)

    client = _make_client()
    with patch("benchmark_service.client.websockets.connect", return_value=mock_connect):
        with pytest.raises(BenchmarkServiceError, match="something went wrong"):
            await getattr(client, method)(*args)


@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("setup_task", ["task-1", "inst-1", DAYTONA_CONFIG]),
        (
            "evaluate_instance",
            ["task-1", "inst-1", DAYTONA_CONFIG],
        ),
    ],
    ids=["setup_task", "evaluate_instance"],
)
async def test_ws_message_chunks_with_callback(method: str, args: list[str]) -> None:
    result_data = {"status": "ok"} if method == "setup_task" else {"score": 1.0}
    messages = [
        json.dumps({"type": "message", "data": "step 1"}),
        json.dumps({"type": "message", "data": "step 2"}),
        json.dumps({"type": "result", "data": result_data}),
    ]
    mock_connect = _ws_mock(messages)
    on_message = MagicMock()

    client = _make_client()
    with patch("benchmark_service.client.websockets.connect", return_value=mock_connect):
        await getattr(client, method)(*args, on_message=on_message)

    assert on_message.call_count == 2
    on_message.assert_any_call("step 1")
    on_message.assert_any_call("step 2")


@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("setup_task", ["task-1", "inst-1", DAYTONA_CONFIG]),
        (
            "evaluate_instance",
            ["task-1", "inst-1", DAYTONA_CONFIG],
        ),
    ],
    ids=["setup_task", "evaluate_instance"],
)
async def test_ws_message_chunks_without_callback(method: str, args: list[str]) -> None:
    result_data = {"status": "ok"} if method == "setup_task" else {"score": 1.0}
    messages = [
        json.dumps({"type": "message", "data": "step 1"}),
        json.dumps({"type": "result", "data": result_data}),
    ]
    mock_connect = _ws_mock(messages)

    client = _make_client()
    with patch("benchmark_service.client.websockets.connect", return_value=mock_connect):
        result = await getattr(client, method)(*args)

    if method == "setup_task":
        assert result.status == "ok"
    else:
        assert result == result_data


@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("setup_task", ["task-1", "inst-1", DAYTONA_CONFIG]),
        (
            "evaluate_instance",
            ["task-1", "inst-1", DAYTONA_CONFIG],
        ),
    ],
    ids=["setup_task", "evaluate_instance"],
)
async def test_ws_connection_closed_without_result(method: str, args: list[str]) -> None:
    messages: list[str] = []  # no messages — simulates immediate close
    mock_connect = _ws_mock(messages)

    client = _make_client()
    with patch("benchmark_service.client.websockets.connect", return_value=mock_connect):
        with pytest.raises(BenchmarkServiceStreamClosedError) as exc_info:
            await getattr(client, method)(*args)

    exc = exc_info.value
    assert str(exc) == f"WebSocket closed with code 1000 after {exc.idle_s:.1f}s without an application message"


@pytest.mark.parametrize(
    ("close_frame", "expected_detail"),
    [
        (
            Close(1008, "Evaluation quota reached; retry after 2026-07-27T00:00:00Z."),
            "with code 1008: Evaluation quota reached; retry after 2026-07-27T00:00:00Z.",
        ),
        (None, "without a close frame"),
    ],
    ids=["code_and_reason", "no_close_frame"],
)
async def test_ws_close_reports_the_close_frame_detail(close_frame: Close | None, expected_detail: str) -> None:
    """A server close carrying a reason (quota rejection) and an abrupt drop render distinguishably."""
    mock_connect = _ws_mock([], ConnectionClosedError(close_frame, None))
    client = _make_client()

    with patch("benchmark_service.client.websockets.connect", return_value=mock_connect):
        with pytest.raises(BenchmarkServiceStreamClosedError) as exc_info:
            await client.evaluate_instance("task-1", "inst-1", DAYTONA_CONFIG)

    exc = exc_info.value
    assert str(exc) == f"WebSocket closed {expected_detail} after {exc.idle_s:.1f}s without an application message"


async def test_ws_connect_pings_on_the_server_cadence_without_a_pong_deadline() -> None:
    """The client pings on the server's 30s cadence but never closes a socket for a missing pong."""
    mock_connect = _ws_mock([json.dumps({"type": "result", "data": {"score": 1.0}})])
    client = _make_client()

    with patch("benchmark_service.client.websockets.connect", return_value=mock_connect) as connect:
        await client.evaluate_instance("task-1", "inst-1", DAYTONA_CONFIG)

    assert connect.call_args.kwargs["ping_interval"] == 30
    assert connect.call_args.kwargs["ping_timeout"] is None


class _SilentStream:
    """Stream that stays open and never produces an application message."""

    def __aiter__(self) -> "_SilentStream":
        return self

    async def __anext__(self) -> str:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


def _silent_ws_mock() -> AsyncMock:
    ws = _SilentStream()
    ws.send = AsyncMock()  # type: ignore[attr-defined]

    mock_connect = AsyncMock()
    mock_connect.__aenter__ = AsyncMock(return_value=ws)
    mock_connect.__aexit__ = AsyncMock(return_value=False)
    return mock_connect


@pytest.mark.parametrize(
    ("health_status", "expected_health"),
    [("ok", "service still reports healthy"), (None, "service health check also failing")],
    ids=["service_healthy", "service_unreachable"],
)
async def test_ws_idle_past_the_budget_fails_the_stream(health_status: str | None, expected_health: str) -> None:
    """A silent-but-open stream must fail instead of blocking forever, and record service liveness.

    Keepalive cannot see a peer that stops producing while the socket stays open, so without this
    budget the task stays In Progress until someone force-stops the run.
    """
    client = BenchmarkServiceClient(url=BASE_URL, headers=HEADERS, timeout=10, ws_idle_timeout_s=0.05)
    if health_status is None:
        health = AsyncMock(side_effect=httpx.ConnectError("service down"))
    else:
        health = AsyncMock(return_value=HealthCheckResponse(status=health_status))

    with (
        patch("benchmark_service.client.websockets.connect", return_value=_silent_ws_mock()),
        patch.object(client, "health_check", health),
    ):
        with pytest.raises(BenchmarkServiceStreamIdleError) as exc_info:
            await client.evaluate_instance("task-1", "inst-1", DAYTONA_CONFIG)

    exc = exc_info.value
    assert exc.idle_s >= 0.05
    assert str(exc) == f"WebSocket idle for {exc.idle_s:.1f}s without an application message ({expected_health})"


@pytest.mark.parametrize("ws_idle_timeout_s", [None, 0], ids=["none", "zero"])
async def test_ws_idle_watchdog_can_be_disabled(ws_idle_timeout_s: float | None) -> None:
    """Both documented ways to disable it keep the pre-watchdog behavior of waiting indefinitely."""
    client = BenchmarkServiceClient(url=BASE_URL, headers=HEADERS, timeout=10, ws_idle_timeout_s=ws_idle_timeout_s)

    with patch("benchmark_service.client.websockets.connect", return_value=_silent_ws_mock()):
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(client.evaluate_instance("task-1", "inst-1", DAYTONA_CONFIG), timeout=0.05)


@pytest.mark.parametrize(
    ("env_value", "expected"),
    [(None, 3600.0), ("120.5", 120.5), ("0", None), ("nonsense", 3600.0)],
    ids=["unset", "override", "disabled", "unparseable"],
)
def test_ws_idle_timeout_from_the_environment(env_value: str | None, expected: float | None) -> None:
    env = {} if env_value is None else {"BENCHMARK_SERVICE_WS_IDLE_TIMEOUT_S": env_value}
    with patch.dict(os.environ, env, clear=True):
        client = BenchmarkServiceClient(url=BASE_URL, headers=HEADERS, timeout=10)

    assert client._ws_idle_timeout_s == expected  # pyright: ignore[reportPrivateUsage]


def test_dockerfile_template_matches_the_client_keepalive_constants() -> None:
    """The uvicorn --ws-ping-* flags and the client's _SERVER_PING_* constants must not drift apart."""
    dockerfile = Path(__file__).resolve().parents[1] / "templates" / "Dockerfile"
    cmd_line = next(line for line in dockerfile.read_text().splitlines() if line.startswith("CMD "))
    cmd: list[str] = json.loads(cmd_line.removeprefix("CMD "))

    assert cmd[cmd.index("--ws-ping-interval") + 1] == str(_SERVER_PING_INTERVAL_S)
    assert cmd[cmd.index("--ws-ping-timeout") + 1] == str(_SERVER_PING_TIMEOUT_S)


async def test_ws_close_silence_is_measured_from_last_application_message() -> None:
    """idle_s counts from the last application message, not from connection establishment."""
    close_frame = Close(1011, "keepalive ping timeout")
    mock_connect = _ws_mock(
        [json.dumps({"type": "message", "data": "still grading"})],
        ConnectionClosedError(close_frame, None),
    )
    client = _make_client()
    # The client stamps time.monotonic() three times on this path: connection establishment,
    # the application message, and the close.
    fake_time = MagicMock()
    fake_time.monotonic.side_effect = [100.0, 250.0, 251.5]

    with patch("benchmark_service.client.websockets.connect", return_value=mock_connect):
        with patch("benchmark_service.client.time", fake_time):
            with pytest.raises(BenchmarkServiceStreamClosedError) as exc_info:
                await client.evaluate_instance("task-1", "inst-1", DAYTONA_CONFIG)

    assert exc_info.value.close_code == 1011
    assert exc_info.value.close_reason == "keepalive ping timeout"
    # 251.5 - 250.0, from the message; measured from connection establishment it would be 151.5.
    assert exc_info.value.idle_s == 1.5


async def test_ws_preconnection_failure_propagates_as_transport_error() -> None:
    """DNS/connect/handshake failures stay distinguishable: they are never wrapped in a service error."""
    client = _make_client()

    with patch("benchmark_service.client.websockets.connect", side_effect=socket.gaierror("Name or service not known")):
        with pytest.raises(socket.gaierror):
            await client.evaluate_instance("task-1", "inst-1", DAYTONA_CONFIG)


async def test_client_list_tasks_returns_v1_dataset_tasks_response(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
) -> None:
    """client.list_tasks hits /v1/datasets/{dataset}/tasks and returns V1DatasetTasksResponse."""
    client, mock_http = benchmark_client
    mock_resp = _mock_response(
        json_data={
            "dataset": "default",
            "tasks": [
                {"id": "task-1", "question": "What is 1+1?"},
                {"id": "task-2", "question": "What is 2+2?"},
                {"id": "task-3", "question": "What is 3+3?"},
            ],
        }
    )
    mock_http.get = AsyncMock(return_value=mock_resp)

    result = await client.list_tasks(dataset="default")

    assert isinstance(result, V1DatasetTasksResponse)
    assert result.dataset == "default"
    assert {t.id for t in result.tasks} == {"task-1", "task-2", "task-3"}
    mock_http.get.assert_called_once_with(f"{BASE_URL}/v1/datasets/default/tasks")


async def test_client_v1_evaluate_posts_payload_and_returns_v1_eval_response(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
) -> None:
    client, mock_http = benchmark_client
    mock_resp = _mock_response(
        json_data={
            "run_id": "run-1",
            "task_id": "task-1",
            "status": "evaluated",
            "evaluator_version": "stub-1.0",
            "result": {"resolved": True},
            "errors": [],
        }
    )
    mock_http.post = AsyncMock(return_value=mock_resp)

    result = await client.v1_evaluate(
        run_id="run-1",
        task_id="task-1",
        payload_data="abc-key",
        payload_schema="code-migration.workspace-tar.v1",
        payload_type=V1PayloadType.ARTIFACT,
        dataset="validation",
        versions=V1Versions(runner="benchmark-orchestrator-1.2.3"),
    )

    assert isinstance(result, V1EvalResponse)
    assert result.status == "evaluated"
    assert result.result == {"resolved": True}
    called_url, called_kwargs = mock_http.post.call_args
    assert called_url[0] == f"{BASE_URL}/v1/evaluate"
    assert called_kwargs["json"]["payload"] == {
        "type": "artifact",
        "schema": "code-migration.workspace-tar.v1",
        "data": "abc-key",
    }
    assert called_kwargs["json"]["dataset"] == "validation"
    assert called_kwargs["json"]["versions"] == {"runner": "benchmark-orchestrator-1.2.3"}


async def test_client_v1_upload_url_posts_artifact_identity(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
) -> None:
    client, mock_http = benchmark_client
    mock_http.post = AsyncMock(
        return_value=_mock_response(
            json_data={
                "key": "submission-artifacts/acme/validation/run-1/task-1/submission.xlsx",
                "url": "https://uploads.example/presigned",
                "expires_in": 900,
            }
        )
    )

    result = await client.v1_upload_url(
        run_id="run-1",
        task_id="task-1",
        filename="submission.xlsx",
        dataset="validation",
    )

    assert isinstance(result, V1UploadUrlResponse)
    assert result.expires_in == 900
    mock_http.post.assert_awaited_once_with(
        f"{BASE_URL}/v1/submissions/upload-url",
        json={
            "run_id": "run-1",
            "task_id": "task-1",
            "dataset": "validation",
            "filename": "submission.xlsx",
        },
    )


async def test_client_v1_score_posts_results_and_returns_v1_score_response(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
) -> None:
    client, mock_http = benchmark_client
    mock_resp = _mock_response(
        json_data={"run_id": "run-1", "tasks_evaluated": ["task-1"], "final_score": 100.0, "metadata": {}}
    )
    mock_http.post = AsyncMock(return_value=mock_resp)

    result = await client.v1_score(
        run_id="run-1",
        evaluation_results={
            "task-1": V1ScoreItem(status=V1EvalStatus.EVALUATED, result={"resolved": True}, errors=[]),
            "task-2": None,
        },
        dataset="validation",
    )

    assert isinstance(result, V1ScoreResponse)
    assert result.final_score == 100.0
    called_url, called_kwargs = mock_http.post.call_args
    assert called_url[0] == f"{BASE_URL}/v1/score"
    assert called_kwargs["json"]["evaluation_results"]["task-2"] is None


async def test_client_v1_evaluate_raises_on_error_status(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
) -> None:
    client, mock_http = benchmark_client
    mock_http.post = AsyncMock(return_value=_mock_response(status_code=500))

    with pytest.raises(BenchmarkServiceError, match="v1 evaluate failed") as exc_info:
        await client.v1_evaluate(
            run_id="run-1",
            task_id="task-1",
            payload_data="x",
            payload_schema="s.text.v1",
            payload_type=V1PayloadType.TEXT,
        )

    assert exc_info.value.status_code == 500


def test_non_http_service_error_has_no_status_code() -> None:
    assert BenchmarkServiceError("websocket failed").status_code is None


@pytest.mark.parametrize(
    ("method", "kwargs"),
    [
        (
            "v1_evaluate",
            {
                "run_id": "run-1",
                "task_id": "task-1",
                "payload_data": "answer",
                "payload_schema": "s.text.v1",
                "payload_type": V1PayloadType.TEXT,
            },
        ),
        (
            "v1_score",
            {
                "run_id": "run-1",
                "evaluation_results": {},
            },
        ),
        (
            "v1_upload_url",
            {
                "run_id": "run-1",
                "task_id": "task-1",
                "filename": "submission.xlsx",
            },
        ),
    ],
)
async def test_client_v1_mutations_do_not_retry_after_response_transport_failure(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
    method: str,
    kwargs: dict[str, Any],
) -> None:
    client, mock_http = benchmark_client
    mock_http.post = AsyncMock(
        side_effect=httpx.ReadTimeout(
            "response connection closed",
            request=httpx.Request("POST", f"{BASE_URL}/v1"),
        )
    )

    with pytest.raises(httpx.ReadTimeout):
        await getattr(client, method)(**kwargs)

    mock_http.post.assert_awaited_once()
