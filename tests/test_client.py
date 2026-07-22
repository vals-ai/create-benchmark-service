"""Tests for BenchmarkServiceClient."""

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError
from benchmark_service.sandbox.daytona import DaytonaProviderConfig
from benchmark_service.sandbox.modal import ModalProviderConfig
from benchmark_service.v1_schemas import V1DatasetTasksResponse

BASE_URL = "http://localhost:8000"
HEADERS = {"Authorization": "Bearer token"}
DAYTONA_CONFIG = DaytonaProviderConfig(DAYTONA_API_KEY="key", DAYTONA_API_URL="url", DAYTONA_TARGET="target")


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

    with pytest.raises(BenchmarkServiceError):
        await getattr(client, method)(*args)


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

    def __init__(self, items: list[str]) -> None:
        self._items = iter(items)

    def __aiter__(self) -> "_AsyncIterator":
        return self

    async def __anext__(self) -> str:
        try:
            return next(self._items)
        except StopIteration:
            raise StopAsyncIteration


def _ws_mock(messages: list[str]) -> AsyncMock:
    """Create a mock websockets.connect context manager yielding messages."""
    ws = _AsyncIterator(messages)
    ws.send = AsyncMock()  # type: ignore[attr-defined]

    mock_connect = AsyncMock()
    mock_connect.__aenter__ = AsyncMock(return_value=ws)
    mock_connect.__aexit__ = AsyncMock(return_value=False)
    return mock_connect


def _make_client(url: str = BASE_URL) -> BenchmarkServiceClient:
    return BenchmarkServiceClient(url=url, headers=HEADERS, timeout=10)


@pytest.mark.parametrize(
    ("configured_max_size", "expected_max_size"),
    [
        (None, 10 * 1024 * 1024),
        (12 * 1024 * 1024, 12 * 1024 * 1024),
    ],
    ids=["default", "override"],
)
async def test_websocket_message_size_limit_reaches_connection(
    configured_max_size: int | None, expected_max_size: int
) -> None:
    mock_connect = _ws_mock([json.dumps({"type": "result", "data": {"status": "ok"}})])
    client = (
        _make_client()
        if configured_max_size is None
        else BenchmarkServiceClient(
            url=BASE_URL,
            headers=HEADERS,
            timeout=10,
            max_websocket_message_size=configured_max_size,
        )
    )

    with patch("benchmark_service.client.websockets.connect", return_value=mock_connect) as connect:
        await client.setup_task("task-1", "inst-1", DAYTONA_CONFIG)

    assert connect.call_args.kwargs["max_size"] == expected_max_size


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
        with pytest.raises(BenchmarkServiceError, match="without returning final result"):
            await getattr(client, method)(*args)


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
