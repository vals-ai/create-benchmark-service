"""Tests for BenchmarkServiceClient."""

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError

BASE_URL = "http://localhost:8000"
HEADERS = {"Authorization": "Bearer token"}


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
                "docker_image": "python:3.12",
                "problem_path": "/tmp/problem_statement.txt",
                "cwd": "/work",
                "resources": {"vcpu": 2, "memory": 4, "disk": 10},
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
    ids=["health_check", "verify_task_ids", "retrieve_task", "final_score"],
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
            "docker_image": "python:3.12",
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


async def test_evaluate_response_with_eval_resume_state(
    benchmark_client: tuple[BenchmarkServiceClient, AsyncMock],
) -> None:
    client, mock_http = benchmark_client
    state = {"artifact_prefix": "s3://bucket/run"}
    mock_resp = _mock_response(json_data={"score": 1.0})
    mock_http.post = AsyncMock(return_value=mock_resp)

    result = await client.evaluate_response("task-1", eval_resume_state=state, dataset="mydata")

    mock_http.post.assert_called_once_with(
        f"{BASE_URL}/evaluate-response/",
        json={"task_id": "task-1", "eval_resume_state": state, "dataset": "mydata"},
        timeout=None,
    )
    assert result == {"score": 1.0}


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
    ("method", "args"),
    [
        ("setup_task", ["task-1", "inst-1"]),
        ("evaluate_instance", ["task-1", "inst-1"]),
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
        ("setup_task", ["task-1", "inst-1"]),
        ("evaluate_instance", ["task-1", "inst-1"]),
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
        ("setup_task", ["task-1", "inst-1"]),
        ("evaluate_instance", ["task-1", "inst-1"]),
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
        ("setup_task", ["task-1", "inst-1"]),
        ("evaluate_instance", ["task-1", "inst-1"]),
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
        ("setup_task", ["task-1", "inst-1"]),
        ("evaluate_instance", ["task-1", "inst-1"]),
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


async def test_evaluate_instance_keeps_legacy_live_payload() -> None:
    mock_connect = _ws_mock([json.dumps({"type": "result", "data": {"score": 1.0}})])

    client = _make_client()
    with patch("benchmark_service.client.websockets.connect", return_value=mock_connect):
        await client.evaluate_instance("task-1", "inst-1")

    ws = mock_connect.__aenter__.return_value
    assert json.loads(ws.send.call_args.args[0]) == {"task_id": "task-1", "instance_id": "inst-1"}


async def test_evaluate_instance_preserves_positional_dataset() -> None:
    mock_connect = _ws_mock([json.dumps({"type": "result", "data": {"score": 1.0}})])

    client = _make_client()
    with patch("benchmark_service.client.websockets.connect", return_value=mock_connect):
        await client.evaluate_instance("task-1", "inst-1", None, "alt")

    ws = mock_connect.__aenter__.return_value
    assert json.loads(ws.send.call_args.args[0]) == {"task_id": "task-1", "instance_id": "inst-1", "dataset": "alt"}

