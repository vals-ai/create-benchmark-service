"""Opt-in burst and stream soak for a private Kubernetes sandbox deployment.

Run this from the VPC path used by benchmark services, not through kubectl port-forward.
Set TEST_KUBERNETES_SCALE_TARGET=2000 only with KUBERNETES_SCALE_PROFILE=scale-2000.
"""

from __future__ import annotations

import asyncio
import math
import os
import time
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable

import pytest

from benchmark_service.sandbox.kubernetes.client import KubernetesControlClientDriver
from benchmark_service.sandbox.types import ImageSource, Resources, Sandbox, SandboxCreateRequest, SandboxQuery

_REQUIRED_ENV = (
    "TEST_KUBERNETES_CONTROL_URL",
    "TEST_KUBERNETES_CONTROL_TOKEN",
    "TEST_KUBERNETES_IMAGE",
    "TEST_KUBERNETES_SCALE_TARGET",
)


def _positive_integer(name: str, default: int | None = None) -> int:
    raw_value = os.environ.get(name)
    if raw_value is None:
        if default is None:
            raise ValueError(f"{name} is required")
        return default
    value = int(raw_value)
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


async def _in_batches[Result](
    values: list[Result],
    batch_size: int,
    operation: Callable[[Result], Awaitable[None]],
    *,
    label: str | None = None,
) -> None:
    for offset in range(0, len(values), batch_size):
        results = await asyncio.gather(
            *(operation(value) for value in values[offset : offset + batch_size]),
            return_exceptions=True,
        )
        errors = [result for result in results if isinstance(result, Exception)]
        if errors:
            raise ExceptionGroup(f"{label or 'batch'} operations failed", errors)
        if label is not None:
            completed = min(offset + batch_size, len(values))
            print(f"{label}: {completed}/{len(values)}", flush=True)


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(percentile * len(ordered)) - 1))
    return ordered[index]


async def _read_line(stream: AsyncGenerator[str, None], timeout: float) -> tuple[str, str]:
    output = ""
    while "\n" not in output:
        output += await asyncio.wait_for(anext(stream), timeout=timeout)
    line, remainder = output.split("\n", 1)
    return line, remainder


@pytest.mark.skipif(
    any(not os.environ.get(name) for name in _REQUIRED_ENV),
    reason="Kubernetes scale-test variables are not configured",
)
async def test_live_kubernetes_burst_and_active_streams() -> None:
    """Prove the configured sandbox target through the real private network path.

    Test cases:
    - Sandboxes are created in bounded waves until the requested live target is ready.
    - Every sandbox holds an HTTP command stream at the same time without Kubernetes exec.
    - A second command releases each stream, all terminal events arrive, and cleanup succeeds.
    """
    target = _positive_integer("TEST_KUBERNETES_SCALE_TARGET")
    if target > 2_000:
        raise ValueError("TEST_KUBERNETES_SCALE_TARGET must not exceed 2000")
    create_batch = _positive_integer("TEST_KUBERNETES_SCALE_CREATE_BATCH", 100)
    cleanup_batch = _positive_integer("TEST_KUBERNETES_SCALE_CLEANUP_BATCH", 200)
    command_timeout = float(os.environ.get("TEST_KUBERNETES_SCALE_COMMAND_TIMEOUT", "600"))
    hold_seconds = float(os.environ.get("TEST_KUBERNETES_SCALE_HOLD_SECONDS", "360"))
    if command_timeout <= hold_seconds or hold_seconds <= 0:
        raise ValueError("TEST_KUBERNETES_SCALE_COMMAND_TIMEOUT must exceed the positive stream hold duration")
    prefix = f"scale-{uuid.uuid4().hex[:10]}"
    driver = KubernetesControlClientDriver(
        api_url=os.environ["TEST_KUBERNETES_CONTROL_URL"],
        api_token=os.environ["TEST_KUBERNETES_CONTROL_TOKEN"],
        connect_timeout=30,
        request_timeout=900,
        max_connections=target + cleanup_batch,
        max_keepalive_connections=min(target + cleanup_batch, 512),
    )
    sandboxes: list[Sandbox] = []
    streams: list[AsyncGenerator[str, None]] = []
    stream_openers: list[asyncio.Task[None]] = []
    stream_consumers: list[asyncio.Task[None]] = []
    cleanup_errors: list[Exception] = []
    test_error: Exception | None = None
    startup_seconds: list[float] = []
    test_started = time.monotonic()

    async def create(index: int) -> None:
        started = time.monotonic()
        name = f"{prefix}-{index:04d}"
        sandbox = await driver.create_sandbox(
            SandboxCreateRequest(
                source=ImageSource(image=os.environ["TEST_KUBERNETES_IMAGE"]),
                resources=Resources(vcpu=1, memory=1, disk=5),
                name=name,
                labels={"scale_test": prefix},
                env_vars={},
                auto_stop_interval=30,
                create_timeout=600,
            )
        )
        sandboxes.append(sandbox)
        startup_seconds.append(time.monotonic() - started)

    async def finish(stream: AsyncGenerator[str, None]) -> None:
        output = "".join([chunk async for chunk in stream])
        assert output == "stream-finished\n"

    async def open_stream(sandbox: Sandbox) -> None:
        release_path = f"/workspace/{prefix}-release"
        stream = sandbox.command(
            f"printf 'stream-ready\\n'; while test ! -e {release_path}; do sleep 0.1; done; "
            "printf 'stream-finished\\n'",
            timeout=command_timeout,
        )
        streams.append(stream)
        line, remainder = await _read_line(stream, command_timeout)
        assert line == "stream-ready" and remainder == ""
        stream_consumers.append(asyncio.create_task(finish(stream)))

    async def release(sandbox: Sandbox) -> None:
        result = await sandbox.exec(f"touch /workspace/{prefix}-release", timeout=60)
        assert result.exit_code == 0, result.output

    async def delete(sandbox: Sandbox) -> None:
        await driver.delete_sandbox(sandbox.id)

    try:
        await _in_batches(list(range(target)), create_batch, create, label="sandboxes ready")
        assert len(sandboxes) == target
        creation_elapsed = time.monotonic() - test_started
        print(
            "cold start: "
            f"total={creation_elapsed:.1f}s "
            f"p50={_percentile(startup_seconds, 0.50):.1f}s "
            f"p95={_percentile(startup_seconds, 0.95):.1f}s "
            f"p99={_percentile(startup_seconds, 0.99):.1f}s",
            flush=True,
        )

        stream_openers = [asyncio.create_task(open_stream(sandbox)) for sandbox in sandboxes]
        await asyncio.gather(*stream_openers)
        assert len(streams) == target
        assert len(stream_consumers) == target
        streams_ready_elapsed = time.monotonic() - test_started
        print(f"active streams: {target}/{target} after {streams_ready_elapsed:.1f}s", flush=True)

        completed_consumers, _ = await asyncio.wait(
            stream_consumers,
            timeout=hold_seconds,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if completed_consumers:
            await asyncio.gather(*completed_consumers)
            raise AssertionError("A command stream ended before the release command")
        print(f"active streams held: {target}/{target} for {hold_seconds:.1f}s", flush=True)

        await _in_batches(sandboxes, cleanup_batch, release, label="streams released")
        await asyncio.gather(*stream_consumers)
        print(
            f"scale proof complete: target={target} total={time.monotonic() - test_started:.1f}s",
            flush=True,
        )
    except Exception as error:
        test_error = error
    finally:
        for stream_task in [*stream_openers, *stream_consumers]:
            if not stream_task.done():
                stream_task.cancel()
        await asyncio.gather(*stream_openers, *stream_consumers, return_exceptions=True)
        await asyncio.gather(*(stream.aclose() for stream in streams), return_exceptions=True)
        try:
            await _in_batches(sandboxes, cleanup_batch, delete, label="sandboxes deleted")
        except Exception as error:
            cleanup_errors.append(error)
        try:
            remaining = [
                sandbox
                async for sandbox in driver.list_sandboxes(
                    SandboxQuery(labels={"scale_test": prefix}, page_size=target)
                )
            ]
            await _in_batches(remaining, cleanup_batch, delete, label="late sandboxes deleted")
        except Exception as error:
            cleanup_errors.append(error)
        try:
            await driver.close()
        except Exception as error:
            cleanup_errors.append(error)
    if test_error is not None and cleanup_errors:
        raise ExceptionGroup("Kubernetes scale test and cleanup failed", [test_error, *cleanup_errors])
    if test_error is not None:
        raise test_error
    if cleanup_errors:
        raise ExceptionGroup("Could not clean up the Kubernetes scale test", cleanup_errors)


async def test_scale_failure_reports_cleanup_failure(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Keep a stream failure and its cleanup failure visible together.

    Test cases:
    - A failed stream remains visible when deleting its sandbox also fails.
    - The failed delete is not reported as successful cleanup.
    """

    class FailingSandbox:
        id = "sandbox-1"

        def command(self, command: str, timeout: float) -> AsyncGenerator[str, None]:
            async def fail() -> AsyncGenerator[str, None]:
                raise RuntimeError("stream failed")
                yield ""  # pragma: no cover

            return fail()

    class FailingDriver:
        async def create_sandbox(self, request: SandboxCreateRequest) -> FailingSandbox:
            return FailingSandbox()

        async def delete_sandbox(self, sandbox_id: str) -> None:
            raise RuntimeError("delete failed")

        async def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]:
            if False:
                yield Sandbox  # pragma: no cover

        async def close(self) -> None:
            return None

    def create_failing_driver(**options: object) -> FailingDriver:
        del options
        return FailingDriver()

    for name, value in {
        "TEST_KUBERNETES_CONTROL_URL": "http://control",
        "TEST_KUBERNETES_CONTROL_TOKEN": "token",
        "TEST_KUBERNETES_IMAGE": "python:3.12",
        "TEST_KUBERNETES_SCALE_TARGET": "1",
        "TEST_KUBERNETES_SCALE_CREATE_BATCH": "1",
        "TEST_KUBERNETES_SCALE_CLEANUP_BATCH": "1",
        "TEST_KUBERNETES_SCALE_COMMAND_TIMEOUT": "1",
        "TEST_KUBERNETES_SCALE_HOLD_SECONDS": "0.1",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setitem(globals(), "KubernetesControlClientDriver", create_failing_driver)

    with pytest.raises(ExceptionGroup) as error_info:
        await test_live_kubernetes_burst_and_active_streams()

    errors = error_info.value.exceptions
    assert any("stream failed" in repr(error) for error in errors)
    assert any("delete failed" in repr(error) for error in errors)
    assert "sandboxes deleted" not in capsys.readouterr().out
