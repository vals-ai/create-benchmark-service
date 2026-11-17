from __future__ import annotations

import base64
import json
from collections.abc import AsyncGenerator, Mapping
from urllib.parse import quote

import httpx

from benchmark_service.sandbox.ecs_nitro.agent import TIMEOUT_EXIT_CODE
from benchmark_service.sandbox.types import SandboxConnectionError, SandboxError, SandboxNotFoundError

_COMMAND_GRACE_SECONDS = 30.0


class AgentClient:
    def __init__(self, address: str, port: int, token: str) -> None:
        self._base = f"http://{address}:{port}"
        self._headers = {"Authorization": f"Bearer {token}"}

    def _client(self, timeout: float | None) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=self._base,
            headers=self._headers,
            timeout=httpx.Timeout(connect=10.0, read=timeout, write=60.0, pool=10.0),
        )

    @staticmethod
    def _error(response: httpx.Response, action: str) -> SandboxError:
        try:
            detail = str(response.json().get("error"))
        except ValueError:
            detail = response.text[:500]
        if response.status_code == 404 and not action.startswith(("upload", "download")):
            return SandboxNotFoundError(f"{action}: {detail}")
        return SandboxError(f"{action} failed with HTTP {response.status_code}: {detail}")

    async def healthy(self) -> bool:
        try:
            async with self._client(5.0) as client:
                return (await client.get("/health")).status_code == 200
        except httpx.HTTPError:
            return False

    async def run(
        self, command: str, cwd: str | None, timeout: float | None, env: Mapping[str, str]
    ) -> AsyncGenerator[bytes | int, None]:
        body = {"command": command, "cwd": cwd, "env": dict(env), "timeout": timeout}
        read_timeout = None if timeout is None else timeout + _COMMAND_GRACE_SECONDS
        try:
            async with self._client(read_timeout) as client, client.stream("POST", "/exec", json=body) as response:
                if response.status_code != 200:
                    await response.aread()
                    raise self._error(response, "exec")
                async for line in response.aiter_lines():
                    if not line:
                        continue
                    frame = json.loads(line)
                    if "output" in frame:
                        yield base64.b64decode(frame["output"])
                    elif "exit_code" in frame:
                        yield int(frame["exit_code"])
                        return
                    else:
                        raise SandboxError(f"exec failed: {frame.get('error')}")
        except httpx.ReadTimeout:
            yield TIMEOUT_EXIT_CODE
            return
        except httpx.HTTPError as error:
            raise SandboxConnectionError(f"sandbox agent at {self._base} unreachable: {error}") from error
        raise SandboxConnectionError("sandbox agent closed the exec stream before reporting an exit code")

    async def upload(self, remote_path: str, content: bytes) -> None:
        try:
            async with self._client(300.0) as client:
                response = await client.put(f"/files?path={quote(remote_path)}", content=content)
        except httpx.HTTPError as error:
            raise SandboxConnectionError(f"upload to {remote_path} failed: {error}") from error
        if response.status_code != 200:
            raise self._error(response, f"upload {remote_path}")

    async def download(self, remote_path: str) -> AsyncGenerator[bytes, None]:
        try:
            async with (
                self._client(300.0) as client,
                client.stream("GET", f"/files?path={quote(remote_path)}") as response,
            ):
                if response.status_code != 200:
                    await response.aread()
                    raise self._error(response, f"download {remote_path}")
                async for chunk in response.aiter_bytes():
                    yield chunk
        except httpx.HTTPError as error:
            raise SandboxConnectionError(f"download of {remote_path} failed: {error}") from error
