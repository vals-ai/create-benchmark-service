from __future__ import annotations

import asyncio
import shlex
import uuid
from collections.abc import AsyncGenerator, Iterable, Mapping

from benchmark_service.sandbox._process_group import (
    cleanup_command, episode_owner_command, owner_command, probe_command, stop_command,
)
from benchmark_service.sandbox.types import (
    ComposeSource,
    ControlledWorkload,
    ControlledWorkloadResult,
    ExecResult,
    GenerationContainment,
    LINUX_PROCESS_GROUP_V1,
    Sandbox,
    SandboxError,
    validate_command_env,
)

# Outer docker-in-docker shell identity; never forwarded into service containers.
_OUTER_SHELL_ENV = (
    "HOME",
    "PATH",
    "HOSTNAME",
    "PWD",
    "OLDPWD",
    "SHLVL",
    "SHELL",
    "TERM",
    "USER",
    "LOGNAME",
    "_",
    "DOCKER_[A-Za-z0-9_]*",
    "DIND_[A-Za-z0-9_]*",
)
_COMPOSE_EXEC_ENV_ARGS = (
    "$(env | sed -En '/^(" + "|".join(_OUTER_SHELL_ENV) + ")=/d; s/^([A-Za-z_][A-Za-z0-9_]*)=.*/-e \\1/p')"
)




class ComposeSandbox(Sandbox):
    def __init__(self, outer: Sandbox, source: ComposeSource) -> None:
        self._outer = outer
        self._service = source.service
        self._compose_command_prefix = source.compose_command
        self.labels = outer.labels
        self.created_at = outer.created_at

    @property
    def id(self) -> str:
        return self._outer.id

    @property
    def name(self) -> str:
        return self._outer.name

    @property
    def state(self) -> str:
        return self._outer.state

    @property
    def provider_metadata(self) -> Mapping[str, str]:
        return self._outer.provider_metadata

    @property
    def generation_containment(self) -> GenerationContainment:
        return LINUX_PROCESS_GROUP_V1

    async def probe_generation_containment(self) -> None:
        await self._outer.probe_generation_containment()
        marker = f"/tmp/.cbs-probe-{uuid.uuid4().hex}"
        result = await self.exec(probe_command(marker))
        if result.exit_code != 0:
            raise SandboxError(f"Compose service does not support process groups: {result.output}")

    def _process_group_workload(
        self, command: str, episode_script: str | None, *, cwd: str | None,
        env_vars: Mapping[str, str] | None
    ) -> ControlledWorkload:
        return _ComposeControlledWorkload(self, command, cwd, validate_command_env(env_vars), episode_script)

    def _compose_command(self, parts: list[str]) -> str:
        return f"{self._compose_command_prefix} {shlex.join(parts)}"

    def _compose_exec_command(self, parts: list[str]) -> str:
        return f"{self._compose_command_prefix} exec {_COMPOSE_EXEC_ENV_ARGS} {shlex.join(parts)}"

    def _exec_command(self, command: str, cwd: str | None, env_names: Iterable[str] = ()) -> str:
        parts = ["-T"]
        for name in env_names:
            parts.extend(["-e", name])
        if cwd:
            parts.extend(["-w", cwd])
        parts.extend([self._service, "sh", "-lc", command])
        return self._compose_exec_command(parts)

    def _container_lookup(self) -> str:
        return f"container_id=$({self._compose_command(['ps', '-q', self._service])})"

    def _temp_path(self, prefix: str, remote_path: str) -> str:
        name = remote_path.rstrip("/").rsplit("/", 1)[-1] or "file"
        return f"/var/tmp/{prefix}-{uuid.uuid4().hex}-{name}"

    async def exec(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> ExecResult:
        return await self._outer.exec(self._exec_command(command, cwd), timeout=timeout)

    async def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        env = validate_command_env(env_vars) if env_vars is not None else None
        inner = self._exec_command(command, cwd, env or ())
        async for chunk in self._outer.command(inner, timeout=timeout, env_vars=env):
            yield chunk

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        temp = self._temp_path("compose-upload", remote_path)
        try:
            await self._outer.upload_file(temp, content)
            parent = remote_path.rstrip("/").rsplit("/", 1)[0] or "."
            make_parent = shlex.quote(f"mkdir -p {shlex.quote(parent)}")
            write_file = shlex.quote(f"cat > {shlex.quote(remote_path)}")
            result = await self._outer.exec(
                (
                    f"{self._container_lookup()}; "
                    f"docker exec \"$container_id\" sh -lc {make_parent}; "
                    f"cat {shlex.quote(temp)} | docker exec -i \"$container_id\" sh -lc {write_file}"
                ),
                timeout=60,
            )
            if result.exit_code != 0:
                raise SandboxError(f"compose upload failed: {result.output}")
        finally:
            await self._outer.exec(shlex.join(["rm", "-f", temp]), timeout=10)

    async def download_file(self, remote_path: str) -> bytes:
        temp = self._temp_path("compose-download", remote_path)
        try:
            read_file = shlex.quote(f"cat {shlex.quote(remote_path)}")
            result = await self._outer.exec(
                (
                    f"{self._container_lookup()}; "
                    f"docker exec \"$container_id\" sh -lc {read_file} > {shlex.quote(temp)}"
                ),
                timeout=60,
            )
            if result.exit_code != 0:
                raise SandboxError(f"compose download failed: {result.output}")
            return await self._outer.download_file(temp)
        finally:
            await self._outer.exec(shlex.join(["rm", "-f", temp]), timeout=10)

    async def stream_download(self, remote_path: str) -> AsyncGenerator[bytes, None]:
        temp = self._temp_path("compose-download", remote_path)
        try:
            read_file = shlex.quote(f"cat {shlex.quote(remote_path)}")
            result = await self._outer.exec(
                (
                    f"{self._container_lookup()}; "
                    f"docker exec \"$container_id\" sh -lc {read_file} > {shlex.quote(temp)}"
                ),
                timeout=60,
            )
            if result.exit_code != 0:
                raise SandboxError(f"compose download failed: {result.output}")
            async for chunk in self._outer.stream_download(temp):
                yield chunk
        finally:
            await self._outer.exec(shlex.join(["rm", "-f", temp]), timeout=10)

    async def modify_egress_rules(self, allowed_addresses: list[str]) -> None:
        await self._outer.modify_egress_rules(allowed_addresses)

    async def block_all_egress(self) -> None:
        await self._outer.block_all_egress()

    async def clear_egress_rules(self) -> None:
        await self._outer.clear_egress_rules()


class _ComposeControlledWorkload(ControlledWorkload):
    def __init__(
        self,
        sandbox: ComposeSandbox,
        command: str,
        cwd: str | None,
        env_vars: dict[str, str],
        episode_script: str | None,
    ) -> None:
        self._sandbox = sandbox
        self._command = command
        self._cwd = cwd
        self._env_vars = env_vars
        self._episode_script = episode_script
        self._marker = f"/tmp/.cbs-controlled-{uuid.uuid4().hex}"
        self._launch_task = asyncio.create_task(self._launch())
        self._result_task: asyncio.Task[ControlledWorkloadResult] | None = None
        self._stop_task: asyncio.Task[tuple[float, int | None]] | None = None
        self._cleanup_task: asyncio.Task[None] | None = None

    async def _launch(self) -> tuple[ControlledWorkload, str]:
        sandbox = self._sandbox
        lookup = await sandbox._outer.exec(  # pyright: ignore[reportPrivateUsage]
            sandbox._compose_command(["ps", "-q", sandbox._service]),  # pyright: ignore[reportPrivateUsage]
            timeout=10,
        )
        container_id = lookup.output.strip()
        if lookup.exit_code != 0 or not container_id:
            raise SandboxError(f"Compose service container lookup failed: {lookup.output}")

        # The inner owner stops its group after foreground exit, before inherited
        # stdout can hold docker exec open.
        inner = (
            episode_owner_command(self._command, self._marker, self._episode_script)
            if self._episode_script is not None
            else owner_command(self._command, self._marker, "sh -lc")
        )
        args: list[str] = []
        for name in self._env_vars:
            args.extend(["-e", name])
        if self._cwd:
            args.extend(["-w", self._cwd])
        docker_command = (
            f"docker exec {_COMPOSE_EXEC_ENV_ARGS} {shlex.join(args)} "
            f"{shlex.quote(container_id)} sh -lc {shlex.quote(inner)}"
        )
        workload = sandbox._outer.controlled_workload(  # pyright: ignore[reportPrivateUsage]
            docker_command, env_vars=self._env_vars
        )
        self._result_task = asyncio.create_task(workload.wait())
        return workload, container_id

    async def output(self) -> AsyncGenerator[str, None]:
        workload, _ = await asyncio.shield(self._launch_task)
        async for chunk in workload.output():
            yield chunk

    async def wait(self) -> ControlledWorkloadResult:
        await asyncio.shield(self._launch_task)
        assert self._result_task is not None
        outer_result = await asyncio.shield(self._result_task)
        stopped_at, status = await asyncio.shield(self._ensure_stopped())
        assert self._cleanup_task is not None
        await asyncio.shield(self._cleanup_task)
        if status is None and outer_result.result.exit_code == 0:
            raise SandboxError("Compose workload exited without a foreground status")
        result = ExecResult(
            exit_code=status if status is not None else outer_result.result.exit_code,
            output=outer_result.result.output,
        )
        return ControlledWorkloadResult(result, stopped_at)

    async def kill(self) -> None:
        workload, _ = await asyncio.shield(self._launch_task)
        await asyncio.shield(self._ensure_stopped())
        await workload.kill()

    async def _ensure_stopped(self) -> tuple[float, int | None]:
        if self._stop_task is None:
            self._stop_task = asyncio.create_task(self._stop())
        return await self._stop_task

    async def _stop(self) -> tuple[float, int | None]:
        _, container_id = await self._launch_task
        outer = self._sandbox._outer  # pyright: ignore[reportPrivateUsage]
        assert self._result_task is not None
        command = stop_command(self._marker, episode_owner=self._episode_script is not None)
        while True:
            stopped = await outer.exec(
                f"docker exec {shlex.quote(container_id)} sh -c {shlex.quote(command)}",
                timeout=10,
            )
            if stopped.exit_code == 0:
                break
            if stopped.exit_code != 75:
                raise SandboxError(f"Compose process group did not stop: {stopped.output}")
            if self._result_task.done():
                await self._result_task
                raise SandboxError("Compose workload finished without a service process-group marker")
            await asyncio.sleep(0.05)
        self._cleanup_task = asyncio.create_task(self._cleanup(container_id))
        return asyncio.get_running_loop().time(), int(stopped.output.strip()) if stopped.output.strip() else None

    async def _cleanup(self, container_id: str) -> None:
        assert self._result_task is not None
        await asyncio.gather(self._result_task, return_exceptions=True)
        outer = self._sandbox._outer  # pyright: ignore[reportPrivateUsage]
        removed = await outer.exec(
            f"docker exec {shlex.quote(container_id)} sh -c {shlex.quote(cleanup_command(self._marker))}",
            timeout=10,
        )
        if removed.exit_code != 0:
            raise SandboxError(f"Compose process-group control cleanup failed: {removed.output}")
