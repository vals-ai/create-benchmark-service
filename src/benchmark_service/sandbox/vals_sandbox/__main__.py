"""Try Vals Sandbox from a shell.

    python -m benchmark_service.sandbox.vals_sandbox build-image
    python -m benchmark_service.sandbox.vals_sandbox up --image <arn>
    python -m benchmark_service.sandbox.vals_sandbox exec <id> -- python3 -c 'print(1)'
    python -m benchmark_service.sandbox.vals_sandbox down <id>

Configuration comes from the VALS_SANDBOX_* environment variables and the AWS SDK's usual credentials.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import click

from benchmark_service.sandbox.vals_sandbox import ValsSandboxProviderConfig, ValsSandboxProvider
from benchmark_service.sandbox.vals_sandbox.image import (
    DEFAULT_BASE_DOCKERFILE,
    aws_client,
    baseline_memory_mib,
    ensure_image,
)
from benchmark_service.sandbox.types import (
    DockerfileSource,
    ImageSource,
    Resources,
    SandboxCommandError,
    SandboxCreateRequest,
    SandboxQuery,
    SandboxSource,
)


def _run[T](work: Callable[[ValsSandboxProvider], Awaitable[T]]) -> T:
    async def main() -> T:
        provider = ValsSandboxProvider(ValsSandboxProviderConfig.from_env())
        async with provider:
            return await work(provider)

    return asyncio.run(main())


@click.group()
def cli() -> None:
    """Spin up, use, and tear down Vals Sandbox sandboxes."""


@cli.command("build-image")
@click.option("--dockerfile", type=click.Path(exists=True, dir_okay=False, path_type=Path), default=None)
@click.option("--vcpu", type=float, default=2.0, show_default=True, help="Peak vCPU the image must reach")
@click.option("--memory", type=int, default=4, show_default=True, help="Peak memory in GiB")
def build_image(dockerfile: Path | None, vcpu: float, memory: int) -> None:
    """Build (or reuse) a MicroVM image and print its ARN; defaults to Amazon Linux 2023 with Python."""
    config = ValsSandboxProviderConfig.from_env()
    if not (config.VALS_SANDBOX_BUILD_BUCKET and config.VALS_SANDBOX_BUILD_ROLE_ARN):
        raise click.UsageError("Set VALS_SANDBOX_BUILD_BUCKET and VALS_SANDBOX_BUILD_ROLE_ARN")
    region = config.VALS_SANDBOX_REGION
    arn = ensure_image(
        aws_client("lambda-microvms", region),
        aws_client("s3", region),
        dockerfile=dockerfile.read_text() if dockerfile else DEFAULT_BASE_DOCKERFILE,
        context_dir=dockerfile.parent if dockerfile else None,
        memory_mib=baseline_memory_mib(vcpu, memory),
        bucket=config.VALS_SANDBOX_BUILD_BUCKET,
        build_role_arn=config.VALS_SANDBOX_BUILD_ROLE_ARN,
        architecture=config.VALS_SANDBOX_ARCHITECTURE,
        log=lambda message: click.echo(message, err=True),
    )
    click.echo(arn)


@cli.command()
@click.option("--image", default=None, help="MicroVM image ARN, container image, or omit for VALS_SANDBOX_IMAGE")
@click.option("--dockerfile", type=click.Path(exists=True, dir_okay=False, resolve_path=True), default=None)
@click.option("--label", "labels", multiple=True, help="key=value, repeatable")
@click.option("--env", "env_vars", multiple=True, help="KEY=VALUE, repeatable")
@click.option("--vcpu", type=float, default=2.0, show_default=True)
@click.option("--memory", type=int, default=4, show_default=True, help="GiB")
@click.option("--idle-minutes", type=int, default=15, show_default=True, help="Terminate after this long idle")
@click.option("--no-internet", is_flag=True, help="Start without internet egress")
def up(
    image: str | None,
    dockerfile: str | None,
    labels: tuple[str, ...],
    env_vars: tuple[str, ...],
    vcpu: float,
    memory: int,
    idle_minutes: int,
    no_internet: bool,
) -> None:
    """Start a sandbox and print its id."""
    config = ValsSandboxProviderConfig.from_env()
    source: SandboxSource
    if dockerfile:
        source = DockerfileSource(dockerfile=dockerfile)
    elif image or config.VALS_SANDBOX_IMAGE:
        source = ImageSource(image=image or config.VALS_SANDBOX_IMAGE or "")
    else:
        raise click.UsageError("Pass --image or --dockerfile, or set VALS_SANDBOX_IMAGE")
    request = SandboxCreateRequest(
        source=source,
        resources=Resources(vcpu=vcpu, memory=memory, disk=8),
        name=f"cli-{Path.cwd().name}",
        labels={"created-by": "lambda-microvm-cli", **_pairs(labels)},
        env_vars=_pairs(env_vars),
        auto_stop_interval=idle_minutes,
        create_timeout=300,
        network_block_all=no_internet,
    )

    async def work(provider: ValsSandboxProvider) -> str:
        return (await provider.create_sandbox(request)).id

    click.echo(_run(work))


@cli.command("exec", context_settings={"ignore_unknown_options": True})
@click.argument("sandbox_id")
@click.argument("command", nargs=-1, required=True, type=click.UNPROCESSED)
@click.option("--cwd", default=None)
@click.option("--timeout", type=float, default=None)
def exec_command(sandbox_id: str, command: tuple[str, ...], cwd: str | None, timeout: float | None) -> None:
    """Run a shell command and stream its output; exits with the command's exit code."""

    async def work(provider: ValsSandboxProvider) -> int:
        sandbox = await provider.get_sandbox(sandbox_id)
        try:
            async for chunk in sandbox.command(" ".join(command), cwd=cwd, timeout=timeout):
                sys.stdout.write(chunk)
                sys.stdout.flush()
        except SandboxCommandError as error:
            return error.exit_code
        return 0

    sys.exit(_run(work))


@cli.command()
@click.argument("sandbox_id")
@click.argument("local", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.argument("remote")
def upload(sandbox_id: str, local: Path, remote: str) -> None:
    """Copy a local file into the sandbox."""

    async def work(provider: ValsSandboxProvider) -> None:
        await (await provider.get_sandbox(sandbox_id)).upload_file(remote, local.read_bytes())

    _run(work)


@cli.command()
@click.argument("sandbox_id")
@click.argument("remote")
@click.argument("local", type=click.Path(dir_okay=False, path_type=Path))
def download(sandbox_id: str, remote: str, local: Path) -> None:
    """Copy a file out of the sandbox."""

    async def work(provider: ValsSandboxProvider) -> None:
        local.write_bytes(await (await provider.get_sandbox(sandbox_id)).download_file(remote))

    _run(work)


@cli.command()
@click.argument("sandbox_id")
def down(sandbox_id: str) -> None:
    """Terminate the sandbox."""

    async def work(provider: ValsSandboxProvider) -> str:
        await provider.delete_sandbox(sandbox_id)
        return (await provider.get_sandbox(sandbox_id)).state

    click.echo(_run(work))


@cli.command("ls")
@click.option("--label", "labels", multiple=True, help="key=value filter, repeatable")
def list_command(labels: tuple[str, ...]) -> None:
    """List live sandboxes."""

    async def work(provider: ValsSandboxProvider) -> list[dict[str, Any]]:
        return [
            {"id": sandbox.id, "name": sandbox.name, "state": sandbox.state, "labels": dict(sandbox.labels or {})}
            async for sandbox in provider.list_sandboxes(SandboxQuery(labels=_pairs(labels), page_size=50))
        ]

    for row in _run(work):
        click.echo(f"{row['id']}\t{row['state']}\t{row['name']}\t{row['labels']}")


def _pairs(values: tuple[str, ...]) -> dict[str, str]:
    pairs: dict[str, str] = {}
    for value in values:
        key, sep, rest = value.partition("=")
        if not sep:
            raise click.BadParameter(f"expected key=value, got {value!r}")
        pairs[key] = rest
    return pairs


if __name__ == "__main__":
    cli()
