"""CBS-owned whole-episode process supervision for process-group sandboxes."""

from __future__ import annotations

import shlex
import uuid
from collections.abc import Mapping
from pathlib import Path

from benchmark_service.sandbox.types import (
    ControlledWorkload,
    LINUX_CGROUP_V2_V1,
    LINUX_PROCESS_GROUP_V1,
    Sandbox,
    SandboxError,
)


async def controlled_episode_workload(
    sandbox: Sandbox,
    command: str,
    *,
    cwd: str | None = None,
    env_vars: Mapping[str, str] | None = None,
) -> ControlledWorkload:
    """Prepare the uploaded owner before constructing the auto-starting native workload."""
    if sandbox.generation_containment == LINUX_CGROUP_V2_V1:
        return sandbox.controlled_workload(command, cwd=cwd, env_vars=env_vars)
    if sandbox.generation_containment != LINUX_PROCESS_GROUP_V1:
        raise SandboxError("Episode requires supported generation containment")
    directory = f"/tmp/cbs-episode-{uuid.uuid4().hex}"
    result = await sandbox.exec(f"mkdir {shlex.quote(directory)}")
    if result.exit_code:
        raise SandboxError(f"Episode directory preparation failed: {result.output}")
    script = f"{directory}/supervisor.py"
    await sandbox.upload_file(script, Path(__file__).with_name("episode_supervisor.py").read_bytes())
    return sandbox._controlled_episode_workload(command, script, cwd=cwd, env_vars=env_vars)
