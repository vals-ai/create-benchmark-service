"""Exercise process-group probe and stop with the running environment's /bin/sh."""

import asyncio
import os
import signal
from pathlib import Path


from benchmark_service.sandbox._process_group import probe_command, stop_command


async def test_probe_and_stop_negative_process_group(tmp_path: Path) -> None:
    shell = Path("/bin/sh")
    (tmp_path / "sh").symlink_to(shell)
    env = {**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}"}
    marker = tmp_path / "group-id"
    probe = await asyncio.create_subprocess_exec(
        str(shell), "-c", probe_command(str(marker)), env=env,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
    )
    output, _ = await asyncio.wait_for(probe.communicate(), 5)
    assert probe.returncode == 0, output
    assert not marker.exists()

    process = await asyncio.create_subprocess_exec(
        "setsid", str(shell), "-c", "printf 'ready\\n'; sleep 30 & wait",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
    )
    try:
        assert process.stdout is not None
        assert await asyncio.wait_for(process.stdout.readline(), 5) == b"ready\n"
        marker.write_text(str(process.pid))
        stop = await asyncio.create_subprocess_exec(
            str(shell), "-c", stop_command(process.pid, str(marker)),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        output, _ = await asyncio.wait_for(stop.communicate(), 5)
        assert stop.returncode == 0, output
        assert not marker.exists()
        assert await asyncio.wait_for(process.wait(), 5) != 0
    finally:
        if process.returncode is None:
            os.killpg(process.pid, signal.SIGKILL)
            await process.wait()
