"""Exercise process-group probe and stop with the running environment's /bin/sh."""

import asyncio
import os
import shlex
from pathlib import Path

import pytest


from benchmark_service.sandbox._process_group import cleanup_command, owner_command, probe_command, stop_command


async def _finish_owned_process(process: asyncio.subprocess.Process, control: Path) -> None:
    marker = control / "pgid"
    try:
        async with asyncio.timeout(5):
            while True:
                published = marker.read_text() if marker.exists() else ""
                if published.endswith("\n") or process.returncode is not None:
                    break
                await asyncio.sleep(0.01)
        if published.endswith("\n"):
            group_id = int(published)
            stopped = await asyncio.create_subprocess_exec("/bin/sh", "-c", stop_command(group_id, str(control)))
            assert await asyncio.wait_for(stopped.wait(), 5) == 0
        if process.stdout is not None:
            await asyncio.wait_for(process.stdout.read(), 5)
        await asyncio.wait_for(process.wait(), 5)
        if control.exists():
            cleanup = await asyncio.create_subprocess_exec("/bin/sh", "-c", cleanup_command(str(control)))
            assert await asyncio.wait_for(cleanup.wait(), 5) == 0
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


@pytest.mark.parametrize("session_leader", [False, True])
async def test_probe_and_stop_negative_process_group(tmp_path: Path, session_leader: bool) -> None:
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

    control = tmp_path / "owner"
    process = await asyncio.create_subprocess_exec(
        str(shell), "-c", owner_command("printf 'ready\n'; sleep 30 & wait", str(control), "/bin/sh -c"),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        start_new_session=session_leader,
    )
    try:
        assert process.stdout is not None
        assert await asyncio.wait_for(process.stdout.readline(), 5) == b"ready\n"
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(process.wait(), 0.05)
        group_id = int((control / "pgid").read_text())
        stop = await asyncio.create_subprocess_exec(
            str(shell), "-c", stop_command(group_id, str(control)),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        output, _ = await asyncio.wait_for(stop.communicate(), 5)
        assert stop.returncode == 0, output
        assert control.exists()
        assert await asyncio.wait_for(process.wait(), 5) != 0
    finally:
        await _finish_owned_process(process, control)
        assert not control.exists()


@pytest.mark.parametrize("session_leader", [False, True])
async def test_natural_owner_preserves_foreground_stdin_status_and_output(
    tmp_path: Path, session_leader: bool,
) -> None:
    control = tmp_path / "natural"
    process = await asyncio.create_subprocess_exec(
        "/bin/sh", "-c",
        owner_command('read word; printf "got:%s\n" "$word"; (sleep 0.2; echo child-tail) & exit 7',
                      str(control), "/bin/sh -c"),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        start_new_session=session_leader,
    )
    try:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(process.wait(), 0.05)
        output, _ = await asyncio.wait_for(process.communicate(b"hello\n"), 5)
        assert output == b"got:hello\n"
        assert process.returncode != 0
        group_id = int((control / "pgid").read_text())
        completed = await asyncio.create_subprocess_exec(
            "/bin/sh", "-c", stop_command(group_id, str(control)),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        status, _ = await asyncio.wait_for(completed.communicate(), 5)
        assert completed.returncode == 0, status
        assert status == b"7\n"
        assert control.exists()
    finally:
        await _finish_owned_process(process, control)
        assert not control.exists()


async def test_failed_assertion_cleanup_spares_unrelated_sibling(tmp_path: Path) -> None:
    sibling = await asyncio.create_subprocess_exec("sleep", "30", start_new_session=True)
    try:
        control = tmp_path / "owned"
        process = await asyncio.create_subprocess_exec(
            "/bin/sh", "-c", owner_command("printf 'ready:%s\n' $$; exec sleep 30", str(control), "/bin/sh -c"),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        try:
            assert process.stdout is not None
            owned_pid = int((await asyncio.wait_for(process.stdout.readline(), 5)).removeprefix(b"ready:"))
            async with asyncio.timeout(5):
                while Path(f"/proc/{owned_pid}/comm").read_text().strip() != "sleep":
                    await asyncio.sleep(0.01)
            try:
                with pytest.raises(AssertionError, match="intentional"):
                    assert False, "intentional"
            finally:
                group_id = int((control / "pgid").read_text())
                cleanup = await asyncio.create_subprocess_exec(
                    "/bin/sh", "-c", stop_command(group_id, str(control)),
                )
                assert await asyncio.wait_for(cleanup.wait(), 5) == 0
            owned_stat = Path(f"/proc/{owned_pid}/stat")
            try:
                stat = owned_stat.read_text()
            except FileNotFoundError:
                pass
            else:
                assert stat.split(") ", 1)[1][0] in ("Z", "X")
            assert sibling.returncode is None
            assert await asyncio.wait_for(process.wait(), 5) != 0
        finally:
            await _finish_owned_process(process, control)
    finally:
        if sibling.returncode is None:
            sibling.kill()
        await sibling.wait()


async def test_preready_assertion_cleanup_waits_for_owned_group(tmp_path: Path) -> None:
    gate = tmp_path / "launch-gate"
    os.mkfifo(gate)
    sibling = await asyncio.create_subprocess_exec("sleep", "30", start_new_session=True)
    try:
        control = tmp_path / "delayed-owner"
        command = f"read -r _ < {shlex.quote(str(gate))}; " + owner_command("exec sleep 30", str(control), "/bin/sh -c")
        process = await asyncio.create_subprocess_exec(
            "/bin/sh", "-c", command,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        try:
            assert not (control / "pgid").exists()
            with pytest.raises(AssertionError, match="preready"):
                assert False, "preready"
        finally:
            with gate.open("w") as writer:
                writer.write("start\n")
            await _finish_owned_process(process, control)
        assert sibling.returncode is None
        assert not control.exists()
    finally:
        if sibling.returncode is None:
            sibling.kill()
        await sibling.wait()
