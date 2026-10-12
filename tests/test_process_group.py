"""Exercise process-group probe and stop with the running environment's /bin/sh."""

import asyncio
import os
import shlex
import signal
from pathlib import Path

import pytest


from benchmark_service.sandbox._process_group import cleanup_command, owner_command, probe_command, stop_command


def _group_live(pgid: int) -> bool:
    for stat in Path('/proc').glob('[0-9]*/stat'):
        try:
            state, _, group = stat.read_text().split(') ', 1)[1].split()[:3]
        except FileNotFoundError:
            continue
        if int(group) == pgid and state not in ('Z', 'X'):
            return True
    return False


async def _finish_owned_process(process: asyncio.subprocess.Process, control: Path) -> None:
    marker = control / 'pgid'
    group_id = None
    stopped = False
    try:
        async with asyncio.timeout(5):
            while True:
                published = marker.read_text() if marker.exists() else ''
                if published.endswith('\n') or process.returncode is not None:
                    break
                await asyncio.sleep(0.01)
        if published.endswith('\n'):
            group_id = int(published)
            stop = await asyncio.create_subprocess_exec('/bin/sh', '-c', stop_command(str(control)))
            assert await asyncio.wait_for(stop.wait(), 5) == 0
            stopped = True
    finally:
        if not stopped:
            if group_id is None and marker.exists():
                published = marker.read_text()
                if published.endswith('\n'):
                    group_id = int(published)
            if group_id is not None:
                try:
                    os.killpg(group_id, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        if process.returncode is None:
            process.kill()
        if process.stdout is not None:
            await asyncio.wait_for(process.stdout.read(), 5)
        await asyncio.wait_for(process.wait(), 5)
        if not stopped and group_id is not None:
            async with asyncio.timeout(5):
                while _group_live(group_id):
                    await asyncio.sleep(0.01)
        if control.exists():
            cleanup = await asyncio.create_subprocess_exec('/bin/sh', '-c', cleanup_command(str(control)))
            assert await asyncio.wait_for(cleanup.wait(), 5) == 0


async def test_stop_requires_published_marker(tmp_path: Path) -> None:
    stop = await asyncio.create_subprocess_exec(
        "/bin/sh", "-c", stop_command(str(tmp_path / "not-admitted")),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
    )
    output, _ = await asyncio.wait_for(stop.communicate(), 5)
    assert stop.returncode == 75
    assert output == b""


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
        stop = await asyncio.create_subprocess_exec(
            str(shell), "-c", stop_command(str(control)),
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
        completed = await asyncio.create_subprocess_exec(
            "/bin/sh", "-c", stop_command(str(control)),
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
                cleanup = await asyncio.create_subprocess_exec(
                    "/bin/sh", "-c", stop_command(str(control)),
                )
                assert await asyncio.wait_for(cleanup.wait(), 5) == 0
            owned_stat = Path(f"/proc/{owned_pid}/stat")
            try:
                stat = owned_stat.read_text()
            except FileNotFoundError:
                pass
            else:
                assert stat.split(") ", 1)[1][0] in ("Z", "X")
            assert _group_live(sibling.pid)
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
        command = (
            f"exec 5<>{shlex.quote(str(gate))}; printf 'gate-ready\n'; "
            f"read -r _ <&5; exec 5>&-; "
            + owner_command("exec sleep 30", str(control), "/bin/sh -c")
        )
        process = await asyncio.create_subprocess_exec(
            "/bin/sh", "-c", command,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        cleanup_entered = asyncio.Event()

        async def finish() -> None:
            cleanup_entered.set()
            await _finish_owned_process(process, control)

        cleanup = asyncio.create_task(finish())
        try:
            assert process.stdout is not None
            assert await asyncio.wait_for(process.stdout.readline(), 5) == b'gate-ready\n'
            await asyncio.wait_for(cleanup_entered.wait(), 5)
            assert not cleanup.done()
            assert not (control / 'pgid').exists()
            with pytest.raises(AssertionError, match='preready'):
                assert False, 'preready'
        finally:
            try:
                with gate.open('r+b', buffering=0) as writer:
                    writer.write(b'start\n')
            finally:
                await asyncio.wait_for(cleanup, 5)
        assert _group_live(sibling.pid)
        assert not control.exists()
    finally:
        if sibling.returncode is None:
            sibling.kill()
        await sibling.wait()
