from __future__ import annotations

import asyncio
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from benchmark_service.sandbox.episode import controlled_episode_workload
from benchmark_service.sandbox.types import (
    ControlledWorkloadResult,
    ExecResult,
    LINUX_PROCESS_GROUP_V1,
    SandboxError,
)


SCRIPT = Path(__file__).parents[1] / "src/benchmark_service/sandbox/episode_supervisor.py"


def _wait_for(path: Path) -> None:
    deadline = time.monotonic() + 5
    while not path.exists():
        assert time.monotonic() < deadline
        time.sleep(0.01)


def _live(pid: int) -> bool:
    try:
        with open(f"/proc/{pid}/stat") as stat:
            return stat.read().split()[2] not in ("Z", "X")
    except FileNotFoundError:
        return False


def _request_stop(directory: Path) -> bytes:
    with socket.socket(socket.AF_UNIX) as control:
        control.settimeout(5)
        control.connect(str(directory / "control"))
        control.sendall(b"STOP")
        return control.recv(7)


def _finish_owner(process: subprocess.Popen, directory: Path) -> None:
    ready = directory / "READY"
    deadline = time.monotonic() + 5
    while not ready.exists() and process.poll() is None:
        assert time.monotonic() < deadline
        time.sleep(0.01)
    if process.poll() is None:
        try:
            _request_stop(directory)
        except ConnectionRefusedError:
            pass
        finally:
            process.wait(timeout=5)
    if ready.exists():
        assert (directory / "DRAINED").exists()


def test_natural_exit_drains_separate_session_and_preserves_status(tmp_path: Path) -> None:
    child = tmp_path / "child"
    command = f"setsid sh -c 'echo $$ > {child}.tmp; mv {child}.tmp {child}; sleep 30' & while ! test -f {child}; do sleep 0.01; done; exit 23"
    script = shutil.copyfile(SCRIPT, tmp_path / "supervisor.py")
    process = subprocess.Popen(["python3", str(script), str(tmp_path), command])
    try:
        _wait_for(child)
        assert process.wait(timeout=5) == 23
        assert (tmp_path / "READY").exists()
        assert (tmp_path / "DRAINED").exists()
        assert not _live(int(child.read_text()))
    finally:
        _finish_owner(process, tmp_path)


def test_forced_stop_drains_owner_and_not_unrelated_sibling(tmp_path: Path) -> None:
    child = tmp_path / "child"
    orchestrator = tmp_path / "orchestrator"
    command = f"echo $$ > {orchestrator}; setsid sh -c 'echo $$ > {child}.tmp; mv {child}.tmp {child}; sleep 30' & wait"
    script = shutil.copyfile(SCRIPT, tmp_path / "supervisor.py")
    process = subprocess.Popen(["python3", str(script), str(tmp_path), command])
    try:
        unrelated = subprocess.Popen(["sleep", "30"], start_new_session=True)
        try:
            _wait_for(child)
            orchestrator_pid = int(orchestrator.read_text())
            assert os.getsid(orchestrator_pid) == orchestrator_pid
            assert os.getsid(orchestrator_pid) != os.getsid(process.pid)
            assert _request_stop(tmp_path) == b"DRAINED"
            assert process.wait(timeout=5) == 137
            assert not _live(int(child.read_text()))
            assert unrelated.poll() is None
        finally:
            unrelated.kill()
            unrelated.wait()
    finally:
        _finish_owner(process, tmp_path)


@pytest.mark.parametrize("stop", [False, True])
def test_original_child_stays_unreaped_until_adopted_descendants_are_gone(tmp_path: Path, stop: bool) -> None:
    child = tmp_path / "child"
    command = f"setsid sh -c 'echo $$ > {child}.tmp; mv {child}.tmp {child}; sleep 30' & "
    command += "wait" if stop else f"while ! test -f {child}; do sleep 0.01; done; exit 23"
    bootstrap = (
        "import importlib.util,subprocess,sys\n"
        "spec=importlib.util.spec_from_file_location('owner',sys.argv[1])\n"
        "owner=importlib.util.module_from_spec(spec); spec.loader.exec_module(owner)\n"
        "original_wait=subprocess.Popen.wait\n"
        "def observed_wait(self,*args,**kwargs):\n"
        "    if any(pid != self.pid for pid in owner.children()):\n"
        "        open(sys.argv[1]+'/early_reap','w').close()\n"
        "    return original_wait(self,*args,**kwargs)\n"
        "subprocess.Popen.wait=observed_wait\n"
        "sys.argv=[sys.argv[1],sys.argv[2],sys.argv[3]]\n"
        "sys.exit(owner.main())\n"
    )
    script = shutil.copyfile(SCRIPT, tmp_path / "supervisor.py")
    process = subprocess.Popen(["python3", "-c", bootstrap, str(script), str(tmp_path), command])
    try:
        _wait_for(child)
        if stop:
            assert _request_stop(tmp_path) == b"DRAINED"
        assert process.wait(timeout=5) == (137 if stop else 23)
        assert not (tmp_path / "early_reap").exists()
        assert not _live(int(child.read_text()))
    finally:
        _finish_owner(process, tmp_path)


def test_signal_exit_is_shell_compatible(tmp_path: Path) -> None:
    process = subprocess.run(
        [sys.executable, str(SCRIPT), str(tmp_path), "kill -TERM $$"], timeout=5
    )
    assert process.returncode == 143
    assert (tmp_path / "DRAINED").exists()


def test_failed_popen_has_no_child_and_drained_evidence(tmp_path: Path) -> None:
    bootstrap = (
        "import importlib.util,sys\n"
        "spec=importlib.util.spec_from_file_location('owner',sys.argv[1])\n"
        "owner=importlib.util.module_from_spec(spec); spec.loader.exec_module(owner)\n"
        "def fail(*args,**kwargs): raise OSError(2,'missing shell')\n"
        "owner.subprocess.Popen=fail\n"
        "sys.argv=[sys.argv[1],sys.argv[2],sys.argv[3]]\n"
        "sys.exit(owner.main())\n"
    )
    process = subprocess.run(
        [sys.executable, "-c", bootstrap, str(SCRIPT), str(tmp_path), "echo never"],
        timeout=5,
        capture_output=True,
        text=True,
    )
    assert process.returncode == 127
    assert "episode launch failed" in process.stderr
    assert (tmp_path / "DRAINED").exists()


class _Native:
    def __init__(self, result: ControlledWorkloadResult) -> None:
        self.result = result
        self.killed = False

    async def wait(self) -> ControlledWorkloadResult:
        return self.result

    async def kill(self) -> None:
        self.killed = True



class _Sandbox:
    def __init__(self, containment, state: str) -> None:
        self.generation_containment = containment
        self.state = state
        self.native = _Native(ControlledWorkloadResult(ExecResult(exit_code=23), 42.0))

    async def exec(self, command: str):
        if command.startswith("if test -f"):
            return ExecResult(exit_code=0, output=self.state + "\n")
        return ExecResult(exit_code=0)

    async def upload_file(self, path: str, content: bytes) -> None:
        return None

    def controlled_workload(self, command: str, *, cwd=None, env_vars=None):
        return self.native

@pytest.mark.asyncio
async def test_ready_without_drained_cannot_be_native_success() -> None:
    pg = _Sandbox(LINUX_PROCESS_GROUP_V1, "READY")
    workload = await controlled_episode_workload(pg, "echo hi")
    with pytest.raises(SandboxError, match="without confirmed descendant drain"):
        await workload.wait()
    with pytest.raises(SandboxError, match="without confirmed descendant drain"):
        await workload.kill()
    assert not pg.native.killed


@pytest.mark.asyncio
async def test_kill_rechecks_pre_ready_state_after_native_exit() -> None:
    class RacingSandbox(_Sandbox):
        async def exec(self, command: str):
            if command.startswith("if test -f"):
                state = self.state
                self.state = "READY"
                return ExecResult(exit_code=0, output=state + "\n")
            return await super().exec(command)

    pg = RacingSandbox(LINUX_PROCESS_GROUP_V1, "PENDING")
    workload = await controlled_episode_workload(pg, "echo hi")
    await asyncio.sleep(0)
    with pytest.raises(SandboxError, match="without confirmed descendant drain"):
        await workload.kill()
    assert not pg.native.killed
