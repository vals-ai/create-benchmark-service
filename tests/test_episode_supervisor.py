from __future__ import annotations

import os
import shutil
import signal
import subprocess
import time
from pathlib import Path

import pytest

from benchmark_service.sandbox._process_group import (
    cleanup_command,
    episode_owner_command,
    stop_command,
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


def _start_owner(directory: Path, command: str, script: Path = SCRIPT) -> subprocess.Popen[bytes]:
    if script == SCRIPT:
        script = directory.parent / "supervisor.py"
        shutil.copyfile(SCRIPT, script)
    return subprocess.Popen(
        ["/bin/sh", "-c", episode_owner_command(command, str(directory), str(script))],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _stop(directory: Path) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["/bin/sh", "-c", stop_command(str(directory), episode_owner=True)],
        capture_output=True,
        timeout=5,
    )


def _finish_owner(process: subprocess.Popen[bytes], directory: Path) -> None:
    marker = directory / 'pgid'
    child_marker = directory.parent / 'child'
    owner_group = None
    stopped = False
    try:
        deadline = time.monotonic() + 5
        while True:
            published = marker.read_text() if marker.exists() else ''
            if published.endswith('\n') or process.poll() is not None:
                break
            assert time.monotonic() < deadline
            time.sleep(0.01)
        if published.endswith('\n'):
            owner_group = int(published)
            stopped = _stop(directory).returncode == 0
    finally:
        if owner_group is None and marker.exists():
            published = marker.read_text()
            if published.endswith('\n'):
                owner_group = int(published)
        child_group = int(child_marker.read_text()) if child_marker.exists() else None
        groups: set[int] = set()
        if child_group is not None and _live(child_group):
            groups.add(child_group)
        if not stopped and owner_group is not None:
            children = Path(f'/proc/{owner_group}/task/{owner_group}/children')
            if children.exists():
                for child in children.read_text().split():
                    try:
                        groups.add(os.getpgid(int(child)))
                    except ProcessLookupError:
                        pass
            groups.add(owner_group)
        for group in groups:
            try:
                os.killpg(group, signal.SIGKILL)
            except ProcessLookupError:
                pass
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=5)
        if child_group is not None:
            deadline = time.monotonic() + 5
            while _live(child_group):
                assert time.monotonic() < deadline
                time.sleep(0.01)
        if directory.exists():
            cleanup = subprocess.run(
                ['/bin/sh', '-c', cleanup_command(str(directory))],
                capture_output=True,
                timeout=5,
            )
            assert cleanup.returncode == 0, cleanup.stderr


def test_natural_exit_drains_separate_session_and_preserves_status(tmp_path: Path) -> None:
    directory = tmp_path / "owner"
    child = tmp_path / "child"
    command = (
        f"setsid sh -c 'echo $$ > {child}.tmp; mv {child}.tmp {child}; sleep 30' & "
        f"while ! test -f {child}; do sleep 0.01; done; exit 23"
    )
    process = _start_owner(directory, command)
    try:
        _wait_for(child)
        assert process.wait(timeout=5) == 137
        assert _stop(directory).stdout == b"23\n"
        assert not _live(int(child.read_text()))
    finally:
        _finish_owner(process, directory)


def test_forced_stop_drains_owner_and_not_unrelated_sibling(tmp_path: Path) -> None:
    directory = tmp_path / "owner"
    child = tmp_path / "child"
    orchestrator = tmp_path / "orchestrator"
    command = f"echo $$ > {orchestrator}; setsid sh -c 'echo $$ > {child}.tmp; mv {child}.tmp {child}; sleep 30' & wait"
    process = _start_owner(directory, command)
    try:
        unrelated = subprocess.Popen(["sleep", "30"], start_new_session=True)
        try:
            _wait_for(child)
            orchestrator_pid = int(orchestrator.read_text())
            assert os.getsid(orchestrator_pid) == orchestrator_pid
            assert os.getsid(orchestrator_pid) != os.getsid(process.pid)
            stopped = _stop(directory)
            assert stopped.returncode == 0, stopped.stderr
            assert stopped.stdout == b"137\n"
            assert process.wait(timeout=5) == 137
            assert not _live(int(child.read_text()))
            assert unrelated.poll() is None
        finally:
            unrelated.kill()
            unrelated.wait(timeout=5)
    finally:
        _finish_owner(process, directory)


@pytest.mark.parametrize("stop", [False, True])
def test_original_child_stays_unreaped_until_adopted_descendants_are_gone(
    tmp_path: Path, stop: bool
) -> None:
    directory = tmp_path / "owner"
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
        f"        open({str(tmp_path / 'early_reap')!r},'w').close()\n"
        "    return original_wait(self,*args,**kwargs)\n"
        "subprocess.Popen.wait=observed_wait\n"
        "sys.argv=[sys.argv[1],sys.argv[2],sys.argv[3]]\n"
        "sys.exit(owner.main())\n"
    )
    wrapper = tmp_path / "instrumented.py"
    wrapper.write_text(
        "import sys\n"
        f"sys.argv=[sys.argv[0], {str(SCRIPT)!r}, *sys.argv[1:]]\n"
        + bootstrap
    )
    process = _start_owner(directory, command, wrapper)
    try:
        _wait_for(child)
        if stop:
            stopped = _stop(directory)
            assert stopped.returncode == 0, stopped.stderr
            assert stopped.stdout == b"137\n"
        assert process.wait(timeout=5) == 137
        if not stop:
            assert _stop(directory).stdout == b"23\n"
        assert not (tmp_path / "early_reap").exists()
        assert not _live(int(child.read_text()))
    finally:
        _finish_owner(process, directory)


def test_signal_exit_is_shell_compatible(tmp_path: Path) -> None:
    directory = tmp_path / "owner"
    process = _start_owner(directory, "kill -TERM $$")
    try:
        assert process.wait(timeout=5) == 137
        stopped = _stop(directory)
        assert stopped.returncode == 0, stopped.stderr
        assert stopped.stdout == b"143\n"
    finally:
        _finish_owner(process, directory)


def test_failed_popen_reports_failure_without_fabricated_absence(tmp_path: Path) -> None:
    directory = tmp_path / "owner"
    bootstrap = (
        "import importlib.util,sys\n"
        f"spec=importlib.util.spec_from_file_location('owner',{str(SCRIPT)!r})\n"
        "owner=importlib.util.module_from_spec(spec); spec.loader.exec_module(owner)\n"
        "def fail(*args,**kwargs): raise OSError(2,'missing shell')\n"
        "owner.subprocess.Popen=fail\n"
        "sys.exit(owner.main())\n"
    )
    wrapper = tmp_path / "failed_popen.py"
    wrapper.write_text(bootstrap)
    process = _start_owner(directory, "echo never", wrapper)
    try:
        _, stderr = process.communicate(timeout=5)
        assert process.returncode == 137
        assert b"episode launch failed" in stderr
        stopped = _stop(directory)
        assert stopped.returncode == 0, stopped.stderr
        assert stopped.stdout == b"127\n"
    finally:
        _finish_owner(process, directory)


def test_prelaunch_failure_never_claims_postdrain_status(tmp_path: Path) -> None:
    directory = tmp_path / "owner"
    process = _start_owner(directory, "touch " + str(tmp_path / "must-not-run"), tmp_path / "missing.py")
    try:
        _, stderr = process.communicate(timeout=5)
        assert process.returncode != 0
        assert b"missing.py" in stderr
        assert (directory / "status").read_text() == "PRELAUNCH\n"
        stopped = _stop(directory)
        assert stopped.returncode == 0
        assert stopped.stdout == b""
        assert not (tmp_path / "must-not-run").exists()
    finally:
        _finish_owner(process, directory)



def test_owner_crash_after_launch_cannot_claim_descendant_drain(tmp_path: Path) -> None:
    directory = tmp_path / "owner"
    child = tmp_path / "child"
    process = _start_owner(directory, f"echo $$ > {child}.tmp; mv {child}.tmp {child}; exec sleep 2")
    try:
        _wait_for(child)
        assert not (directory / "status").exists()
        owner_pid = int((directory / "pgid").read_text())
        os.kill(owner_pid, 9)
        assert process.wait(timeout=5) == 137
        stopped = _stop(directory)
        assert stopped.returncode == 76
        assert stopped.stdout == b""
    finally:
        _finish_owner(process, directory)


def test_queued_stop_during_popen_failure_reports_postdrain_failure(tmp_path: Path) -> None:
    directory = tmp_path / "owner"
    release = tmp_path / "release"
    bootstrap = (
        "import importlib.util,os,sys,time\n"
        f"open({str(tmp_path / 'at_gate')!r},'w').close()\n"
        f"while not os.path.exists({str(release)!r}): time.sleep(0.01)\n"
        f"spec=importlib.util.spec_from_file_location('owner',{str(SCRIPT)!r})\n"
        "owner=importlib.util.module_from_spec(spec); spec.loader.exec_module(owner)\n"
        "def fail(*args,**kwargs): raise OSError(2,'missing shell')\n"
        "owner.subprocess.Popen=fail\n"
        "sys.exit(owner.main())\n"
    )
    wrapper = tmp_path / "queued_stop.py"
    wrapper.write_text(bootstrap)
    process = _start_owner(directory, "echo never", wrapper)
    try:
        _wait_for(tmp_path / "at_gate")
        assert (directory / "status").read_text() == "PRELAUNCH\n"
        with open(directory / "stop", "w") as control:
            control.write("stop\n")
        release.touch()
        _, stderr = process.communicate(timeout=5)
        assert process.returncode == 137
        assert b"episode launch failed" in stderr
        stopped = _stop(directory)
        assert stopped.returncode == 0, stopped.stderr
        assert stopped.stdout == b"127\n"
    finally:
        release.touch()
        _finish_owner(process, directory)
