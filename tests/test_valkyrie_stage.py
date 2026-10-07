"""Exercise standalone agent rendering and process-group lifecycle."""

import ctypes
import errno
import json
import os
import signal
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from benchmark_service import valkyrie_stage


@pytest.fixture(autouse=True)
def _restore_subreaper() -> Iterator[None]:
    libc = ctypes.CDLL(None, use_errno=True)
    original = ctypes.c_int()
    if libc.prctl(37, ctypes.byref(original), 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "PR_GET_CHILD_SUBREAPER")
    try:
        yield
    finally:
        if libc.prctl(36, original.value, 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), "PR_SET_CHILD_SUBREAPER")


class Reporter:
    def __init__(self) -> None:
        self.events: list[tuple[str, str | None]] = []

    def begin(self, container: str | None) -> None:
        self.events.append(("begin", container))

    def end(self) -> None:
        self.events.append(("end", None))


def agents(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, **config: object) -> tuple[valkyrie_stage.Agents, Reporter]:
    stage_dir = tmp_path / "stage"
    stage_dir.mkdir()
    reporter = Reporter()
    monkeypatch.setenv("VALKYRIE_STAGE_DIR", str(stage_dir))
    monkeypatch.setattr(valkyrie_stage, "StageReporter", lambda: reporter)
    (stage_dir / "agent.json").write_text(json.dumps({
        "run_cmd": "exit 0",
        "continue_cmd": None,
        "interrupt_grace_seconds": None,
        "container_name": None,
        "final_output": None,
        "parallel_agents": 2,
        "slots_root": str(tmp_path / "slots"),
        **config,
    }))
    return valkyrie_stage.Agents(), reporter


def test_commands_render_only_bound_tokens_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    slot_root = tmp_path / "{container_name}" / "{slot_dir}"
    observation = tmp_path / "observation-{slot_dir}"
    observation.write_text("observed")
    first = "printf '%s\\n' '{\"json\": {\"ok\": true}}' '{problem_statement_path}' > '{slot_dir}/first'"
    continuation = "printf '%s\\n' '{\"json\": {\"ok\": true}}' '{problem_statement_path}' > '{slot_dir}/next'"
    worker, reporter = agents(
        tmp_path, monkeypatch, run_cmd=first, continue_cmd=continuation,
        slots_root=str(slot_root), final_output="{slot_dir}/first",
    )
    slot_dir = slot_root / "one" / "agent"
    worker.start("one", str(observation))
    assert worker.wait_any() == valkyrie_stage.SlotResult("one", "exited", 0)
    assert (slot_dir / "first").read_text().splitlines() == ['{"json": {"ok": true}}', str(observation)]
    assert (slot_root / "one" / "turns" / "1" / "first").read_text() == (slot_dir / "first").read_text()
    worker.start("one", str(observation))
    assert worker.wait_any() == valkyrie_stage.SlotResult("one", "exited", 0)
    assert (slot_dir / "next").read_text().splitlines() == ['{"json": {"ok": true}}', str(observation)]
    assert reporter.events == [("begin", None), ("end", None)] * 2


def test_container_is_bound_only_when_configured(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    worker, _ = agents(tmp_path, monkeypatch, run_cmd="printf '%s' '{container_name}' > '{slot_dir}/name'")
    worker.start("one", "observation")
    assert worker.wait_any().exit_code == 0
    assert (tmp_path / "slots" / "one" / "agent" / "name").read_text() == "{container_name}"


def test_configured_container_binding_keeps_inserted_braces_opaque(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    docker = tmp_path / "docker"
    docker.write_text("#!/bin/sh\nif [ \"$1\" = rm ]; then printf '%s' \"$3\" > '" + str(tmp_path / "removed") + "'; fi\n")
    docker.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    worker, reporter = agents(
        tmp_path, monkeypatch,
        container_name="box-{slot}-{slot_dir}",
        run_cmd="printf '%s' '{container_name}' > '{slot_dir}/name'",
    )
    worker.start("one", "observation")
    assert worker.wait_any() == valkyrie_stage.SlotResult("one", "exited", 0)
    assert (tmp_path / "slots" / "one" / "agent" / "name").read_text() == "box-one-{slot_dir}"
    assert (tmp_path / "removed").read_text() == "box-one-{slot_dir}"
    assert reporter.events == [("begin", "box-one-{slot_dir}"), ("end", None)]


def test_natural_exit_keeps_leader_until_group_cleanup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    gate = tmp_path / "slots" / "one" / "agent" / "go"
    gate.parent.mkdir(parents=True)
    os.mkfifo(gate)
    worker, reporter = agents(
        tmp_path, monkeypatch,
        run_cmd=(
            "sleep 30 & printf '%s' \"$!\" > '{slot_dir}/child.pid.tmp'; "
            "mv '{slot_dir}/child.pid.tmp' '{slot_dir}/child.pid'; "
            "read ready < '{slot_dir}/go'; exit 23"
        ),
    )
    original_signal = worker._signal_group
    observed: list[int] = []

    def signal_group(pid: int, sig: signal.Signals) -> None:
        if sig == signal.SIGKILL:
            assert os.waitid(os.P_PID, pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is not None
            observed.append(pid)
        original_signal(pid, sig)

    monkeypatch.setattr(worker, "_signal_group", signal_group)
    child_pid_file = gate.parent / "child.pid"
    libc = ctypes.CDLL(None, use_errno=True)
    leader_fd: int | None = None
    child_fd: int | None = None
    child_pid: int | None = None
    released = False
    worker.start("one", "observation")
    try:
        leader = worker._active["one"].process
        leader_fd = libc.pidfd_open(leader.pid, 0)
        if leader_fd == -1:
            raise OSError(ctypes.get_errno(), "pidfd_open")
        deadline = time.monotonic() + 5
        while not child_pid_file.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        child_pid = int(child_pid_file.read_text())
        child_fd = libc.pidfd_open(child_pid, 0)
        if child_fd == -1:
            raise OSError(ctypes.get_errno(), "pidfd_open")
        released = True
        with gate.open("w") as channel:
            channel.write("go\n")
        result = worker.wait_any()
        assert result == valkyrie_stage.SlotResult("one", "exited", 23)
        assert observed
        assert not (Path("/proc") / str(child_pid)).exists()
        assert reporter.events == [("begin", None), ("end", None)]
    finally:
        monkeypatch.setattr(worker, "_signal_group", original_signal)
        try:
            if not released:
                original_signal(leader.pid, signal.SIGKILL)
                while not worker._group_empty(leader.pid):
                    time.sleep(0.01)
            else:
                try:
                    stopped = libc.pidfd_send_signal(child_fd, signal.SIGKILL, None, 0)
                    assert stopped == 0 or ctypes.get_errno() == errno.ESRCH
                finally:
                    stopped = libc.pidfd_send_signal(leader_fd, signal.SIGKILL, None, 0)
                    assert stopped == 0 or ctypes.get_errno() == errno.ESRCH
        finally:
            try:
                leader.wait()
            finally:
                try:
                    if child_pid is not None:
                        try:
                            os.waitpid(child_pid, 0)
                        except ChildProcessError:
                            pass
                finally:
                    try:
                        if child_fd is not None and child_fd >= 0:
                            os.close(child_fd)
                    finally:
                        if leader_fd is not None and leader_fd >= 0:
                            os.close(leader_fd)


def test_parallel_exhaustion_and_graceful_stop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    worker, reporter = agents(
        tmp_path, monkeypatch, run_cmd="sleep 30", interrupt_grace_seconds=0.2,
    )
    worker.start("one", "observation")
    try:
        worker.start("two", "observation")
        assert reporter.events == [("begin", None)]
        assert worker.stop("one").reason == "stopped"
        assert reporter.events == [("begin", None)]
        (tmp_path / "stage" / "exhausted").touch()
        assert worker.wait_any().reason == "exhausted"
        assert reporter.events == [("begin", None), ("end", None)]
    finally:
        for slot in list(worker._active):
            worker.stop(slot)
