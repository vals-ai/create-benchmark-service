"""Exercise standalone agent rendering and process-group lifecycle."""

import base64
import ctypes
import errno
import io
import json
import os
import queue
import shlex
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from benchmark_service import valkyrie_stage


@pytest.fixture(autouse=True)
def restore_subreaper() -> Iterator[None]:
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
    original_signal = os.killpg
    observed: list[int] = []

    def signal_group(pid: int, sig: signal.Signals) -> None:
        if sig == signal.SIGKILL:
            assert os.waitid(os.P_PID, pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is not None
            observed.append(pid)
        original_signal(pid, sig)

    monkeypatch.setattr(os, "killpg", signal_group)
    child_pid_file = gate.parent / "child.pid"
    libc = ctypes.CDLL(None, use_errno=True)
    leader_fd: int | None = None
    child_fd: int | None = None
    child_pid: int | None = None
    released = False
    worker.start("one", "observation")
    leader = worker._active["one"].process  # pyright: ignore[reportPrivateUsage]
    try:
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
        monkeypatch.setattr(os, "killpg", original_signal)
        try:
            if not released:
                original_signal(leader.pid, signal.SIGKILL)
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
    started_slots = {"one"}
    try:
        worker.start("two", "observation")
        started_slots.add("two")
        assert reporter.events == [("begin", None)]
        assert worker.stop("one").reason == "stopped"
        started_slots.remove("one")
        assert reporter.events == [("begin", None)]
        (tmp_path / "stage" / "exhausted").touch()
        assert worker.wait_any().reason == "exhausted"
        started_slots.clear()
        assert reporter.events == [("begin", None), ("end", None)]
    finally:
        for slot in ("one", "two"):
            if slot in started_slots:
                worker.stop(slot)


def test_generation_children_use_generation_gateway_while_evaluation_uses_native(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_hits: queue.Queue[str] = queue.Queue()
    native_hits: queue.Queue[str] = queue.Queue()
    release_generation = threading.Event()

    class GenerationHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            generation_hits.put(self.path)
            release_generation.wait(5)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"generation")

    class NativeHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            native_hits.put(self.path)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"native")

    with (
        ThreadingHTTPServer(("127.0.0.1", 0), GenerationHandler) as generation_server,
        ThreadingHTTPServer(("127.0.0.1", 0), NativeHandler) as native_server,
    ):
        generation_thread = threading.Thread(target=generation_server.serve_forever)
        native_thread = threading.Thread(target=native_server.serve_forever)
        generation_thread.start()
        native_thread.start()
        active: set[str] = set()
        evaluator: subprocess.Popen[bytes] | None = None
        try:
            native_url = f"http://127.0.0.1:{native_server.server_port}"
            generation_url = f"http://127.0.0.1:{generation_server.server_port}"
            monkeypatch.setenv("MODEL_GATEWAY_URL", native_url)
            monkeypatch.setenv("VALKYRIE_GENERATION_MODEL_GATEWAY_URL", generation_url)
            client = (
                "import os, sys, urllib.request; "
                "urllib.request.urlopen(os.environ['MODEL_GATEWAY_URL'] + '/' + sys.argv[1], timeout=5).read()"
            )
            stage_reporter = valkyrie_stage.StageReporter
            agents(
                tmp_path, monkeypatch,
                run_cmd=f"{shlex.quote(sys.executable)} -c {shlex.quote(client)} '{{problem_statement_path}}'",
            )
            stage_dir = tmp_path / "stage"
            (stage_dir / "key").write_text("11" * 32)
            (stage_dir / "ack").mkdir()
            events: list[str] = []

            class AckOutput(io.StringIO):
                def write(self, text: str) -> int:
                    size = super().write(text)
                    if text.startswith("\nVALKYRIE-STAGE/1 "):
                        _, payload, _ = text.strip().split(" ")
                        frame = json.loads(base64.urlsafe_b64decode(payload + "==="))
                        events.append(frame["event"])
                        (stage_dir / "ack" / str(frame["seq"])).touch()
                    return size

            monkeypatch.setattr(sys, "stdout", AckOutput())
            monkeypatch.setattr(valkyrie_stage, "StageReporter", stage_reporter)
            worker = valkyrie_stage.Agents()
            worker.start("one", "one")
            active.add("one")
            worker.start("two", "two")
            active.add("two")
            evaluator = subprocess.Popen([sys.executable, "-c", client, "evaluation"])
            assert {generation_hits.get(timeout=5), generation_hits.get(timeout=5)} == {"/one", "/two"}
            assert native_hits.get(timeout=5) == "/evaluation"
            assert events == ["begin"]
            release_generation.set()
            assert evaluator.wait(timeout=5) == 0
            assert {worker.wait_any(), worker.wait_any()} == {
                valkyrie_stage.SlotResult("one", "exited", 0),
                valkyrie_stage.SlotResult("two", "exited", 0),
            }
            active.clear()
            assert events == ["begin", "end"]

            monkeypatch.delenv("VALKYRIE_GENERATION_MODEL_GATEWAY_URL")
            worker.start("three", "native-child")
            active.add("three")
            assert native_hits.get(timeout=5) == "/native-child"
            assert worker.wait_any() == valkyrie_stage.SlotResult("three", "exited", 0)
            active.clear()
            assert events == ["begin", "end"] * 2
        finally:
            release_generation.set()
            if evaluator is not None and evaluator.poll() is None:
                evaluator.terminate()
                evaluator.wait(timeout=5)
            for slot in active:
                worker.stop(slot)
            generation_server.shutdown()
            native_server.shutdown()
            generation_thread.join()
            native_thread.join()
