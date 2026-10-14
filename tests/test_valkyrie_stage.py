"""Exercise stage reporting, agent rendering, and process-group lifecycle."""
# pyright: reportPrivateUsage=false
import base64
import ctypes
import errno
import hashlib
import hmac
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
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from benchmark_service import valkyrie_stage


def test_stage_reporter_retransmits_identical_signed_frames_until_ack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stage_dir = tmp_path / "stage"
    stage_dir.mkdir()
    (stage_dir / "ack").mkdir()
    key = bytes.fromhex("11" * 32)
    (stage_dir / "key").write_text(key.hex())
    monkeypatch.setenv("VALKYRIE_STAGE_DIR", str(stage_dir))
    output = io.StringIO()
    monkeypatch.setattr(sys, "stdout", output)
    now = [0.0]
    sleeps = [0]
    event_started = [now[0]]

    def emitted() -> list[str]:
        lines = output.getvalue().splitlines(keepends=True)
        assert len(lines) % 2 == 0
        return ["".join(lines[index : index + 2]) for index in range(0, len(lines), 2)]

    def sleep(seconds: float) -> None:
        frames = emitted()
        assert frames
        if len(frames) in (2, 4):
            resend_interval = now[0] - event_started[0]
            assert resend_interval > 0
            (stage_dir / "ack" / str(len(frames) // 2)).touch()
            now[0] += resend_interval + 1
        sleeps[0] += 1
        assert sleeps[0] < 1000, "reporter never retransmitted the unacknowledged frame"
        now[0] += seconds

    monkeypatch.setattr(valkyrie_stage.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(valkyrie_stage.time, "sleep", sleep)
    reporter = valkyrie_stage.StageReporter()
    reporter.begin("round-one")
    begin_frames = emitted()
    assert len(begin_frames) == 2
    assert begin_frames[0] == begin_frames[1]

    event_started[0] = now[0]
    reporter.end()
    frames = emitted()
    assert len(frames) == 4
    assert frames[2] == frames[3]
    for expected_seq, wire in enumerate((frames[0], frames[2]), start=1):
        assert wire.startswith("\nVALKYRIE-STAGE/1 ") and wire.endswith("\n")
        encoded, mac = wire.removeprefix("\nVALKYRIE-STAGE/1 ").removesuffix("\n").split(" ")
        assert hmac.new(key, encoded.encode("ascii"), hashlib.sha256).hexdigest() == mac
        payload = json.loads(base64.urlsafe_b64decode(encoded + "==="))
        assert payload == {
            "seq": expected_seq,
            "event": "begin" if expected_seq == 1 else "end",
            "container": "round-one",
        }



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
    (stage_dir / "agent.json").write_text(
        json.dumps(
            {
                "run_cmd": "exit 0",
                "continue_cmd": None,
                "interrupt_grace_seconds": None,
                "container_name": None,
                "final_output": None,
                "parallel_agents": 2,
                "slots_root": str(tmp_path / "slots"),
                **config,
            }
        )
    )
    return valkyrie_stage.Agents(), reporter


def test_commands_render_only_bound_tokens_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    slot_root = tmp_path / "{container_name}" / "{slot_dir}"
    observation = tmp_path / "observation-{slot_dir}"
    observation.write_text("observed")
    first = "printf '%s\\n' '{\"json\": {\"ok\": true}}' '{problem_statement_path}' > '{slot_dir}/first'"
    continuation = "printf '%s\\n' '{\"json\": {\"ok\": true}}' '{problem_statement_path}' > '{slot_dir}/next'"
    worker, reporter = agents(
        tmp_path,
        monkeypatch,
        run_cmd=first,
        continue_cmd=continuation,
        slots_root=str(slot_root),
        final_output="{slot_dir}/first",
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
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    docker = tmp_path / "docker"
    docker.write_text(
        '#!/bin/sh\nif [ "$1" = rm ]; then printf \'%s\' "$3" > \'' + str(tmp_path / "removed") + "'; fi\n"
    )
    docker.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    worker, reporter = agents(
        tmp_path,
        monkeypatch,
        container_name="box-{slot}-{slot_dir}",
        run_cmd="printf '%s' '{container_name}' > '{slot_dir}/name'",
    )
    worker.start("one", "observation")
    assert worker.wait_any() == valkyrie_stage.SlotResult("one", "exited", 0)
    assert (tmp_path / "slots" / "one" / "agent" / "name").read_text() == "box-one-{slot_dir}"
    assert (tmp_path / "removed").read_text() == "box-one-{slot_dir}"
    assert reporter.events == [("begin", "box-one-{slot_dir}"), ("end", None)]


def test_natural_exit_retains_code_until_group_cleanup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    gate = tmp_path / "slots" / "one" / "agent" / "go"
    gate.parent.mkdir(parents=True)
    os.mkfifo(gate)
    worker, reporter = agents(
        tmp_path,
        monkeypatch,
        run_cmd=(
            "sleep 30 & printf '%s' \"$!\" > '{slot_dir}/child.pid.tmp'; "
            "mv '{slot_dir}/child.pid.tmp' '{slot_dir}/child.pid'; "
            "read ready < '{slot_dir}/go'; exit 23"
        ),
    )
    child_pid_file = gate.parent / "child.pid"
    child_fd: int | None = None
    child_pid: int | None = None
    absent_at_end: list[bool] = []
    original_end = reporter.end

    def end() -> None:
        assert child_fd is not None
        libc = ctypes.CDLL(None, use_errno=True)
        absent_at_end.append(libc.pidfd_send_signal(child_fd, 0, None, 0) == -1 and ctypes.get_errno() == errno.ESRCH)
        original_end()

    monkeypatch.setattr(reporter, "end", end)
    worker.start("one", "observation")
    owner = worker._active["one"].process
    try:
        deadline = time.monotonic() + 5
        while not child_pid_file.exists():
            assert time.monotonic() < deadline, "group child never reported readiness"
            time.sleep(0.01)
        libc = ctypes.CDLL(None, use_errno=True)
        child_pid = int(child_pid_file.read_text())
        child_fd = libc.pidfd_open(child_pid, 0)
        if child_fd == -1:
            raise OSError(ctypes.get_errno(), "pidfd_open")
        with gate.open("w") as channel:
            channel.write("go\n")
        assert worker.wait_any() == valkyrie_stage.SlotResult("one", "exited", 23)
        assert absent_at_end == [True]
        assert not (Path("/proc") / str(child_pid)).exists()
        assert reporter.events == [("begin", None), ("end", None)]
    finally:
        monkeypatch.setattr(reporter, "end", original_end)
        libc = ctypes.CDLL(None, use_errno=True)
        if child_fd is not None and child_fd >= 0:
            libc.pidfd_send_signal(child_fd, signal.SIGKILL, None, 0)
        try:
            if "one" in worker._active:
                worker.stop("one")
        finally:
            if owner.poll() is None:
                owner.kill()
            owner.wait()
            if child_pid is not None:
                try:
                    os.waitpid(child_pid, 0)
                except ChildProcessError:
                    pass
            if child_fd is not None and child_fd >= 0:
                os.close(child_fd)


def test_parallel_exhaustion_and_graceful_stop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    worker, reporter = agents(
        tmp_path,
        monkeypatch,
        run_cmd="sleep 30",
        interrupt_grace_seconds=0.2,
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


def _wait_for_root_exit(worker: valkyrie_stage.Agents) -> None:
    deadline = time.monotonic() + 5
    while worker._turn_status(worker._active["one"]) == b"A":
        assert time.monotonic() < deadline
        time.sleep(0.01)



@pytest.mark.parametrize("mode", ["natural", "stop", "exhausted"])
def test_detached_child_is_reaped_before_stage_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    slot_dir = tmp_path / "slots" / "one" / "agent"
    slot_dir.mkdir(parents=True)
    gate = slot_dir / "go"
    child_code = (
        "import os, pathlib, signal; "
        "pathlib.Path('child.pid.tmp').write_text(str(os.getpid())); "
        "pathlib.Path('child.pid.tmp').replace('child.pid'); "
        "signal.pause()"
    )
    root_code = (
        "import os, pathlib, subprocess, sys, time\n"
        "pathlib.Path('root.pid.tmp').write_text(str(os.getpid()))\n"
        "pathlib.Path('root.pid.tmp').replace('root.pid')\n"
        f"subprocess.Popen([sys.executable, '-c', {child_code!r}], start_new_session=True)\n"
        "while not pathlib.Path('go').exists(): time.sleep(0.01)\n"
        "sys.exit(23)\n"
    )
    worker, reporter = agents(
        tmp_path,
        monkeypatch,
        run_cmd=f"cd {{slot_dir}} && exec {shlex.quote(sys.executable)} -c {shlex.quote(root_code)}",
    )
    original_end = reporter.end
    child_absent_at_end: list[bool] = []
    child_pid: int | None = None
    child_fd: int | None = None
    root_fd: int | None = None

    def end() -> None:
        assert child_fd is not None
        libc = ctypes.CDLL(None, use_errno=True)
        absent = libc.pidfd_send_signal(child_fd, 0, None, 0) == -1 and ctypes.get_errno() == errno.ESRCH
        child_absent_at_end.append(absent)
        original_end()

    monkeypatch.setattr(reporter, "end", end)
    worker.start("one", "observation")
    owner = worker._active["one"].process
    try:
        deadline = time.monotonic() + 5
        root_pid_file = slot_dir / "root.pid"
        child_pid_file = slot_dir / "child.pid"
        while not root_pid_file.exists():
            assert time.monotonic() < deadline, "root never reported readiness"
            time.sleep(0.01)
        libc = ctypes.CDLL(None, use_errno=True)
        root_fd = libc.pidfd_open(int(root_pid_file.read_text()), 0)
        if root_fd == -1:
            raise OSError(ctypes.get_errno(), "pidfd_open")
        while not child_pid_file.exists():
            assert time.monotonic() < deadline, "detached child never reported readiness"
            time.sleep(0.01)
        child_pid = int(child_pid_file.read_text())
        child_fd = libc.pidfd_open(child_pid, 0)
        if child_fd == -1:
            raise OSError(ctypes.get_errno(), "pidfd_open")
        if mode == "natural":
            gate.touch()
            assert worker.wait_any() == valkyrie_stage.SlotResult("one", "exited", 23)
        elif mode == "stop":
            assert worker.stop("one").reason == "stopped"
        else:
            (tmp_path / "stage" / "exhausted").touch()
            assert worker.wait_any().reason == "exhausted"
        assert child_absent_at_end == [True]
        assert reporter.events == [("begin", None), ("end", None)]
    finally:
        monkeypatch.setattr(reporter, "end", original_end)
        libc = ctypes.CDLL(None, use_errno=True)
        if child_fd is not None and child_fd >= 0:
            libc.pidfd_send_signal(child_fd, signal.SIGKILL, None, 0)
        if root_fd is not None and root_fd >= 0:
            libc.pidfd_send_signal(root_fd, signal.SIGKILL, None, 0)
        try:
            if "one" in worker._active:
                worker.stop("one")
        finally:
            if owner.poll() is None:
                owner.kill()
            owner.wait()
            if child_pid is not None:
                try:
                    os.waitpid(child_pid, 0)
                except ChildProcessError:
                    pass
            if child_fd is not None and child_fd >= 0:
                os.close(child_fd)
            if root_fd is not None and root_fd >= 0:
                os.close(root_fd)




def test_sigint_grace_waits_for_child_after_root_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    slot_dir = tmp_path / "slots" / "one" / "agent"
    slot_dir.mkdir(parents=True)
    gate = slot_dir / "grace-go"
    child_code = (
        "import os, pathlib, signal, time\n"
        "parent = os.getppid()\n"
        "def interrupt(sig, frame):\n"
        "    pathlib.Path('interrupted').touch()\n"
        "    while os.getppid() == parent: time.sleep(0.01)\n"
        "    pathlib.Path('root-exited').touch()\n"
        "    while not pathlib.Path('grace-go').exists(): time.sleep(0.01)\n"
        "    pathlib.Path('grace-complete').touch()\n"
        "    os._exit(0)\n"
        "signal.signal(signal.SIGINT, interrupt)\n"
        "pathlib.Path('ready.tmp').write_text(str(os.getpid()))\n"
        "pathlib.Path('ready.tmp').replace('ready')\n"
        "signal.pause()\n"
    )
    root_code = (
        "import os, pathlib, signal, subprocess, sys\n"
        "pathlib.Path('root.pid.tmp').write_text(str(os.getpid()))\n"
        "pathlib.Path('root.pid.tmp').replace('root.pid')\n"
        "signal.signal(signal.SIGINT, lambda sig, frame: os._exit(22))\n"
        f"subprocess.Popen([sys.executable, '-c', {child_code!r}])\n"
        "signal.pause()\n"
    )
    worker, reporter = agents(
        tmp_path,
        monkeypatch,
        run_cmd=f"cd {{slot_dir}} && exec {shlex.quote(sys.executable)} -c {shlex.quote(root_code)}",
        interrupt_grace_seconds=3.0,
    )
    end_observed: list[bool] = []
    original_end = reporter.end

    def end() -> None:
        end_observed.append((slot_dir / "grace-complete").exists())
        original_end()

    monkeypatch.setattr(reporter, "end", end)
    worker.start("one", "observation")
    owner = worker._active["one"].process
    child_pid: int | None = None
    child_fd: int | None = None
    root_fd: int | None = None
    stop_result: list[valkyrie_stage.SlotResult] = []
    stop_failure: list[BaseException] = []

    def stopping() -> None:
        try:
            stop_result.append(worker.stop("one"))
        except BaseException as exc:
            stop_failure.append(exc)

    stopper: threading.Thread | None = None
    try:
        ready = slot_dir / "ready"
        root_pid_file = slot_dir / "root.pid"
        deadline = time.monotonic() + 5
        while not root_pid_file.exists():
            assert time.monotonic() < deadline, "root never reported readiness"
            time.sleep(0.01)
        libc = ctypes.CDLL(None, use_errno=True)
        root_fd = libc.pidfd_open(int(root_pid_file.read_text()), 0)
        if root_fd == -1:
            raise OSError(ctypes.get_errno(), "pidfd_open")
        while not ready.exists():
            assert time.monotonic() < deadline, "grace child never reported readiness"
            time.sleep(0.01)
        child_pid = int(ready.read_text())
        child_fd = libc.pidfd_open(child_pid, 0)
        if child_fd == -1:
            raise OSError(ctypes.get_errno(), "pidfd_open")
        stopper = threading.Thread(target=stopping, daemon=True)
        stopper.start()
        while not (slot_dir / "root-exited").exists():
            assert time.monotonic() < deadline, "child did not observe root exit after SIGINT"
            time.sleep(0.01)
        assert (slot_dir / "interrupted").exists()
        gate.touch()
        stopper.join(timeout=5)
        assert not stopper.is_alive(), "stop did not finish after graceful child exit"
        assert not stop_failure, stop_failure
        assert stop_result == [valkyrie_stage.SlotResult("one", "stopped", 22)]
        assert end_observed == [True]
        assert reporter.events == [("begin", None), ("end", None)]
    finally:
        monkeypatch.setattr(reporter, "end", original_end)
        libc = ctypes.CDLL(None, use_errno=True)
        if child_fd is not None and child_fd >= 0:
            libc.pidfd_send_signal(child_fd, signal.SIGKILL, None, 0)
        if root_fd is not None and root_fd >= 0:
            libc.pidfd_send_signal(root_fd, signal.SIGKILL, None, 0)
        try:
            if stopper is not None:
                stopper.join(timeout=5)
            elif "one" in worker._active:
                worker.stop("one")
        finally:
            if owner.poll() is None:
                owner.kill()
            owner.wait()
            try:
                if stopper is not None:
                    stopper.join(timeout=5)
                    assert not stopper.is_alive(), "stop thread remained active after owner teardown"
            finally:
                if child_pid is not None:
                    try:
                        os.waitpid(child_pid, 0)
                    except ChildProcessError:
                        pass
                if child_fd is not None and child_fd >= 0:
                    os.close(child_fd)
                if root_fd is not None and root_fd >= 0:
                    os.close(root_fd)

@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        ("live", valkyrie_stage.SlotResult("one", "stopped", -9)),
        ("exited", valkyrie_stage.SlotResult("one", "exited", 22)),
        ("exhausted", valkyrie_stage.SlotResult("one", "exhausted", -9)),
        ("grace_exhausted", valkyrie_stage.SlotResult("one", "exhausted", -9)),
    ],
)
def test_stop_returns_current_turn_without_consuming_older_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str, expected: valkyrie_stage.SlotResult
) -> None:
    ready = tmp_path / "slots" / "one" / "agent" / "ready"
    command = "exit 22" if mode == "exited" else f"trap '' INT; touch {shlex.quote(str(ready))}; exec sleep 30"
    worker, _ = agents(
        tmp_path,
        monkeypatch,
        run_cmd="exit 11",
        continue_cmd=command,
        interrupt_grace_seconds=1.0 if mode == "grace_exhausted" else None,
    )
    worker.start("one", "observation")
    _wait_for_root_exit(worker)
    worker.start("one", "observation")
    try:
        if mode != "exited":
            deadline = time.monotonic() + 5
            while not ready.exists():
                assert time.monotonic() < deadline
                time.sleep(0.01)
        if mode == "exited":
            _wait_for_root_exit(worker)
        elif mode == "exhausted":
            (tmp_path / "stage" / "exhausted").touch()
        elif mode == "grace_exhausted":
            group_empty = worker._group_empty

            def exhaust_during_grace(running: valkyrie_stage._RunningSlot) -> bool:
                empty = group_empty(running)
                (tmp_path / "stage" / "exhausted").touch()
                return empty

            monkeypatch.setattr(worker, "_group_empty", exhaust_during_grace)
        result = worker.stop("one")
        assert result == expected
        assert worker.wait_any() == valkyrie_stage.SlotResult("one", "exited", 11)
    finally:
        if "one" in worker._active:
            worker.stop("one")


def test_stop_selects_current_turn_when_outcomes_are_equal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    worker, _ = agents(tmp_path, monkeypatch, run_cmd="exit 11", continue_cmd="exit 11")
    worker.start("one", "observation")
    _wait_for_root_exit(worker)
    worker.start("one", "observation")
    older = worker._finished[0]
    _wait_for_root_exit(worker)
    current = worker.stop("one")
    assert current == older
    assert current is not older
    assert worker.wait_any() is older


def test_stop_of_inactive_slot_preserves_fifo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    worker, _ = agents(tmp_path, monkeypatch, run_cmd="exit 11", continue_cmd="exit 22")
    worker.start("one", "observation")
    _wait_for_root_exit(worker)
    worker.start("one", "observation")
    _wait_for_root_exit(worker)
    worker._collect()
    assert worker.stop("one") == valkyrie_stage.SlotResult("one", "exited", 11)
    assert worker.wait_any() == valkyrie_stage.SlotResult("one", "exited", 22)


def test_start_exhausts_active_group_when_marker_arrives_after_collection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ready = tmp_path / "slots" / "one" / "agent" / "root.pid"
    command = (
        f"echo $$ > {shlex.quote(str(ready))}.tmp; "
        f"mv {shlex.quote(str(ready))}.tmp {shlex.quote(str(ready))}; exec sleep 30"
    )
    worker, reporter = agents(tmp_path, monkeypatch, run_cmd=command)
    worker.start("one", "observation")
    owner = worker._active["one"].process
    root_fd: int | None = None
    try:
        deadline = time.monotonic() + 5
        while not ready.exists():
            assert time.monotonic() < deadline, "root never reported readiness"
            time.sleep(0.01)
        libc = ctypes.CDLL(None, use_errno=True)
        root_fd = libc.pidfd_open(int(ready.read_text()), 0)
        if root_fd == -1:
            raise OSError(ctypes.get_errno(), "pidfd_open")
        collect = worker._collect

        def collect_then_exhaust() -> None:
            collect()
            (tmp_path / "stage" / "exhausted").touch()

        with monkeypatch.context() as patch:
            patch.setattr(worker, "_collect", collect_then_exhaust)
            with pytest.raises(valkyrie_stage.Exhausted):
                worker.start("two", "observation")
        assert libc.pidfd_send_signal(root_fd, 0, None, 0) == -1
        assert ctypes.get_errno() == errno.ESRCH
        assert reporter.events == [("begin", None), ("end", None)]
        assert worker.wait_any() == valkyrie_stage.SlotResult("one", "exhausted", -9)
    finally:
        if root_fd is not None and root_fd >= 0:
            ctypes.CDLL(None, use_errno=True).pidfd_send_signal(root_fd, signal.SIGKILL, None, 0)
        try:
            for slot in list(worker._active):
                worker.stop(slot)
        finally:
            if owner.poll() is None:
                owner.kill()
            owner.wait()
            if root_fd is not None and root_fd >= 0:
                os.close(root_fd)


def test_generation_children_use_generation_gateway_while_evaluation_uses_native(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
                tmp_path,
                monkeypatch,
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
            try:
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
                for slot in active:
                    worker.stop(slot)
        finally:
            release_generation.set()
            if evaluator is not None and evaluator.poll() is None:
                evaluator.terminate()
                evaluator.wait(timeout=5)
            generation_server.shutdown()
            native_server.shutdown()
            generation_thread.join()
            native_thread.join()
