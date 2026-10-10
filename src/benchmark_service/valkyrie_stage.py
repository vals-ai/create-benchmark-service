"""Standalone stage reporting and episode agent orchestration for sandbox upload."""

from __future__ import annotations

import base64
import ctypes
import hashlib
import hmac
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import threading
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


class StageReporter:
    """Send ordered generation boundaries and wait for Tracker acknowledgments."""

    def __init__(self) -> None:
        self._stage_dir = Path(os.environ["VALKYRIE_STAGE_DIR"])
        self._key = bytes.fromhex((self._stage_dir / "key").read_text())
        self._seq = 0
        self._container: str | None = None

    def begin(self, container: str | None) -> None:
        self._container = container
        self._report("begin", container)

    def end(self) -> None:
        self._report("end", self._container)
        self._container = None

    def _report(self, event: str, container: str | None) -> None:
        self._seq += 1
        payload = json.dumps(
            {"seq": self._seq, "event": event, "container": container},
            separators=(",", ":"),
        ).encode("ascii")
        frame = base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")
        mac = hmac.new(self._key, frame.encode("ascii"), hashlib.sha256).hexdigest()
        print(f"\nVALKYRIE-STAGE/1 {frame} {mac}", file=sys.stdout, flush=True)
        ack = self._stage_dir / "ack" / str(self._seq)
        while not ack.exists():
            time.sleep(0.1)


class Exhausted(Exception):
    """Tracker exhausted the shared generation budget."""


@dataclass(frozen=True)
class SlotResult:
    slot: str
    reason: Literal["exited", "stopped", "exhausted"]
    exit_code: int | None


@dataclass
class _RunningSlot:
    process: subprocess.Popen[bytes]
    container: str | None
    final_output: Path | None


class Agents:
    """Launch selected-agent turns and report the union of running intervals."""

    def __init__(self) -> None:
        self._stage_dir = Path(os.environ["VALKYRIE_STAGE_DIR"])
        config = json.loads((self._stage_dir / "agent.json").read_text())
        self._run_cmd: str = config["run_cmd"]
        self._continue_cmd: str | None = config["continue_cmd"]
        self._interrupt_grace_seconds: float | None = config["interrupt_grace_seconds"]
        self._container_template: str | None = config["container_name"]
        self._final_output_template: str | None = config["final_output"]
        self._parallel_agents: int = config["parallel_agents"]
        self._slots_root = Path(config["slots_root"])
        if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), "PR_SET_CHILD_SUBREAPER")
        self._lock = threading.RLock()
        self._reporter = StageReporter()
        self._active: dict[str, _RunningSlot] = {}
        self._turns: dict[str, int] = {}
        self._finished: deque[SlotResult] = deque()

    def start(self, slot: str, observation_path: str) -> None:
        with self._lock:
            self._start(slot, observation_path)

    def _start(self, slot: str, observation_path: str) -> None:
        self._collect()
        if (self._stage_dir / "exhausted").exists():
            self._exhaust_all()
            raise Exhausted
        if slot in self._active:
            raise ValueError(f"slot {slot!r} is already running")
        if len(self._active) >= self._parallel_agents:
            raise ValueError("parallel_agents limit reached")

        slot_dir = self._slots_root / slot / "agent"
        slot_dir.mkdir(parents=True, exist_ok=True)
        container = self._container_template.replace("{slot}", slot) if self._container_template is not None else None
        if slot in self._turns:
            if self._continue_cmd is None:
                raise ValueError(f"slot {slot!r} has no continue_cmd")
            template = self._continue_cmd
        else:
            template = self._run_cmd
        bindings = {"problem_statement_path": observation_path, "slot_dir": str(slot_dir)}
        if container is not None:
            bindings["container_name"] = container
        command = re.sub(
            "|".join(re.escape("{" + name + "}") for name in bindings),
            lambda match: bindings[match.group()[1:-1]],
            template,
        )
        final_output = (
            Path(self._final_output_template.replace("{slot_dir}", str(slot_dir)))
            if self._final_output_template is not None
            else None
        )
        if not self._active:
            self._reporter.begin(container)
            if (self._stage_dir / "exhausted").exists():
                raise Exhausted
        generation_gateway_url = os.environ.get("VALKYRIE_GENERATION_MODEL_GATEWAY_URL")
        child_env = None
        if generation_gateway_url is not None:
            child_env = os.environ.copy()
            child_env["MODEL_GATEWAY_URL"] = generation_gateway_url
        process = subprocess.Popen(["sh", "-c", command], env=child_env, start_new_session=True)
        self._active[slot] = _RunningSlot(process, container, final_output)
        self._turns[slot] = self._turns.get(slot, 0) + 1

    def stop(self, slot: str) -> SlotResult:
        with self._lock:
            return self._stop(slot)

    def _stop(self, slot: str) -> SlotResult:
        active_at_entry = slot in self._active
        self._collect()
        if slot not in self._active:
            return self._take(slot, active_at_entry)
        running = self._active[slot]
        if self._interrupt_grace_seconds is not None:
            self._signal_group(running.process.pid, signal.SIGINT)
            deadline = time.monotonic() + self._interrupt_grace_seconds
            while time.monotonic() < deadline:
                self._observe_exit(running.process)
                if self._group_empty(running.process.pid):
                    break
                if (self._stage_dir / "exhausted").exists():
                    self._exhaust_all()
                    return self._take(slot, active_at_entry)
                time.sleep(0.1)
        self._complete(slot, "stopped")
        return self._take(slot, active_at_entry)

    def wait_any(self) -> SlotResult:
        while True:
            with self._lock:
                self._collect()
                if self._finished:
                    return self._finished.popleft()
                if not self._active:
                    raise ValueError("no active slots")
            time.sleep(0.1)

    def _collect(self) -> None:
        if (self._stage_dir / "exhausted").exists():
            self._exhaust_all()
        for slot, running in list(self._active.items()):
            if self._observe_exit(running.process):
                self._complete(slot, "exited")

    @staticmethod
    def _observe_exit(process: subprocess.Popen[bytes]) -> bool:
        return os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is not None

    def _exhaust_all(self) -> None:
        for running in self._active.values():
            self._signal_group(running.process.pid, signal.SIGKILL)
        for slot in list(self._active):
            self._complete(slot, "exhausted")

    @staticmethod
    def _signal_group(pid: int, sig: signal.Signals) -> None:
        try:
            os.killpg(pid, sig)
        except ProcessLookupError:
            pass  # An exited group is already absent.

    @staticmethod
    def _group_empty(pgid: int) -> bool:
        empty = True
        for entry in Path("/proc").iterdir():
            if not entry.name.isdecimal():
                continue
            try:
                stat = (entry / "stat").read_text()
            except (FileNotFoundError, ProcessLookupError):
                continue  # Process exited during the scan.
            state, parent, group, _ = stat.rpartition(") ")[2].split(" ", 3)
            if int(group) != pgid:
                continue
            if state == "Z":
                if int(entry.name) == pgid:
                    continue  # Popen owns the direct child's exit status.
                if int(parent) == os.getpid() and os.waitpid(int(entry.name), os.WNOHANG)[0] != 0:
                    continue
            empty = False
        return empty

    def _complete(self, slot: str, reason: Literal["exited", "stopped", "exhausted"]) -> None:
        running = self._active[slot]
        self._signal_group(running.process.pid, signal.SIGKILL)
        while not self._group_empty(running.process.pid):
            time.sleep(0.1)
        exit_code = running.process.wait()

        if running.container is not None:
            subprocess.run(["docker", "rm", "-f", running.container], capture_output=True, check=False)
            listed = subprocess.run(
                ["docker", "container", "ls", "-a", "--format", "{{.Names}}"],
                capture_output=True,
                text=True,
                check=True,
            )
            if running.container in listed.stdout.splitlines():
                raise RuntimeError(f"container {running.container!r} is still present")

        del self._active[slot]
        if not self._active:
            self._reporter.end()

        if running.final_output is not None and running.final_output.exists():
            turn_dir = self._slots_root / slot / "turns" / str(self._turns[slot])
            turn_dir.parent.mkdir(parents=True, exist_ok=True)
            if running.final_output.is_dir():
                shutil.copytree(running.final_output, turn_dir)
            else:
                turn_dir.mkdir()
                shutil.copy2(running.final_output, turn_dir / running.final_output.name)
        self._finished.append(SlotResult(slot, reason, exit_code))

    def _take(self, slot: str, newest: bool) -> SlotResult:
        indices = range(len(self._finished) - 1, -1, -1) if newest else range(len(self._finished))
        for index in indices:
            result = self._finished[index]
            if result.slot == slot:
                del self._finished[index]
                return result
        raise ValueError(f"slot {slot!r} has no completed turn")


def valkyrie_stage_source() -> bytes:
    """Return this standalone module's bytes for upload into a sandbox."""
    return Path(__file__).read_bytes()
