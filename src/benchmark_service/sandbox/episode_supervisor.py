"""Linux process-group owner for an episode, uploaded before native launch."""
from __future__ import annotations

import argparse
import ctypes
import os
import select
import signal
import subprocess
import sys
import time

PR_SET_CHILD_SUBREAPER = 36


def children() -> list[int]:
    with open(f"/proc/self/task/{os.getpid()}/children") as stream:
        return [int(pid) for pid in stream.read().split()]


def drain(child: subprocess.Popen[bytes] | None) -> None:
    root_pid = child.pid if child is not None else None
    while True:
        owned = [pid for pid in children() if pid != root_pid]
        if not owned:
            break
        for pid in owned:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        for pid in owned:
            try:
                os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                pass
        time.sleep(0.01)


def finish(directory: str, status: int) -> int:
    with open(os.path.join(directory, "status"), "w") as stream:
        stream.write(f"{status}\n")
    # The shell owner terminates its own group after status publication. The
    # Python owner does the same so native transport completion stays 137.
    os.killpg(os.getpgrp(), signal.SIGKILL)
    return status


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory")
    parser.add_argument("command")
    args = parser.parse_args()
    if ctypes.CDLL(None, use_errno=True).prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "PR_SET_CHILD_SUBREAPER")
    # The exec-replaced group leader inherited held FIFO FD 3 and its PGID.
    control = 3
    os.unlink(os.path.join(args.directory, "status"))
    try:
        child = subprocess.Popen(["/bin/sh", "-c", args.command], start_new_session=True)
    except OSError as exc:
        print(f"episode launch failed: {exc}", file=sys.stderr, flush=True)
        drain(None)
        return finish(args.directory, 127)
    while True:
        exited = os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOWAIT | os.WNOHANG)
        if exited is not None:
            drain(child)
            returncode = child.wait()
            code = returncode if returncode >= 0 else 128 - returncode
            return finish(args.directory, code)
        readable, _, _ = select.select([control], [], [], 0.05)
        if readable and os.read(control, 4096):
            os.kill(child.pid, signal.SIGKILL)
            os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOWAIT)
            drain(child)
            child.wait()
            return finish(args.directory, 137)


if __name__ == "__main__":
    sys.exit(main())
