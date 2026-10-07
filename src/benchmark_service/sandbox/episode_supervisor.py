"""Standalone Linux/Python 3.8 episode owner, uploaded into a sandbox."""

from __future__ import annotations

import argparse
import ctypes
import os
import select
import signal
import socket
import subprocess
import sys
import time


PR_SET_CHILD_SUBREAPER = 36



def publish(directory: str, name: str, value: str) -> None:
    with open(os.path.join(directory, name), "w") as stream:
        stream.write(value)


def children() -> list[int]:
    path = "/proc/self/task/{}/children".format(os.getpid())
    with open(path) as stream:
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory")
    parser.add_argument("command")
    args = parser.parse_args()
    if ctypes.CDLL(None, use_errno=True).prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "PR_SET_CHILD_SUBREAPER")
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(os.path.join(args.directory, "control"))
    listener.listen(1)
    publish(args.directory, "READY", "ready\n")
    try:
        child = subprocess.Popen(["/bin/sh", "-c", args.command], start_new_session=True)
    except OSError as exc:
        print("episode launch failed: {}".format(exc), file=sys.stderr, flush=True)
        drain(None)
        publish(args.directory, "DRAINED", "drained\n")
        return 127
    while True:
        status = os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOWAIT | os.WNOHANG)
        if status is not None:
            drain(child)
            returncode = child.wait()
            code = returncode if returncode >= 0 else 128 - returncode
            publish(args.directory, "DRAINED", "drained\n")
            return code
        readable, _, _ = select.select([listener], [], [], 0.05)
        if readable:
            connection, _ = listener.accept()
            with connection:
                if connection.recv(4) == b"STOP":
                    if os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOWAIT | os.WNOHANG) is None:
                        os.kill(child.pid, signal.SIGKILL)
                        os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOWAIT)
                    drain(child)
                    child.wait()
                    publish(args.directory, "DRAINED", "drained\n")
                    connection.sendall(b"DRAINED")
                    return 137


if __name__ == "__main__":
    sys.exit(main())
