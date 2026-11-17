"""Command and file agent that runs inside an ECS Nitro sandbox task.

Stdlib only: the task image may have no Python, so the agent image ships a static
interpreter that runs this file. The provider reaches it on the task ENI.

Protocol (every request needs ``Authorization: Bearer <token>``):

- ``GET /health`` returns 200.
- ``POST /exec`` takes ``{"command", "cwd", "env", "timeout"}`` and streams
  NDJSON frames: ``{"output": <base64>}`` then one ``{"exit_code": n}`` or ``{"error": msg}``.
- ``PUT /files?path=`` writes the body atomically, creating parent directories.
- ``GET /files?path=`` streams the file, 404 when it is missing.
"""

from __future__ import annotations

import base64
import hmac
import json
import os
import selectors
import signal
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

TOKEN_ENV = "CBS_AGENT_TOKEN"
PORT_ENV = "CBS_AGENT_PORT"
IDLE_TIMEOUT_ENV = "CBS_AGENT_IDLE_TIMEOUT"
AGENT_ENV_NAMES = (TOKEN_ENV, PORT_ENV, IDLE_TIMEOUT_ENV)
TIMEOUT_EXIT_CODE = 124
_EXIT_POLL_SECONDS = 0.2
_CHUNK = 1024 * 1024


class _State:
    def __init__(self, token: str) -> None:
        self.token = token
        self.lock = threading.Lock()
        self.active = 0
        self.last_activity = time.monotonic()

    def begin(self) -> None:
        with self.lock:
            self.active += 1
            self.last_activity = time.monotonic()

    def end(self) -> None:
        with self.lock:
            self.active -= 1
            self.last_activity = time.monotonic()

    def idle_for(self) -> float:
        with self.lock:
            return 0.0 if self.active else time.monotonic() - self.last_activity


def _child_env(extra: dict[str, str]) -> dict[str, str]:
    env = {key: value for key, value in os.environ.items() if key not in AGENT_ENV_NAMES}
    env.update(extra)
    return env


def _kill_group(process: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()


class _Handler(BaseHTTPRequestHandler):
    server_version = "cbs-sandbox-agent"
    state: _State

    def log_message(self, format: str, *args: object) -> None:
        return

    def _authorized(self) -> bool:
        header = self.headers.get("Authorization", "")
        expected = f"Bearer {self.state.token}"
        if hmac.compare_digest(header.encode(), expected.encode()):
            return True
        self._reply(401, {"error": "unauthorized"})
        return False

    def _reply(self, status: int, body: dict[str, object]) -> None:
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _path_param(self) -> str | None:
        values = parse_qs(urlparse(self.path).query).get("path")
        if not values or not values[0].startswith("/"):
            self._reply(400, {"error": "path must be an absolute path"})
            return None
        return values[0]

    def _route(self) -> str:
        return urlparse(self.path).path

    def _handle(self, method: str) -> None:
        if not self._authorized():
            return
        self.state.begin()
        try:
            route = self._route()
            if method == "GET" and route == "/health":
                self._reply(200, {"ok": True})
            elif method == "POST" and route == "/exec":
                self._exec()
            elif method == "PUT" and route == "/files":
                self._upload()
            elif method == "GET" and route == "/files":
                self._download()
            else:
                self._reply(404, {"error": f"no route {method} {route}"})
        finally:
            self.state.end()

    def do_GET(self) -> None:
        self._handle("GET")

    def do_POST(self) -> None:
        self._handle("POST")

    def do_PUT(self) -> None:
        self._handle("PUT")

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length else b""

    def _frame(self, frame: dict[str, object]) -> None:
        self.wfile.write(json.dumps(frame).encode() + b"\n")
        self.wfile.flush()

    def _exec(self) -> None:
        try:
            request: dict[str, Any] = json.loads(self._read_body() or b"{}")
            command = request["command"]
            if not isinstance(command, str):
                raise TypeError("command must be a string")
            cwd = request.get("cwd")
            raw_env: dict[str, Any] = request.get("env") or {}
            env = {str(key): str(value) for key, value in raw_env.items()}
            timeout = request.get("timeout")
        except (ValueError, KeyError, TypeError, AttributeError) as error:
            self._reply(400, {"error": f"invalid exec request: {error}"})
            return

        self.send_response(200)
        self.send_header("Content-Type", "application/x-ndjson")
        self.send_header("Connection", "close")
        self.end_headers()
        try:
            process = subprocess.Popen(
                ["/bin/sh", "-c", command],
                cwd=cwd or None,
                env=_child_env(env),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except OSError as error:
            self._frame({"error": f"failed to start command: {error}"})
            return

        assert process.stdout is not None
        deadline = None if timeout is None else time.monotonic() + float(timeout)
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        selector.register(self.connection, selectors.EVENT_READ)
        output_open = True
        try:
            while output_open or process.poll() is None:
                wait = None if deadline is None else deadline - time.monotonic()
                if wait is not None and wait <= 0:
                    _kill_group(process)
                    self._frame({"exit_code": TIMEOUT_EXIT_CODE})
                    return
                if not output_open:
                    wait = _EXIT_POLL_SECONDS if wait is None else min(wait, _EXIT_POLL_SECONDS)
                for key, _ in selector.select(wait):
                    if key.fileobj is self.connection:
                        # The request body is fully read, so a readable socket means the client went away.
                        _kill_group(process)
                        return
                    chunk = os.read(process.stdout.fileno(), 65536)
                    if chunk:
                        self._frame({"output": base64.b64encode(chunk).decode()})
                    else:
                        output_open = False
                        selector.unregister(process.stdout)
            self._frame({"exit_code": process.wait()})
        except (BrokenPipeError, ConnectionResetError):
            _kill_group(process)
        finally:
            selector.close()
            process.stdout.close()
            if process.poll() is None:
                _kill_group(process)

    def _upload(self) -> None:
        path = self._path_param()
        if path is None:
            return
        directory = os.path.dirname(path) or "/"
        try:
            os.makedirs(directory, exist_ok=True)
            fd, temp_path = tempfile.mkstemp(dir=directory, prefix=".cbs-upload-")
            try:
                remaining = int(self.headers.get("Content-Length") or 0)
                with os.fdopen(fd, "wb") as handle:
                    while remaining:
                        chunk = self.rfile.read(min(_CHUNK, remaining))
                        if not chunk:
                            raise ConnectionError("upload body ended early")
                        handle.write(chunk)
                        remaining -= len(chunk)
                os.chmod(temp_path, 0o644)
                os.replace(temp_path, path)
            except BaseException:
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
                raise
        except (OSError, ConnectionError) as error:
            self._reply(500, {"error": f"upload to {path} failed: {error}"})
            return
        self._reply(200, {"ok": True})

    def _download(self) -> None:
        path = self._path_param()
        if path is None:
            return
        try:
            handle = open(path, "rb")
        except FileNotFoundError:
            self._reply(404, {"error": f"{path} not found"})
            return
        except IsADirectoryError:
            self._reply(400, {"error": f"{path} is a directory"})
            return
        except OSError as error:
            self._reply(500, {"error": f"open {path} failed: {error}"})
            return
        with handle:
            size = os.fstat(handle.fileno()).st_size
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(size))
            self.end_headers()
            while chunk := handle.read(_CHUNK):
                self.wfile.write(chunk)


def _watch_idle(state: _State, idle_timeout: float, server: ThreadingHTTPServer) -> None:
    while True:
        time.sleep(min(30.0, idle_timeout))
        if state.idle_for() >= idle_timeout:
            server.shutdown()
            return


def serve(token: str, port: int, idle_timeout: float = 0, host: str = "0.0.0.0") -> None:
    state = _State(token)
    handler = type("Handler", (_Handler,), {"state": state})
    server = ThreadingHTTPServer((host, port), handler)
    server.daemon_threads = True
    if idle_timeout > 0:
        threading.Thread(target=_watch_idle, args=(state, idle_timeout, server), daemon=True).start()
    server.serve_forever()


def main() -> int:
    token = os.environ.get(TOKEN_ENV, "")
    if not token:
        print(f"{TOKEN_ENV} is required", file=sys.stderr)
        return 2
    port = int(os.environ.get(PORT_ENV) or 7070)
    idle_timeout = float(os.environ.get(IDLE_TIMEOUT_ENV) or 0)
    serve(token, port, idle_timeout)
    return 0


if __name__ == "__main__":
    sys.exit(main())
