"""In-MicroVM sandbox agent: Lambda lifecycle hooks plus exec and file routes.

Copied into every MicroVM image and started as its CMD. Standard library only, Python 3.9+,
so it runs on stock Amazon Linux 2023. Hooks listen on HOOK_PORT, which auth tokens exclude;
callers reach APP_PORT through the MicroVM endpoint.
"""

from __future__ import annotations

import json
import os
import queue
import shutil
import signal
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

APP_PORT = 8080
HOOK_PORT = 9000
HOOK_PREFIX = "/aws/lambda-microvms/runtime/v1/"
TIMEOUT_EXIT_CODE = 124
HEARTBEAT_SECONDS = 15.0
_CHUNK_BYTES = 64 * 1024
_POLL_SECONDS = 0.1

_state_lock = threading.Lock()
_state: dict[str, Any] = {"name": "", "labels": {}, "env_vars": {}, "microvm_id": "", "created_at": ""}


def _shell() -> list[str]:
    return [shutil.which("bash") or "/bin/sh", "-c"]


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:
        pass

    def _body(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length else b""

    def _json(self, status: int, payload: dict[str, Any]) -> None:
        data = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _chunk(self, data: bytes) -> None:
        self.wfile.write(f"{len(data):X}\r\n".encode() + data + b"\r\n")
        self.wfile.flush()


class HookHandler(_Handler):
    def do_POST(self) -> None:
        body = self._body()
        hook = self.path.removeprefix(HOOK_PREFIX) if self.path.startswith(HOOK_PREFIX) else ""
        if hook == "run":
            request = json.loads(body or b"{}")
            payload = json.loads(request.get("runHookPayload") or "{}")
            with _state_lock:
                _state.update(
                    name=str(payload.get("name") or ""),
                    labels=dict(payload.get("labels") or {}),
                    env_vars=dict(payload.get("env_vars") or {}),
                    created_at=str(payload.get("created_at") or ""),
                    microvm_id=str(request.get("microvmId") or ""),
                )
        if hook in ("suspend", "terminate"):
            os.sync()
        if hook in ("ready", "validate", "run", "resume", "suspend", "terminate"):
            self._json(200, {"ok": True})
        else:
            self._json(404, {"error": "unknown hook"})


class AppHandler(_Handler):
    def do_GET(self) -> None:
        url = urlparse(self.path)
        if url.path == "/health":
            self._json(200, {"ok": True})
        elif url.path == "/info":
            with _state_lock:
                info = {key: _state[key] for key in ("name", "labels", "microvm_id", "created_at")}
            self._json(200, info)
        elif url.path == "/files":
            self._download(_query_path(url.query))
        else:
            self._json(404, {"error": "not found"})

    def do_PUT(self) -> None:
        url = urlparse(self.path)
        if url.path != "/files":
            self._json(404, {"error": "not found"})
            return
        path = _query_path(url.query)
        if not path:
            self._json(400, {"error": "path is required"})
            return
        data = self._body()
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            with open(path, "wb") as handle:
                handle.write(data)
        except OSError as error:
            self._json(400, {"error": f"{type(error).__name__}: {error}"})
            return
        self._json(200, {"bytes": len(data)})

    def do_POST(self) -> None:
        if urlparse(self.path).path != "/exec":
            self._json(404, {"error": "not found"})
            return
        request = json.loads(self._body() or b"{}")
        self._exec(request)

    def _download(self, path: str) -> None:
        if not path or not os.path.isfile(path):
            self._json(404, {"error": f"no such file: {path}"})
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(os.path.getsize(path)))
        self.end_headers()
        with open(path, "rb") as handle:
            shutil.copyfileobj(handle, self.wfile, _CHUNK_BYTES)

    def _exec(self, request: dict[str, Any]) -> None:
        with _state_lock:
            env = {**os.environ, **_state["env_vars"]}
        extra: dict[str, Any] = request.get("env") or {}
        env.update({str(key): str(value) for key, value in extra.items()})
        timeout = request.get("timeout")
        try:
            process = subprocess.Popen(
                [*_shell(), str(request["command"])],
                cwd=request.get("cwd") or None,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except (OSError, KeyError) as error:
            self._json(400, {"error": f"{type(error).__name__}: {error}"})
            return

        self.send_response(200)
        self.send_header("Content-Type", "application/x-ndjson")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()

        chunks: queue.Queue[bytes | None] = queue.Queue()

        def pump() -> None:
            assert process.stdout is not None
            fd = process.stdout.fileno()
            while data := os.read(fd, _CHUNK_BYTES):
                chunks.put(data)
            chunks.put(None)

        threading.Thread(target=pump, daemon=True).start()
        deadline = time.monotonic() + float(timeout) if timeout else None
        last_sent = time.monotonic()
        exit_code: int | None = None
        output_open = True
        try:
            while output_open or process.poll() is None:
                wait = HEARTBEAT_SECONDS
                if deadline is not None:
                    wait = min(wait, deadline - time.monotonic())
                if wait <= 0:
                    _kill(process)
                    exit_code = TIMEOUT_EXIT_CODE
                    break
                data = b""
                if output_open:
                    try:
                        chunk = chunks.get(timeout=wait)
                    except queue.Empty:
                        chunk = b""
                    if chunk is None:
                        output_open = False
                        continue
                    data = chunk
                else:
                    time.sleep(min(wait, _POLL_SECONDS))
                if data:
                    self._chunk((json.dumps({"output": data.decode("utf-8", errors="replace")}) + "\n").encode())
                    last_sent = time.monotonic()
                elif time.monotonic() - last_sent >= HEARTBEAT_SECONDS:
                    self._chunk(b'{"heartbeat":true}\n')
                    last_sent = time.monotonic()
            if exit_code is None:
                returncode = process.wait()
                exit_code = 128 - returncode if returncode < 0 else returncode
            self._chunk((json.dumps({"exit_code": exit_code}) + "\n").encode())
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            _kill(process)


def _kill(process: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()


def _query_path(query: str) -> str:
    values = parse_qs(query).get("path") or [""]
    return values[0]


def serve(app_port: int = APP_PORT, hook_port: int = HOOK_PORT) -> tuple[ThreadingHTTPServer, ThreadingHTTPServer]:
    app = ThreadingHTTPServer(("0.0.0.0", app_port), AppHandler)
    hooks = ThreadingHTTPServer(("0.0.0.0", hook_port), HookHandler)
    for server in (app, hooks):
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
    return app, hooks


if __name__ == "__main__":
    serve(int(os.environ.get("CBS_AGENT_APP_PORT", APP_PORT)), int(os.environ.get("CBS_AGENT_HOOK_PORT", HOOK_PORT)))
    threading.Event().wait()
