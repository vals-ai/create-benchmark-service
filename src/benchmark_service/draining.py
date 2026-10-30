"""Admission and completion accounting shared by workers in one container.

Create the store once before starting workers. Never reset it while any worker
can still own work. A deployment controller must bind acknowledgments to both
the task identity and store_id, and to the requested control revision.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from uuid import uuid4

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from benchmark_service.blocking import run_blocking

DRAIN_STATE_PATH_ENV = "BENCHMARK_SERVICE_DRAIN_STATE_PATH"
DRAIN_HEADER = "X-Benchmark-Service-Draining"


class _Headers(Protocol):
    def get(self, key: str, /) -> object: ...


def is_drain_rejection(status: int, headers: _Headers) -> bool:
    """Recognize a rejection that guarantees benchmark work did not start."""
    return status == 503 and headers.get(DRAIN_HEADER) == "1"


@dataclass(frozen=True)
class DrainSnapshot:
    store_id: str
    revision: int
    accepting: bool
    in_flight: int


class DrainState:
    """Open an existing task-local store; missing or replaced stores are errors."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).resolve()
        connection = sqlite3.connect(self._uri, uri=True)
        try:
            row = connection.execute("SELECT store_id FROM control WHERE id = 1").fetchone()
            if row is None:
                raise RuntimeError("Drain store is not initialized")
            self.store_id: str = str(row[0])
        finally:
            connection.close()

    @property
    def _uri(self) -> str:
        return self.path.as_uri() + "?mode=rw"

    @classmethod
    def create(cls, path: str | Path) -> DrainState:
        """Create once, before workers start. Admission remains closed until configured."""
        target = Path(path).resolve()
        with target.open("xb"):
            pass
        connection = sqlite3.connect(target.as_uri() + "?mode=rw", uri=True)
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            with connection:
                connection.execute(
                    "CREATE TABLE control (id INTEGER PRIMARY KEY CHECK (id = 1), "
                    "store_id TEXT NOT NULL, revision INTEGER NOT NULL, "
                    "accepting INTEGER NOT NULL CHECK (accepting IN (0, 1)))"
                )
                connection.execute("INSERT INTO control VALUES (1, ?, -1, 0)", (str(uuid4()),))
                connection.execute("CREATE TABLE requests (id TEXT PRIMARY KEY)")
        finally:
            connection.close()
        return cls(target)

    @contextmanager
    def _transaction(self) -> Iterator[tuple[sqlite3.Connection, sqlite3.Row]]:
        connection = sqlite3.connect(self._uri, uri=True, timeout=1)
        connection.row_factory = sqlite3.Row
        try:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute("SELECT store_id, revision, accepting FROM control WHERE id = 1").fetchone()
                if row is None or row["store_id"] != self.store_id:
                    raise RuntimeError("Drain store identity changed; completion is unknown")
                yield connection, row
        finally:
            connection.close()

    def set_admission(self, *, accepting: bool, revision: int) -> DrainSnapshot:
        """Apply a monotonic control command to this store incarnation."""
        if revision < 0:
            raise ValueError("Drain control revision must be nonnegative")
        with self._transaction() as (connection, row):
            current_revision, current_accepting = int(row["revision"]), bool(row["accepting"])
            if revision == current_revision and accepting != current_accepting:
                raise ValueError("Conflicting drain commands at the same revision")
            if revision > current_revision:
                connection.execute("UPDATE control SET revision = ?, accepting = ? WHERE id = 1", (revision, accepting))
                current_revision, current_accepting = revision, accepting
            return self._snapshot(connection, current_revision, current_accepting)

    def admit(self, request_id: str) -> bool:
        """Atomically check admission and take ownership of one request."""
        with self._transaction() as (connection, row):
            if not row["accepting"]:
                return False
            connection.execute("INSERT INTO requests VALUES (?)", (request_id,))
            return True

    def complete(self, request_id: str) -> None:
        """Release an owned request. Repeated completion cannot lower another count."""
        with self._transaction() as (connection, _):
            connection.execute("DELETE FROM requests WHERE id = ?", (request_id,))

    def snapshot(self) -> DrainSnapshot:
        with self._transaction() as (connection, row):
            return self._snapshot(connection, int(row["revision"]), bool(row["accepting"]))

    def _snapshot(self, connection: sqlite3.Connection, revision: int, accepting: bool) -> DrainSnapshot:
        count = connection.execute("SELECT COUNT(*) FROM requests").fetchone()
        assert count is not None
        return DrainSnapshot(self.store_id, revision, accepting, int(count[0]))


class DrainMiddleware:
    """Reject new work before dispatch; leave accepted scopes and health checks alone."""

    def __init__(self, app: ASGIApp, state: DrainState) -> None:
        self.app = app
        self.state = state

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in {"http", "websocket"} or scope.get("path") == "/health":
            await self.app(scope, receive, send)
            return
        request_id = str(uuid4())
        try:
            # Cleanup starts before this await: cancellation can arrive after
            # the worker committed admission but before its result is returned.
            if not await run_blocking(lambda: self.state.admit(request_id)):
                await self._reject(scope, receive, send)
                return
            await self.app(scope, receive, send)
        finally:
            await run_blocking(lambda: self.state.complete(request_id))

    async def _reject(self, scope: Scope, receive: Receive, send: Send) -> None:
        response = JSONResponse(
            {"detail": "Service is draining; this request was not started"},
            status_code=503,
            headers={DRAIN_HEADER: "1", "Retry-After": "1", "Connection": "close"},
        )
        if scope["type"] == "http":
            await response(scope, receive, send)
        elif "websocket.http.response" in scope.get("extensions", {}):
            await send({"type": "websocket.http.response.start", "status": 503, "headers": response.raw_headers})
            await send({"type": "websocket.http.response.body", "body": response.body})
        else:
            # No application payload is accepted. Older ASGI servers cannot
            # attach a retry marker to the denial, so callers must not replay it.
            await send({"type": "websocket.close", "code": 1013})
