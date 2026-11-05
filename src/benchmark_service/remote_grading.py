"""Request-scoped authorization for a registered remote grading attempt."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from starlette.datastructures import Headers

REMOTE_GRADING_ISSUER_HEADER = "X-Vals-Remote-Grading-Issuer"
REMOTE_GRADING_GRANT_HEADER = "X-Vals-Remote-Grading-Grant"


@dataclass(frozen=True)
class RemoteGradingAuthorization:
    issuer: str
    token: str = field(repr=False)

    def as_headers(self) -> dict[str, str]:
        return {REMOTE_GRADING_ISSUER_HEADER: self.issuer, REMOTE_GRADING_GRANT_HEADER: self.token}


@dataclass(frozen=True)
class RemoteGradingRegistration:
    issuer: str
    registration_id: str


_current_authorization: ContextVar[RemoteGradingAuthorization | None] = ContextVar(
    "benchmark_service_remote_grading_authorization", default=None
)


def current_remote_grading_authorization() -> RemoteGradingAuthorization | None:
    """Return the registered attempt's authorization during this request."""
    return _current_authorization.get()


@contextmanager
def remote_grading_authorization_scope(authorization: RemoteGradingAuthorization | None) -> Iterator[None]:
    """Bind authorization only for the enclosed request or callback."""
    token = _current_authorization.set(authorization)
    try:
        yield
    finally:
        _current_authorization.reset(token)


def remote_grading_authorization_from_headers(headers: Headers) -> RemoteGradingAuthorization | None:
    """Read the explicit pair without copying its secret into errors."""
    issuers = headers.getlist(REMOTE_GRADING_ISSUER_HEADER)
    grants = headers.getlist(REMOTE_GRADING_GRANT_HEADER)
    if not issuers and not grants:
        return None
    if len(issuers) != 1 or len(grants) != 1:
        raise ValueError("Supply exactly one remote grading issuer and grant")
    if not issuers[0] or not grants[0]:
        raise ValueError("Remote grading issuer and grant must be non-empty")
    return RemoteGradingAuthorization(issuers[0], grants[0])
