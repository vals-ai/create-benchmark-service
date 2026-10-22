"""Request-scoped attribution of grading work to the Valkyrie run, task and attempt it serves.

A caller (the Valkyrie tracker, the Vals app) names the run, task and task
attempt on each evaluation request. Binding them here lets the model calls a
benchmark makes while grading report their cost against that run and attempt,
as the grader rather than the agent, with no change to the benchmark's own
grading code.

Services that grade through model-library pick the binding up automatically:
when ``model_library.attribution`` is importable, the same scope attributes
every query made inside it with ``agent_name="grader"``. Services without
model-library can read ``current_request_attribution()`` and forward it to
whatever runs their grader. A missing value stays ``None``; nothing here invents
a link to a run.
"""

from __future__ import annotations

import importlib
from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager, nullcontext
from contextvars import ContextVar
from dataclasses import dataclass

from benchmark_service.observability import ATTEMPT_ID_HEADER, RUN_ID_HEADER

GRADER_AGENT_NAME = "grader"


@dataclass(frozen=True)
class RequestAttribution:
    run_id: str | None
    task_id: str | None
    attempt_id: str | None

    def as_environment(self) -> dict[str, str]:
        """The known IDs as the model-library settings a separate grading process reads."""
        values = {"RUN_ID": self.run_id, "TASK_ID": self.task_id, "ATTEMPT_ID": self.attempt_id}
        return {name: value for name, value in values.items() if value is not None}


_current_attribution: ContextVar[RequestAttribution | None] = ContextVar(
    "benchmark_service_request_attribution", default=None
)


def current_request_attribution() -> RequestAttribution | None:
    """Return the attribution of the in-flight evaluation request, or None outside one."""
    return _current_attribution.get()


def attribution_from_request(
    headers: Mapping[str, str], *, task_id: str, attempt_id: str | None = None
) -> RequestAttribution:
    """Read the caller's run and attempt from the correlation headers the client sends."""
    return RequestAttribution(
        run_id=headers.get(RUN_ID_HEADER) or headers.get(RUN_ID_HEADER.lower()),
        task_id=task_id,
        attempt_id=attempt_id or headers.get(ATTEMPT_ID_HEADER) or headers.get(ATTEMPT_ID_HEADER.lower()),
    )


@contextmanager
def request_attribution_scope(attribution: RequestAttribution) -> Iterator[None]:
    """Bind one request's attribution, and attribute model-library queries made inside it as the grader."""
    token = _current_attribution.set(attribution)
    try:
        with _model_library_grader_scope(attribution):
            yield
    finally:
        _current_attribution.reset(token)


def _model_library_grader_scope(attribution: RequestAttribution) -> AbstractContextManager[None]:
    try:
        module = importlib.import_module("model_library.attribution")
    except ImportError:
        # The service grades without model-library, or with a release that predates the scope.
        return nullcontext()
    return module.attribution_scope(
        run_id=attribution.run_id,
        question_id=attribution.task_id,
        attempt_id=attribution.attempt_id,
        agent_name=GRADER_AGENT_NAME,
    )
