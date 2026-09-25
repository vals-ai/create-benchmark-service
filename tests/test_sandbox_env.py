"""Tests for a generation sandbox's plain environment variables.

Covers how the runner fills sandbox_env templates from a run's values, and how RetrieveTaskResponse
validates sandbox_env.
"""

import pytest
from pydantic import ValidationError

from benchmark_service import ImageSource, Resources, SandboxEnvError, resolve_sandbox_env
from benchmark_service.schemas import RetrieveTaskResponse


def _response(**fields: object) -> RetrieveTaskResponse:
    return RetrieveTaskResponse.model_validate(
        {
            "source": ImageSource(image="registry/task@sha256:abc"),
            "problem_path": "/app/instruction.md",
            "cwd": "/app",
            "resources": Resources(vcpu=1, memory=2, disk=10),
            **fields,
        }
    )


def test_templates_are_filled_from_the_run_and_literals_pass_through() -> None:
    """`${VAR}` takes the run's value, `${VAR:-default}` falls back, and a literal is kept as written."""
    env = {"MODE": "fast", "OPENAI_API_KEY": "${OPENAI_API_KEY}", "MODEL": "${JUDGE_MODEL:-gpt-4}"}

    resolved = resolve_sandbox_env(env, {"OPENAI_API_KEY": "sk-run"})

    assert resolved == {"MODE": "fast", "OPENAI_API_KEY": "sk-run", "MODEL": "gpt-4"}


def test_a_template_the_run_cannot_fill_names_what_is_missing() -> None:
    """A task needing a value the run did not provide fails with the name to pass, not an empty variable."""
    with pytest.raises(SandboxEnvError, match="OPENAI_API_KEY, which this run does not provide"):
        resolve_sandbox_env({"OPENAI_API_KEY": "${OPENAI_API_KEY}"}, {})


def test_sandbox_env_round_trips_and_defaults_to_empty() -> None:
    """Older services omit the field, and a runner reads the same values a service sent."""
    assert _response().sandbox_env == {}
    response = _response(sandbox_env={"MODE": "fast", "KEY": "${KEY}"})

    assert RetrieveTaskResponse.model_validate_json(response.model_dump_json()).sandbox_env == response.sandbox_env


@pytest.mark.parametrize(
    ("fields", "match"),
    [
        ({"sandbox_env": {"BAD-NAME": "x"}}, "Invalid environment variable names"),
        ({"sandbox_env": {"TOKEN": "x"}, "sandbox_secrets": {"TOKEN": "secret-ref"}}, "both sandbox_env and"),
    ],
)
def test_invalid_sandbox_env_is_refused(fields: dict[str, object], match: str) -> None:
    """Names must be valid variable names, and a name cannot be both plain and provider-managed."""
    with pytest.raises(ValidationError, match=match):
        _response(**fields)
