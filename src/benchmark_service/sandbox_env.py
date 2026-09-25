"""Fill a generation sandbox's environment variables from the run's own values.

A benchmark service can ask for plain environment variables in the generation sandbox through
`RetrieveTaskResponse.sandbox_env`. A value may be a `${VAR}` or `${VAR:-default}` template, the syntax
Harbor tasks use, which the runner fills from the values of the run itself, such as its secrets. The
service never supplies those values, so its own credentials cannot reach an agent.
"""

import re
from collections.abc import Mapping

_TEMPLATE_PATTERN = re.compile(r"\$\{([^}:]+)(?::-(.*))?\}")


class SandboxEnvError(ValueError):
    """Raised when a sandbox_env template names a value the run does not have and gives no default."""


def is_sandbox_env_template(value: str) -> bool:
    """Return whether a sandbox_env value is a `${VAR}` or `${VAR:-default}` template.

    Arguments:
    - value: One sandbox_env value.

    Returns:
    True when the value is a template the runner fills in.
    """

    return _TEMPLATE_PATTERN.fullmatch(value) is not None


def resolve_sandbox_env(env: Mapping[str, str], values: Mapping[str, str]) -> dict[str, str]:
    """Fill sandbox_env templates from the run's values, passing literal values through.

    Arguments:
    - env: The response's sandbox_env.
    - values: Values the run provides, keyed by name, such as its resolved secrets.

    Returns:
    The environment variables with every template filled in.

    Raises:
    SandboxEnvError: If a template without a default names a value the run does not provide.
    """

    resolved: dict[str, str] = {}
    for key, value in env.items():
        match = _TEMPLATE_PATTERN.fullmatch(value)
        if match is None:
            resolved[key] = value
            continue

        name, default = match.group(1), match.group(2)
        if name in values:
            resolved[key] = values[name]
        elif default is not None:
            resolved[key] = default
        else:
            raise SandboxEnvError(f"Task environment variable {key} needs {name}, which this run does not provide")
    return resolved
