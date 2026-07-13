"""The client wheel config must track the root project's runtime contract."""

import tomllib
from pathlib import Path

_ROOT = Path(__file__).parent.parent


def _project(path: str) -> dict:
    return tomllib.loads((_ROOT / path).read_text())["project"]


def test_client_wheel_dependencies_match_root():
    # benchmark_service/__init__ imports the app, middleware, and sandbox
    # providers, so the client wheel needs the full runtime dependency set.
    root, client = _project("pyproject.toml"), _project("client/pyproject.toml")
    assert client["dependencies"] == root["dependencies"]
    assert client["requires-python"] == root["requires-python"]
