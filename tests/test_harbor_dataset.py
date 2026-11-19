"""Tests for the Harbor dataset writer."""

import os
import tomllib
from pathlib import Path

import pytest
from pydantic import ValidationError

from benchmark_service.harbor_dataset import HarborTask, Verifier, write_dataset

GRADER = "ghcr.io/vals-ai/a2j-grader@sha256:" + "a" * 64


def _task(**overrides: object) -> HarborTask:
    fields: dict[str, object] = {
        "task_id": "both__case-001",
        "name": "vals/a2j-both-case-001",
        "instruction": "Help the client.\n",
        "environment_dockerfile": "FROM python:3.12-slim\nCOPY conductor /task/agent/conductor\n",
        "environment_files": {"conductor/conductor.json": '{"condition": "both"}\n'},
        "metadata": {"benchmark": "a2j", "categories": ["housing", "eviction"]},
        "agent_timeout_sec": 10800.0,
        "artifacts": ["/workspace/logs"],
        "verifier": Verifier(
            image=GRADER,
            command="a2j-grade --generation /logs/artifacts/generation.json",
            files={"case.json": "{}\n"},
            env={"MODEL_GATEWAY_URL": "${MODEL_GATEWAY_URL:-}"},
        ),
    }
    return HarborTask.model_validate(fields | overrides)


def test_written_dataset_has_harbor_layout(tmp_path: Path) -> None:
    root = tmp_path / "a2j"
    assert write_dataset(root, [_task()], metric="print(1)\n") == ["both__case-001"]

    task_dir = root / "both__case-001"
    assert sorted(str(p.relative_to(task_dir)) for p in task_dir.rglob("*") if p.is_file()) == [
        "environment/Dockerfile",
        "environment/conductor/conductor.json",
        "instruction.md",
        "task.toml",
        "tests/Dockerfile",
        "tests/case.json",
        "tests/test.sh",
    ]
    assert (task_dir / "tests/Dockerfile").read_text() == f"FROM {GRADER}\nCOPY . /tests/\n"
    assert os.access(task_dir / "tests/test.sh", os.X_OK)
    assert (root / "metric.py").read_text() == "print(1)\n"
    assert "script_path: metric.py" in (root / "harbor.yaml").read_text()

    config = tomllib.loads((task_dir / "task.toml").read_text())
    assert config["task"] == {"name": "vals/a2j-both-case-001"}
    assert config["metadata"]["categories"] == ["housing", "eviction"]
    assert config["metadata"]["valkyrie"] == {"verifier_result_file": "details.json"}
    assert config["verifier"]["environment_mode"] == "separate"
    assert config["verifier"]["env"] == {"MODEL_GATEWAY_URL": "${MODEL_GATEWAY_URL:-}"}
    assert "docker_image" not in config["verifier"]["environment"]


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"name": "A2J/Case 1"}, "lowercase <org>/<task>"),
        ({"task_id": "../escape"}, "Not a valid task ID"),
        ({"environment_image": GRADER}, "exactly one of"),
        ({"environment_dockerfile": None, "environment_image": "python:3.12"}, "Pin the image"),
        ({"files": {"tests/answer.json": "{}"}}, "verifier.files"),
        ({"metadata": {"valkyrie": {"x": 1}}}, "metadata.valkyrie"),
        ({"metadata": {"score": float("nan")}}, "round-trip"),
    ],
)
def test_invalid_task_is_refused(overrides: dict[str, object], message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        _task(**overrides)


def test_unpinned_grader_is_refused() -> None:
    with pytest.raises(ValidationError, match="Pin the image"):
        Verifier(image="ghcr.io/vals-ai/a2j-grader:latest", command="a2j-grade")


def test_duplicate_task_ids_are_refused_before_writing(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Duplicate task IDs"):
        write_dataset(tmp_path / "out", [_task(), _task()], metric="")
    assert not (tmp_path / "out").exists()
