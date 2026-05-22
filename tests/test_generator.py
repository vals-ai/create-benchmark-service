"""Tests for project generator."""

import tempfile
from pathlib import Path

import pytest

import benchmark_service
from cli.generator import (
    BenchmarkNames,
    _resolve_framework_ref,  # pyright: ignore[reportPrivateUsage]
    generate_project,
    transform_name,
    validate_name,
)


@pytest.mark.parametrize(
    ("input_name", "expected"),
    [
        ("swebench", BenchmarkNames(benchmark_name="swebench", benchmark_package="swebench_benchmark_service")),
        ("swe-bench", BenchmarkNames(benchmark_name="swe-bench", benchmark_package="swe_bench_benchmark_service")),
        ("swe_bench", BenchmarkNames(benchmark_name="swe-bench", benchmark_package="swe_bench_benchmark_service")),
        ("SWEBench", BenchmarkNames(benchmark_name="swebench", benchmark_package="swebench_benchmark_service")),
        ("humaneval", BenchmarkNames(benchmark_name="humaneval", benchmark_package="humaneval_benchmark_service")),
    ],
)
def test_transform_name(input_name: str, expected: BenchmarkNames) -> None:
    assert transform_name(input_name) == expected


@pytest.mark.parametrize("name", ["swebench", "swe-bench", "swe_bench", "humaneval", "mbpp"])
def test_valid_names(name: str) -> None:
    validate_name(name)  # Should not raise


@pytest.mark.parametrize(
    ("name", "error_match"),
    [
        ("", "cannot be empty"),
        ("swe@bench", "alphanumeric"),
        ("123bench", "must start with a letter"),
        ("sys", "conflicts with Python standard library"),
        ("json", "conflicts with Python standard library"),
    ],
)
def test_invalid_names(name: str, error_match: str) -> None:
    with pytest.raises(ValueError, match=error_match):
        validate_name(name)


def test_resolve_framework_ref_clean_semver() -> None:
    assert _resolve_framework_ref("1.0.0") == "v1.0.0"
    assert _resolve_framework_ref("1.2.3") == "v1.2.3"


def test_resolve_framework_ref_dev_version_falls_back_to_main(capsys: pytest.CaptureFixture[str]) -> None:
    assert _resolve_framework_ref("1.0.1.dev3+gabc1234") == "main"
    captured = capsys.readouterr()
    assert "warning" in captured.err.lower()
    assert captured.out == ""


@pytest.mark.parametrize(
    ("benchmark_name", "expected_package", "expected_project_name", "expected_readme_title"),
    [
        ("swebench", "swebench_benchmark_service", "swebench-benchmark-service", "# swebench Service"),
        ("swe-bench", "swe_bench_benchmark_service", "swe-bench-benchmark-service", "# swe-bench Service"),
        ("humaneval", "humaneval_benchmark_service", "humaneval-benchmark-service", "# humaneval Service"),
    ],
)
def test_template_rendering(
    benchmark_name: str,
    expected_package: str,
    expected_project_name: str,
    expected_readme_title: str,
) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / f"{benchmark_name}-benchmark-service"
        generate_project(benchmark_name, output_dir)

        main_content = (output_dir / "main.py").read_text()
        assert f"from {expected_package}.benchmark_service import ExampleBenchmark" in main_content

        pyproject_content = (output_dir / "pyproject.toml").read_text()
        assert f'name = "{expected_project_name}"' in pyproject_content
        assert f'packages = ["src/{expected_package}"]' in pyproject_content

        readme_content = (output_dir / "README.md").read_text()
        assert expected_readme_title in readme_content


def test_generated_pyproject_pins_to_tag_for_clean_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(benchmark_service, "__version__", "1.0.0")
    output_dir = tmp_path / "demo-benchmark-service"
    generate_project(benchmark_name="demo", output_dir=output_dir)
    pyproject = (output_dir / "pyproject.toml").read_text()
    assert "create-benchmark-service.git@v1.0.0" in pyproject
    assert "@main" not in pyproject


def test_generated_pyproject_falls_back_to_main_for_dev_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(benchmark_service, "__version__", "1.0.1.dev3+gabc1234")
    output_dir = tmp_path / "demo-benchmark-service"
    generate_project(benchmark_name="demo", output_dir=output_dir)
    pyproject = (output_dir / "pyproject.toml").read_text()
    assert "create-benchmark-service.git@main" in pyproject


def test_generates_project_structure() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / "swebench-benchmark-service"
        generate_project("swebench", output_dir)

        assert (output_dir / "main.py").exists()
        assert (output_dir / "pyproject.toml").exists()
        assert (output_dir / "README.md").exists()
        assert (output_dir / "Makefile").exists()
        assert (output_dir / "Dockerfile").exists()
        assert (output_dir / ".gitignore").exists()
        assert (output_dir / ".dockerignore").exists()
        assert (output_dir / ".python-version").exists()
        assert (output_dir / "src" / "swebench_benchmark_service").is_dir()
        assert (output_dir / "src" / "swebench_benchmark_service" / "__init__.py").exists()
        assert (output_dir / "src" / "swebench_benchmark_service" / "benchmark_service.py").exists()
        assert (output_dir / "tests").is_dir()
        assert (output_dir / ".github" / "workflows").is_dir()


def test_generated_project_excludes_framework_versioning_workflows(tmp_path: Path) -> None:
    output_dir = tmp_path / "swebench-benchmark-service"
    generate_project("swebench", output_dir)

    workflows_dir = output_dir / ".github" / "workflows"
    assert (workflows_dir / "test.yaml").exists()
    assert (workflows_dir / "style.yaml").exists()
    assert (workflows_dir / "typecheck.yaml").exists()
    assert not (workflows_dir / "cli-integration.yaml").exists()
    assert not (workflows_dir / "auto-tag-release.yaml").exists()
    assert not (workflows_dir / "check-pr-title.yaml").exists()


def test_generated_benchmark_service_implements_task_listing(tmp_path: Path) -> None:
    output_dir = tmp_path / "demo-benchmark-service"
    generate_project("demo", output_dir)

    service = (output_dir / "src" / "demo_benchmark_service" / "benchmark_service.py").read_text()
    assert "from benchmark_service.v1_schemas import V1Task" in service
    assert "async def list_tasks(self, dataset: str | None = None) -> list[V1Task]:" in service
    assert 'V1Task(id=task_id, question=task["problem"])' in service


def test_generated_gitignore_excludes_framework_only_entries(tmp_path: Path) -> None:
    output_dir = tmp_path / "swebench-benchmark-service"
    generate_project("swebench", output_dir)

    gitignore = (output_dir / ".gitignore").read_text()
    assert "__pycache__/" in gitignore
    assert ".venv/" in gitignore
    assert ".env" in gitignore
    assert "src/benchmark_service/_version.py" not in gitignore


def test_fails_if_directory_exists() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / "swebench-benchmark-service"
        output_dir.mkdir()

        with pytest.raises(FileExistsError, match="already exists"):
            generate_project("swebench", output_dir)
