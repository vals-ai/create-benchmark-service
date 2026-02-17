"""Tests for project generator."""

import tempfile
from pathlib import Path

import pytest

from cli.generator import BenchmarkNames, generate_project, transform_name, validate_name


@pytest.mark.parametrize(
    ("input_name", "expected"),
    [
        ("swebench", BenchmarkNames(benchmark_name="swebench", benchmark_package="swebench_service")),
        ("swe-bench", BenchmarkNames(benchmark_name="swe-bench", benchmark_package="swe_bench_service")),
        ("swe_bench", BenchmarkNames(benchmark_name="swe-bench", benchmark_package="swe_bench_service")),
        ("SWEBench", BenchmarkNames(benchmark_name="swebench", benchmark_package="swebench_service")),
        ("humaneval", BenchmarkNames(benchmark_name="humaneval", benchmark_package="humaneval_service")),
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


@pytest.mark.parametrize(
    ("benchmark_name", "expected_package", "expected_project_name", "expected_readme_title"),
    [
        ("swebench", "swebench_service", "swebench-service", "# swebench Service"),
        ("swe-bench", "swe_bench_service", "swe-bench-service", "# swe-bench Service"),
        ("humaneval", "humaneval_service", "humaneval-service", "# humaneval Service"),
    ],
)
def test_template_rendering(
    benchmark_name: str,
    expected_package: str,
    expected_project_name: str,
    expected_readme_title: str,
) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / f"{benchmark_name}-service"
        generate_project(benchmark_name, output_dir)

        main_content = (output_dir / "main.py").read_text()
        assert f"from {expected_package}.benchmark_service import ExampleBenchmark" in main_content

        pyproject_content = (output_dir / "pyproject.toml").read_text()
        assert f'name = "{expected_project_name}"' in pyproject_content
        assert f'packages = ["src/{expected_package}"]' in pyproject_content

        readme_content = (output_dir / "README.md").read_text()
        assert expected_readme_title in readme_content


def test_generates_project_structure() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / "swebench-service"
        generate_project("swebench", output_dir)

        assert (output_dir / "main.py").exists()
        assert (output_dir / "pyproject.toml").exists()
        assert (output_dir / "README.md").exists()
        assert (output_dir / "Makefile").exists()
        assert (output_dir / "Dockerfile").exists()
        assert (output_dir / ".gitignore").exists()
        assert (output_dir / ".dockerignore").exists()
        assert (output_dir / ".python-version").exists()
        assert (output_dir / "src" / "swebench_service").is_dir()
        assert (output_dir / "src" / "swebench_service" / "__init__.py").exists()
        assert (output_dir / "src" / "swebench_service" / "benchmark_service.py").exists()
        assert (output_dir / "tests").is_dir()
        assert (output_dir / ".github" / "workflows").is_dir()


def test_fails_if_directory_exists() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / "swebench-service"
        output_dir.mkdir()

        with pytest.raises(FileExistsError, match="already exists"):
            generate_project("swebench", output_dir)
