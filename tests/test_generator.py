"""Tests for project generator."""

import tempfile
from pathlib import Path

import pytest

from cli.generator import BenchmarkNames, generate_project, transform_name, validate_name


class TestTransformName:
    """Tests for transform_name function."""

    def test_simple_name(self):
        """Test simple alphanumeric name."""
        result = transform_name("swebench")
        assert result == BenchmarkNames(benchmark_name="swebench", benchmark_package="swebench")

    def test_hyphenated_name(self):
        """Test name with hyphens."""
        result = transform_name("swe-bench")
        assert result == BenchmarkNames(benchmark_name="swe-bench", benchmark_package="swe_bench")

    def test_underscored_name(self):
        """Test name with underscores converts to hyphens."""
        result = transform_name("swe_bench")
        assert result == BenchmarkNames(benchmark_name="swe-bench", benchmark_package="swe_bench")

    def test_mixed_case(self):
        """Test mixed case converts to lowercase."""
        result = transform_name("SWEBench")
        assert result == BenchmarkNames(benchmark_name="swebench", benchmark_package="swebench")


class TestValidateName:
    """Tests for validate_name function."""

    def test_valid_name(self):
        """Test that valid names pass."""
        validate_name("swebench")
        validate_name("swe-bench")
        validate_name("swe_bench")
        validate_name("humaneval")

    def test_empty_name(self):
        """Test that empty name raises error."""
        with pytest.raises(ValueError, match="cannot be empty"):
            validate_name("")

    def test_name_with_special_chars(self):
        """Test that special characters raise error."""
        with pytest.raises(ValueError, match="alphanumeric"):
            validate_name("swe@bench")

    def test_name_starting_with_number(self):
        """Test that name starting with number raises error."""
        with pytest.raises(ValueError, match="must start with a letter"):
            validate_name("123bench")


class TestGenerateProject:
    """Tests for generate_project function."""

    def test_generates_project_structure(self):
        """Test that project structure is created correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "test-service"
            generate_project("test", output_dir)

            # Check main files exist
            assert (output_dir / "main.py").exists()
            assert (output_dir / "pyproject.toml").exists()
            assert (output_dir / "README.md").exists()
            assert (output_dir / "Makefile").exists()
            assert (output_dir / "Dockerfile").exists()
            assert (output_dir / ".gitignore").exists()
            assert (output_dir / ".dockerignore").exists()
            assert (output_dir / ".python-version").exists()

            # Check directories exist
            assert (output_dir / "src" / "test").is_dir()
            assert (output_dir / "tests").is_dir()
            assert (output_dir / ".github" / "workflows").is_dir()

            # Check package files exist
            assert (output_dir / "src" / "test" / "__init__.py").exists()
            assert (output_dir / "src" / "test" / "benchmark_service.py").exists()

    def test_generates_correct_main_py(self):
        """Test that main.py has correct imports."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "swebench-service"
            generate_project("swebench", output_dir)

            main_content = (output_dir / "main.py").read_text()
            assert "from swebench.benchmark_service import ExampleBenchmark" in main_content
            assert "from benchmark_service import create_app" in main_content

    def test_generates_correct_pyproject_toml(self):
        """Test that pyproject.toml has correct project name."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "humaneval-service"
            generate_project("humaneval", output_dir)

            pyproject_content = (output_dir / "pyproject.toml").read_text()
            assert 'name = "humaneval-service"' in pyproject_content

    def test_generates_correct_readme_title(self):
        """Test that README has correct title."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "mbpp-service"
            generate_project("mbpp", output_dir)

            readme_content = (output_dir / "README.md").read_text()
            assert "# mbpp Service" in readme_content

    def test_fails_if_directory_exists(self):
        """Test that generation fails if directory already exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "test-service"
            output_dir.mkdir()

            with pytest.raises(FileExistsError, match="already exists"):
                generate_project("test", output_dir)

    def test_hyphenated_name_creates_underscore_package(self):
        """Test that hyphenated names create underscore packages."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "swe-bench-service"
            generate_project("swe-bench", output_dir)

            assert (output_dir / "src" / "swe_bench").is_dir()
            assert (output_dir / "src" / "swe_bench" / "benchmark_service.py").exists()
