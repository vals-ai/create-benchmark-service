"""CLI entry point for create-benchmark-service."""

import asyncio
from importlib import import_module
from pathlib import Path

import click

from benchmark_service.base import BenchmarkService

from .generator import generate_project, transform_name


@click.command()
@click.argument("benchmark_name")
@click.option("--template", type=click.Choice(["default", "vals-ai"]), default="default", show_default=True)
def main(benchmark_name: str, template: str) -> None:
    """Create a new benchmark service.

    Example: create-benchmark-service swebench
    """
    names = transform_name(benchmark_name)
    output_dir_path = Path.cwd() / f"{names['benchmark_name']}-benchmark-service"

    try:
        generate_project(benchmark_name=benchmark_name, output_dir=output_dir_path, template=template)

        print(f"Created {names['benchmark_name']}-benchmark-service at {output_dir_path}")
        print()
        print("Next steps:")
        print(f"  cd {output_dir_path.name}")
        print("  make install")
        print("  make dev")

    except (ValueError, FileExistsError) as e:
        print(f"Error: {e}")
        raise click.Abort()


@click.group()
def service() -> None:
    """Run benchmark service commands."""


@service.command("export-dataset")
@click.option("--service", "service_path", required=True, help="Service class as module:ClassName.")
@click.option("--dataset", required=True)
@click.option("--out", "out_dir", required=True, type=click.Path(path_type=Path, file_okay=False))
def export_dataset(service_path: str, dataset: str, out_dir: Path) -> None:
    """Export a dataset into the vals-datasets layout."""
    module_name, class_name = service_path.split(":")
    cls = getattr(import_module(module_name), class_name)
    assert issubclass(cls, BenchmarkService), "Service must inherit BenchmarkService"

    async def export() -> bool:
        instance = await cls.create()
        out_dir.mkdir(parents=True, exist_ok=True)
        return await instance.export_dataset(dataset, out_dir)

    if not asyncio.run(export()):
        raise click.exceptions.Exit(3)


if __name__ == "__main__":
    main()
