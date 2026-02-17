"""CLI entry point for create-benchmark-service."""

from pathlib import Path

import click

from .generator import generate_project, transform_name


@click.command()
@click.argument("benchmark_name")
def main(benchmark_name: str) -> None:
    """Create a new benchmark service.

    Example: create-benchmark-service swebench
    """
    names = transform_name(benchmark_name)
    output_dir_path = Path.cwd() / f"{names['benchmark_name']}-service"

    try:
        generate_project(benchmark_name=benchmark_name, output_dir=output_dir_path)

        print(f"Created {names['benchmark_name']}-service at {output_dir_path}")
        print()
        print("Next steps:")
        print(f"  cd {output_dir_path.name}")
        print("  make install")
        print("  make dev")

    except (ValueError, FileExistsError) as e:
        print(f"Error: {e}")
        raise click.Abort()


if __name__ == "__main__":
    main()
