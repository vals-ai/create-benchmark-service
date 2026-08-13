# Agent notes for create-benchmark-service

Reusable FastAPI framework for Vals benchmark services. Read `docs/DEVELOPMENT.md` before opening a PR — the bump-tag rule below is the most likely thing to forget.

## PR rules

PR titles must include `#patch`, `#minor`, or `#major`. Enforced by `.github/workflows/check-pr-title.yaml`; PRs without a tag fail before merge.

- `#patch` — bug fix that preserves the public surface
- `#minor` — additive change to the public surface (new route, new schema field with default, new optional CLI flag). While the major version is `0`, breaking changes also use `#minor` (`docs/DEVELOPMENT.md`)
- `#major` — breaking signature or wire-format change once `1.0.0` ships; pre-1.0 it is reserved for declaring `1.0.0`

The "public surface" is defined in `docs/DEVELOPMENT.md`. In short: `BenchmarkService` signatures, `BenchmarkServiceApp` exposed routes, `BenchmarkServiceClient`, `schemas.py` Pydantic models, HTTP/WS protocol shapes, and the CLI invocation. Template file *contents* are not public surface.

## Tooling

- Python ≥3.12, `uv` (not pip), `pytest`, `ruff`, `basedpyright`.
- Tests: `uv run pytest`. Lint: `uv run ruff check .`. Typecheck: `uv run basedpyright src/ tests/`.
- All three must be green before you ask for review.

## Where things live

- `docs/DEVELOPMENT.md` — versioning, release flow, consumer pinning.
- `README.md` — architecture, endpoint reference, schema reference.
- `src/benchmark_service/` — framework source. `app.py` registers routes; `base.py` is the `BenchmarkService` ABC; `schemas.py` and `v1_schemas.py` hold wire types.
- `tests/` — pytest suite. Baseline must stay green; add tests for any new behavior.

## Common pitfalls

- New endpoints touch the public surface → `#minor` at minimum.
- Schema field changes: adding a field with a default = `#minor`; removing or renaming is breaking — `#minor` while pre-1.0, `#major` after.
- Don't modify the auto-generated `_version.py` — `hatch-vcs` derives it from git tags.
- The `/v1/*` surface is Descope-only by design; requests served with `AUTH_DISABLED=true` should 403, not pass through.
