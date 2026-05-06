# Development

## Versioning

This repo uses semantic versioning. The framework version is derived from git tags via `hatch-vcs` and exposed at runtime as `benchmark_service.__version__` and via the `GET /version` endpoint on every service.

### PR title bump tags

Every PR targeting `main` must include one of `#patch`, `#minor`, or `#major` in its title. This is enforced by `.github/workflows/check-pr-title.yaml`; PRs without a bump tag fail before merge.

| Tag | Effect | Example |
| --- | --- | --- |
| `#patch` | Patch bump | v1.0.0 → v1.0.1 |
| `#minor` | Minor bump | v1.0.1 → v1.1.0 |
| `#major` | Major bump | v1.1.0 → v2.0.0 |

### Auto-tag on merge

`.github/workflows/auto-tag-release.yaml` runs on every push to `main` and uses [`anothrNick/github-tag-action`](https://github.com/anothrNick/github-tag-action) to bump and push a new tag. The action reads the merge commit message (which GitHub populates from the PR title) for the bump tag.

The workflow requires a repository secret named `GH_PAT`. It must contain a GitHub personal access token that can check out the repository and push tags. If this secret is missing or lacks tag-write access, the release workflow will fail after merge.

### Publish to PyPI

`.github/workflows/publish-pypi.yaml` runs on `v*` tag pushes. It builds the wheel and source distribution with `uv build --clear`, then publishes them with `uv publish --trusted-publishing always`.

PyPI publishing uses Trusted Publishing. Configure a PyPI trusted publisher for:

- Project name: `create-benchmark-service`
- Owner: `vals-ai`
- Repository: `create-benchmark-service`
- Workflow filename: `publish-pypi.yaml`
- Environment: `pypi`

No PyPI API token repository secret is needed. The publish job uses GitHub OIDC with `id-token: write`.

### Public surface for semver purposes

- `benchmark_service.BenchmarkService` (abstract methods, signatures, behavior)
- `benchmark_service.BenchmarkServiceApp` (constructor, exposed routes)
- `benchmark_service.BenchmarkServiceClient` and `BenchmarkServiceError`
- HTTP route shapes (paths, request/response schemas, status codes)
- WebSocket protocol (`StreamMessageChunk`, `StreamErrorChunk`, `StreamResultChunk`)
- CLI invocation (`create-benchmark-service <name>`) and the public interface of generated projects (CLI flags, on-disk layout). Template *contents* are not public surface — changes there only affect newly-generated services.
- Pydantic schemas in `schemas.py`

### Consumer pinning

Generated benchmark services pin to a specific framework package version (`create-benchmark-service==X.Y.Z`) in their `pyproject.toml`. To upgrade a consumer, edit the pin, run `uv lock`, and merge.

If the CLI was installed from a non-release build, scaffolds fall back to `create-benchmark-service>=0.0.0` and the CLI prints a warning. To get reproducible scaffolds, install the CLI from a published PyPI version:

```bash
uv tool install create-benchmark-service==X.Y.Z
```
