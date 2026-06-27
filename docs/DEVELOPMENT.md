# Development

## Versioning

This repo uses semantic versioning. The framework version is derived from git tags via `hatch-vcs` and exposed at runtime as `benchmark_service.__version__` and via the `GET /version` endpoint on every service.

While the major version is `0`, treat minor bumps as potentially breaking. Once we ship `1.0.0`, normal semver compatibility expectations apply.

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

### Public surface for semver purposes

- `benchmark_service.BenchmarkService` (abstract methods, signatures, behavior)
- `benchmark_service.BenchmarkServiceApp` (constructor, exposed routes)
- `benchmark_service.BenchmarkServiceClient` and `BenchmarkServiceError`
- HTTP route shapes (paths, request/response schemas, status codes)
- WebSocket protocol (`StreamMessageChunk`, `StreamErrorChunk`, `StreamResultChunk`)
- CLI invocation (`create-benchmark-service <name>`) and the public interface of generated projects (CLI flags, on-disk layout). Template *contents* are not public surface — changes there only affect newly-generated services.
- Pydantic schemas in `schemas.py`

### Consumer pinning

Generated benchmark services pin to a specific framework tag (`@vX.Y.Z`) in their `pyproject.toml`. To upgrade a consumer, edit the pin, run `uv lock`, and merge.

If the CLI was installed from a non-tagged commit, scaffolds fall back to `@main` and the CLI prints a warning. To get reproducible scaffolds, install the CLI from a tag:

```bash
uv tool install git+https://github.com/vals-ai/create-benchmark-service.git@vX.Y.Z
```

### Dataset versioning

Separate from the framework version above, a service can declare a **version per dataset** — a human semver labelling the dataset's content. It is served by `get_dataset_version(name)` (on `GET /version?dataset=` and the dataset task-list response) and stamped into generated lab manifests. It is a **label only**: it records the declared version and does not read or verify dataset content (a content-integrity guard is a planned follow-up).

To adopt it:

1. Add a `dataset_versions.yaml` mapping each dataset name (the API name a caller passes as `dataset`) to a version. Versions are per dataset, so splits can differ:

   ```yaml
   validation:
     version: 1.0.1
   default:
     version: 1.0.0
   ```

2. Point your service at it:

   ```python
   class MyBenchmark(BenchmarkService):
       dataset_versions_file: ClassVar[Path] = Path(__file__).parent / "dataset_versions.yaml"
   ```

At startup the declared versions load and the base `get_dataset_version()` serves them (you do not need to override it). Leave `dataset_versions_file` unset (the default) to report no dataset version.

Semver semantics match the framework: **major** = scores not comparable, **minor** = additive/comparable, **patch** = non-scoring fixes.

The version source is the YAML, independent of how `load_datasets()` obtains the data — so this works whether the dataset is a repo-local file or fetched from an external source; declare the label either way.
