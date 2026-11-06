# Sailboxes provider

`SailProviderConfig` adds `type: sail` to the shared provider configuration accepted by
benchmark-service requests and by Valkyrie's provider-secret parser. The existing
Daytona default does not change. Consumers must upgrade their pinned CBS version
after this PR is released before `--provider sail` is available.

Store this JSON through the existing Valkyrie secret workflow, then map the secret
with `valkyrie provider set sail <secret-name>`:

```json
{
  "SAIL_API_KEY": "<organization API key>",
  "SAIL_APP": "vals-benchmark-service"
}
```

The shared provider uses a separate HTTP client for each config. It does not modify
process environment variables or use the Python SDK's process-wide credential cache.
Provider responses are not included in exceptions because they can contain secrets.

For boot-time artifact grading, use `GRADING_SANDBOX_PROVIDER=sail`, `SAIL_API_KEY`,
and optional `SAIL_APP` in the service environment. Existing per-request provider
configuration also works. Keep real keys out of manifests, source control and logs.

## Images and supported operations

- `ImageSource(image="sail:debian")` starts Sail's ready-to-use Debian base.
- `ImageSource(image="sail:devbox")` starts its development base, including Docker.
- Registry image references must name Debian/Ubuntu images from `docker.io`,
  `ghcr.io`, `public.ecr.aws`, or `quay.io`, and must already be built in the same
  Sail organization. Prepare them once with Sail's SDK, for example
  `sail.Image.from_registry("docker.io/library/python:3.12-slim").build()`.
  The provider does not build images during task retrieval or creation. Unprepared
  images fail with HTTP 409. Private ECR images need a separate migration path.
- Commands support working directories, environment variables, streamed stdout
  and stderr, exit codes, and timeouts. Files transfer as bytes.
- Native generation uses the existing process-group supervision protocol, including
  the episode supervisor. Each effective sandbox must pass the containment probe.
- Egress can be blocked, restricted to IPv4/host allowlists, and reopened.
- Labels are saved in a reserved create-time environment field so they survive
  handle reconstruction. Listing is app-scoped, paginated, and filters those labels
  and creation timestamps. It excludes unmanaged and terminal sandboxes.
- Snapshots, targeted snapshots, GPUs, persistent volume mounts and provider-managed
  secret references are not supported in this first version and fail before creation.
  The caller unwraps `ComposeSource` to its outer image, as with other providers.

Sail rounds resource limits up. Its minimum RAM limit is 16 GiB and disk limit is
8 GiB; these limits are not billing reservations. This provider does not promise
strict enforcement of smaller requested limits. Automatic sleep is disabled while
the framework owns a sandbox, to keep execution and deadlines predictable. Explicit
teardown terminates it, and a server-side maximum lifetime of 167 hours bounds a
lost client's allocation. A timed-out or cancelled create retains ownership of the
in-flight response and terminates a late successful allocation before closing its
client. An API failure that never supplies an ID cannot prove immediate cleanup;
the server lifetime remains the backstop.

This PR does not add Sail billing ingestion, change production provider mappings,
or run a paid benchmark. Keep those as separate rollout work.

## Validation

Credential-free protocol and lifecycle tests:

```bash
uv run pytest tests/test_sail_sandbox.py
```

The optional live smoke requires `TEST_SAIL_API_KEY` and explicitly allocates a
small Sailbox, probes process groups, executes a command, and transfers a file.
Run it only with approval for the account and spend:

```bash
uv run pytest tests/integration/test_sail_sandbox.py
```

API contract: [Sail HTTPS API](https://docs.sailresearch.com/sailboxes-http-api).
