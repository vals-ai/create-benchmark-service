# create-benchmark-service (archived)

This repository is archived. create-benchmark-service now lives in valkyrie-internal at [`packages/create-benchmark-service`](https://github.com/vals-ai/valkyrie-internal/tree/dev/packages/create-benchmark-service), and is published to the public [vals-ai/Valkyrie](https://github.com/vals-ai/Valkyrie/tree/prod/packages/create-benchmark-service) mirror. The Vals hosting layer (`benchmark_service.vals`) is now its own package, [`vals-benchmark-service`](https://github.com/vals-ai/valkyrie-internal/tree/dev/packages/vals-benchmark-service), next to it.

- Open new PRs against valkyrie-internal.
- Install from the mirror at a release tag:

    ```toml
    [tool.uv.sources]
    create-benchmark-service = { git = "https://github.com/vals-ai/Valkyrie.git", subdirectory = "packages/create-benchmark-service", tag = "cbs-v<version>" }
    vals-benchmark-service = { git = "https://github.com/vals-ai/Valkyrie.git", subdirectory = "packages/vals-benchmark-service", tag = "vals-benchmark-service-v<version>" }
    ```

- Scaffold a new service with `uvx --from "git+https://github.com/vals-ai/Valkyrie.git@cbs-v0.78.2#subdirectory=packages/create-benchmark-service" create-benchmark-service <benchmark-name>`.
- `packages/vals-benchmark-service/scripts/migrate_cbs_consumer.py` moves an existing service over. Existing `v*` pins on this repository keep working.
