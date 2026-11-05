# Let accepted requests finish before stopping a service

Draining means refusing new requests while accepted requests finish.
The [registry controller](https://github.com/vals-ai/benchmark-services-registry/pull/1780) keeps the container alive and sends new requests elsewhere.

## Enable draining

Create one SQLite store before starting the service workers:

```python
from benchmark_service.draining import DrainState

state = DrainState.create("/tmp/benchmark-drain.sqlite")
```

Set `BENCHMARK_SERVICE_DRAIN_STATE_PATH=/tmp/benchmark-drain.sqlite` before starting the service.
Use a fresh local path for each container lifetime.
Do not use a network filesystem or replace the store while workers can own requests.

Workers start closed to new requests. `/health` stays available.
A missing or invalid store prevents startup.
Without the environment variable, behavior stays unchanged.

## Control new requests

Call [`set_admission`](DRAINING_REFERENCE.md) from a process that can access the local store.
Use a higher `revision` for each new command.
Bind reports to the task, `store_id`, and requested revision.

Publish the replacement address before closing old workers.
Use an updated CBS client to retry draining responses.
Before stopping a container, wait for accepted requests to finish.
Account for detached work and later calls from the same run.
