# Request draining

Draining means refusing new requests while accepted requests finish.
The optional gate counts requests across workers in one container.
A deployment controller supplies routing, task retention, and retirement.
The framework adds no control endpoint.

## Initialize

Create one store on a local filesystem before starting service workers.
Use a fresh path for each container lifetime.
Do not use a network filesystem or replace a store while workers can own requests.

```python
from benchmark_service.draining import DrainState

state = DrainState.create("/tmp/benchmark-drain.sqlite")
```

Set `BENCHMARK_SERVICE_DRAIN_STATE_PATH=/tmp/benchmark-drain.sqlite` before starting the service.
Every worker opens this store.
An absent or invalid store prevents application construction.
Without this environment variable, the service runs without the gate.

## Local API

`DrainState.create(path)` creates a closed store at revision `-1`, which means that control state is unknown.
`DrainState(path)` opens an existing store.
The `/health` endpoint stays available while admission is closed.

```python
state = DrainState("/tmp/benchmark-drain.sqlite")
state.set_admission(accepting=True, revision=0)
state.set_admission(accepting=False, revision=1)
snapshot = state.snapshot()
```

`set_admission` applies commands in revision order.
Older commands do nothing, and conflicting commands at the same revision raise an error.
`snapshot` returns `store_id`, `revision`, `accepting`, and `in_flight`.
A controller binds this evidence to the task and store identities.

`admit(request_id)` accepts work only while admission is open.
`complete(request_id)` releases that request, and repeated completion does not reduce another request count.
The middleware calls these methods for each HTTP request or WebSocket scope.
The count includes cleanup that the application awaits.

A worker crash leaves its requests counted.
Missing, replaced, locked, or corrupt stores raise errors instead of reporting completion.
Zero requests does not mean that every run finished.
Detached work and later requests from the same run need separate retention rules.

## Client behavior

The gate returns HTTP 503 with `X-Benchmark-Service-Draining: 1` before benchmark work starts.
It rejects WebSockets during the opening handshake, before the client sends a payload.
`is_drain_rejection(status, headers)` recognizes this exact status and marker.

The client starts retries for up to 60 seconds.
HTTP retries use a separate pool without keep-alive to bypass idle connections to the old task.
Other accepted requests keep their shared connections.
WebSocket retries open a new connection with the normal 60-second handshake timeout.
The retry window does not limit accepted grading time.

After the WebSocket handshake succeeds, this loop never replays its payload.
It does not retry ordinary 503 responses or ambiguous failures.
Existing legacy HTTP retries remain unchanged.
Without the ASGI `websocket.http.response` extension, the server rejects the handshake without a retry marker.

DNS caches can still return an old task after a new connection opens.
The controller must publish the replacement address before closing old admission.
The gate alone does not provide a zero-downtime deployment.
