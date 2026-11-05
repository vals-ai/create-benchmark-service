# Request draining reference

See the [setup guide](DRAINING.md) to enable draining.
CBS adds no control endpoint or traffic routing.

## Local API

| Call | Behavior |
| --- | --- |
| `DrainState.create(path)` | Creates a closed store at revision `-1`: no controller command yet. |
| `DrainState(path)` | Opens an existing store. |
| `set_admission(accepting=..., revision=...)` | Opens or closes the store. Nonnegative revisions order commands. Older commands do nothing. Conflicts at the same revision raise errors. |
| `snapshot()` | Returns `store_id`, `revision`, `accepting`, and `in_flight`. |
| `admit(request_id)` | Accepts a request only while open. |
| `complete(request_id)` | Removes that request. Repetition cannot reduce another request's count. |

Middleware calls `admit` and `complete` for HTTP requests and WebSocket connections, including error responses and awaited cleanup.
Crashed workers leave requests counted. Missing, replaced, locked, or corrupt stores raise errors, not completion.

## Rejection response

Closed workers return HTTP 503 with `X-Benchmark-Service-Draining: 1` before benchmark work starts.
`is_drain_rejection(status, headers)` recognizes that status and header together.
For WebSockets, rejection happens during connection setup, before the client sends its payload.
Without ASGI `websocket.http.response`, rejection has no retry header.

## Retry rules

The client starts retries for up to 60 seconds.
HTTP retries disable keep-alive in a separate pool to bypass idle connections to old tasks.
Accepted requests keep their connections.
WebSocket retries open a new connection with a 60-second connection timeout.
After connection, this loop never replays the payload.

The retry window does not limit grading time.
Ordinary 503 responses and uncertain failures do not trigger draining retries. Older HTTP retries stay unchanged.
DNS caches can still return an old task.
