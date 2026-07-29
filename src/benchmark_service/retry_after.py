"""Shared parsing for Retry-After-style header values, used by any client-side retry policy."""


def parse_retry_after_seconds(value: object) -> float | None:
    """Parse a Retry-After-style header value as seconds; None if absent or non-numeric.

    Does not handle the HTTP-date form of Retry-After, only the delay-seconds form.
    """
    try:
        seconds = float(str(value))
    except ValueError:
        return None

    if seconds < 0:
        return None

    return seconds
