"""Benchmark-owned stage reports over the agent command's output stream."""

import base64
import hashlib
import hmac
import json
import os
import sys
import time
from pathlib import Path


class StageReporter:
    """Send ordered generation boundaries and wait for Tracker acknowledgments."""

    def __init__(self) -> None:
        self._stage_dir = Path(os.environ["VALKYRIE_STAGE_DIR"])
        self._key = bytes.fromhex((self._stage_dir / "key").read_text())
        self._seq = 0
        self._container: str | None = None

    def begin(self, container: str | None) -> None:
        self._container = container
        self._report("begin", container)

    def end(self) -> None:
        self._report("end", self._container)
        self._container = None

    def _report(self, event: str, container: str | None) -> None:
        self._seq += 1
        payload = json.dumps(
            {"seq": self._seq, "event": event, "container": container},
            separators=(",", ":"),
        ).encode("ascii")
        frame = base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")
        mac = hmac.new(self._key, frame.encode("ascii"), hashlib.sha256).hexdigest()
        print(f"\nVALKYRIE-STAGE/1 {frame} {mac}", file=sys.stdout, flush=True)
        ack = self._stage_dir / "ack" / str(self._seq)
        while not ack.exists():
            time.sleep(0.1)


def stage_reporter_source() -> bytes:
    """Return this standalone module's bytes for upload into a sandbox."""
    return Path(__file__).read_bytes()
