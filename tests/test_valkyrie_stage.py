"""Exercise the standalone staged reporter protocol."""

import base64
import hashlib
import hmac
import io
import json
import sys
from pathlib import Path

import pytest

from benchmark_service import valkyrie_stage


def test_stage_reporter_retransmits_identical_signed_frames_until_ack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stage_dir = tmp_path / "stage"
    stage_dir.mkdir()
    (stage_dir / "ack").mkdir()
    key = bytes.fromhex("11" * 32)
    (stage_dir / "key").write_text(key.hex())
    monkeypatch.setenv("VALKYRIE_STAGE_DIR", str(stage_dir))
    output = io.StringIO()
    monkeypatch.setattr(sys, "stdout", output)
    now = [0.0]
    sleeps = [0]
    event_started = [now[0]]

    def emitted() -> list[str]:
        lines = output.getvalue().splitlines(keepends=True)
        assert len(lines) % 2 == 0
        return ["".join(lines[index : index + 2]) for index in range(0, len(lines), 2)]

    def sleep(seconds: float) -> None:
        frames = emitted()
        assert frames
        if len(frames) in (2, 4):
            resend_interval = now[0] - event_started[0]
            assert resend_interval > 0
            (stage_dir / "ack" / str(len(frames) // 2)).touch()
            now[0] += resend_interval + 1
        sleeps[0] += 1
        assert sleeps[0] < 1000, "reporter never retransmitted the unacknowledged frame"
        now[0] += seconds

    monkeypatch.setattr(valkyrie_stage.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(valkyrie_stage.time, "sleep", sleep)
    reporter = valkyrie_stage.StageReporter()
    reporter.begin("round-one")
    begin_frames = emitted()
    assert len(begin_frames) == 2
    assert begin_frames[0] == begin_frames[1]

    event_started[0] = now[0]
    reporter.end()
    frames = emitted()
    assert len(frames) == 4
    assert frames[2] == frames[3]
    for expected_seq, wire in enumerate((frames[0], frames[2]), start=1):
        assert wire.startswith("\nVALKYRIE-STAGE/1 ") and wire.endswith("\n")
        encoded, mac = wire.removeprefix("\nVALKYRIE-STAGE/1 ").removesuffix("\n").split(" ")
        assert hmac.new(key, encoded.encode("ascii"), hashlib.sha256).hexdigest() == mac
        payload = json.loads(base64.urlsafe_b64decode(encoded + "==="))
        assert payload == {
            "seq": expected_seq,
            "event": "begin" if expected_seq == 1 else "end",
            "container": "round-one",
        }
