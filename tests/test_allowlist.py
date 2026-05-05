"""Tests for the tenant + dataset allowlist loader."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from benchmark_service.auth import clear_allowlist_cache, load_allowlist


@pytest.fixture(autouse=True)
def reset_allowlist_cache() -> None:
    clear_allowlist_cache()


def test_load_allowlist_from_env_json(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"tenants": {"acme-corp": {"datasets": ["validation"]}}}
    monkeypatch.setenv("DESCOPE_TENANT_ALLOWLIST_JSON", json.dumps(payload))
    monkeypatch.delenv("DESCOPE_ALLOWLIST_PATH", raising=False)

    config = load_allowlist()

    assert "acme-corp" in config.tenants
    assert config.tenants["acme-corp"].datasets == ["validation"]


def test_load_allowlist_from_yaml_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    yaml_path = tmp_path / "allowlist.yaml"
    yaml_path.write_text(
        """
tenants:
  acme-corp:
    datasets:
      - validation
  vals-internal:
    datasets:
      - validation
      - test
""".lstrip()
    )

    monkeypatch.delenv("DESCOPE_TENANT_ALLOWLIST_JSON", raising=False)
    monkeypatch.setenv("DESCOPE_ALLOWLIST_PATH", str(yaml_path))

    config = load_allowlist()

    assert config.tenants["acme-corp"].datasets == ["validation"]
    assert config.tenants["vals-internal"].datasets == ["validation", "test"]


def test_load_allowlist_empty_when_neither_set(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.delenv("DESCOPE_TENANT_ALLOWLIST_JSON", raising=False)
    monkeypatch.delenv("DESCOPE_ALLOWLIST_PATH", raising=False)

    with caplog.at_level("WARNING"):
        config = load_allowlist()

    assert config.tenants == {}
    assert any("No tenant allowlist configured" in rec.message for rec in caplog.records)
