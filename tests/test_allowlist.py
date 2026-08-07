"""Tests for the tenant + dataset allowlist loader."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from benchmark_service.auth import clear_allowlist_cache, get_tenant_config, load_allowlist


@pytest.fixture(autouse=True)
def reset_allowlist_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_allowlist_cache()
    monkeypatch.delenv("BENCHMARK_CATALOG_API_URL", raising=False)
    monkeypatch.delenv("SERVICE_NAME", raising=False)


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


def test_tenant_config_parses_trial_mode_from_allowlist_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps(
            {
                "tenants": {
                    "acme-corp": {"datasets": ["validation"]},
                    "trial": {"datasets": ["trial"], "trial_mode": True},
                }
            }
        ),
    )
    allowlist = load_allowlist()
    assert allowlist.tenants["acme-corp"].trial_mode is False
    assert allowlist.tenants["trial"].trial_mode is True


def test_get_tenant_config_returns_none_for_unknown_tenant(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps({"tenants": {"acme-corp": {"datasets": ["validation"]}}}),
    )
    assert get_tenant_config("ghost") is None
    cfg = get_tenant_config("acme-corp")
    assert cfg is not None and cfg.trial_mode is False
