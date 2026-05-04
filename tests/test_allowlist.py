"""Tests for the tenant + dataset allowlist loader and Pydantic models."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from benchmark_service.auth import AllowlistConfig, TenantConfig, clear_allowlist_cache, load_allowlist


@pytest.fixture(autouse=True)
def reset_allowlist_cache() -> None:
    clear_allowlist_cache()


def test_tenant_config_default_datasets_is_empty() -> None:
    config = TenantConfig()
    assert config.datasets == []


def test_tenant_config_accepts_dataset_list() -> None:
    config = TenantConfig(datasets=["validation", "test"])
    assert config.datasets == ["validation", "test"]


def test_allowlist_config_default_tenants_is_empty() -> None:
    config = AllowlistConfig()
    assert config.tenants == {}


def test_allowlist_config_parses_nested_tenants() -> None:
    config = AllowlistConfig.model_validate(
        {
            "tenants": {
                "acme-corp": {"datasets": ["validation"]},
                "vals-internal": {"datasets": ["validation", "test", "default"]},
            }
        }
    )
    assert set(config.tenants.keys()) == {"acme-corp", "vals-internal"}
    assert config.tenants["acme-corp"].datasets == ["validation"]
    assert config.tenants["vals-internal"].datasets == ["validation", "test", "default"]


def test_allowlist_config_rejects_unknown_top_level_keys() -> None:
    # Strict-ish: future-proofing means we explicitly accept "tenants" only.
    # Pydantic v2 ignores unknown keys by default; this test pins the behavior we want.
    config = AllowlistConfig.model_validate({"tenants": {}, "unknown_field": 123})
    assert config.tenants == {}


def test_load_allowlist_from_env_json(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"tenants": {"acme-corp": {"datasets": ["validation"]}}}
    monkeypatch.setenv("DESCOPE_TENANT_ALLOWLIST_JSON", json.dumps(payload))
    monkeypatch.delenv("DESCOPE_ALLOWLIST_PATH", raising=False)

    config = load_allowlist()

    assert "acme-corp" in config.tenants
    assert config.tenants["acme-corp"].datasets == ["validation"]


def test_load_allowlist_env_var_invalid_json_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DESCOPE_TENANT_ALLOWLIST_JSON", "{not valid json")
    monkeypatch.delenv("DESCOPE_ALLOWLIST_PATH", raising=False)

    with pytest.raises(ValueError):
        load_allowlist()


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


def test_load_allowlist_yaml_file_missing_raises(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    missing = tmp_path / "does-not-exist.yaml"
    monkeypatch.delenv("DESCOPE_TENANT_ALLOWLIST_JSON", raising=False)
    monkeypatch.setenv("DESCOPE_ALLOWLIST_PATH", str(missing))

    with pytest.raises(ValueError):
        load_allowlist()


def test_load_allowlist_yaml_file_malformed_raises(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    yaml_path = tmp_path / "allowlist.yaml"
    yaml_path.write_text("tenants: [this is not a dict]")

    monkeypatch.delenv("DESCOPE_TENANT_ALLOWLIST_JSON", raising=False)
    monkeypatch.setenv("DESCOPE_ALLOWLIST_PATH", str(yaml_path))

    with pytest.raises(ValueError):
        load_allowlist()


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
