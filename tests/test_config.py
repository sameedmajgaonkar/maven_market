"""Tests for the config-driven loader (maven_market.config)."""
import pytest

from maven_market.config import load_config, get


def _write(dirpath, name, text):
    (dirpath / name).write_text(text)


def test_missing_env_file_raises(tmp_path):
    _write(tmp_path, "defaults.yml", "catalog: maven_market_uc\n")
    with pytest.raises(FileNotFoundError):
        load_config("nope", config_dir=tmp_path)


def test_env_override_wins_over_defaults(tmp_path):
    _write(tmp_path, "defaults.yml", "catalog: maven_market_uc\nschemas:\n  bronze: bronze\n  silver: silver\n")
    _write(tmp_path, "dev.yml", "schemas:\n  bronze: bronze_dev\n")
    cfg = load_config("dev", config_dir=tmp_path)
    # deep-merge: dev overrides only bronze, silver survives, catalog survives
    assert cfg["catalog"] == "maven_market_uc"
    assert cfg["schemas"]["bronze"] == "bronze_dev"
    assert cfg["schemas"]["silver"] == "silver"


def test_env_var_interpolation_with_and_without_default(tmp_path, monkeypatch):
    monkeypatch.setenv("WORKSPACE_HOST", "https://adb-123.azuredatabricks.net")
    _write(tmp_path, "defaults.yml", "host: ${WORKSPACE_HOST}\nscope: ${SECRET_SCOPE:maven_market}\n")
    _write(tmp_path, "dev.yml", "{}\n")
    cfg = load_config("dev", config_dir=tmp_path)
    assert cfg["host"] == "https://adb-123.azuredatabricks.net"
    assert cfg["scope"] == "maven_market"  # default used when env var unset


def test_missing_env_var_without_default_raises(tmp_path):
    _write(tmp_path, "defaults.yml", "host: ${DEFINITELY_UNSET_VAR}\n")
    _write(tmp_path, "dev.yml", "{}\n")
    with pytest.raises(KeyError):
        load_config("dev", config_dir=tmp_path)


def test_get_returns_nested_value_and_raises_on_missing():
    cfg = {"schemas": {"bronze": "bronze"}}
    assert get(cfg, "schemas.bronze") == "bronze"
    with pytest.raises(KeyError):
        get(cfg, "schemas.gold")
