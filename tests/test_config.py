"""Unit test for the config loader: config/settings.yaml + environment overrides.

Pure - reads the real settings.yaml shipped with the repo, no Docker/DB/Kafka
required (plan section 7.1).
"""
from common.config import get_kafka_bootstrap_servers, get_postgres_conn_params, load_settings


def test_load_settings_returns_expected_top_level_keys():
    settings = load_settings()
    for key in ("symbols", "binance", "kafka", "postgres", "spark", "producer"):
        assert key in settings


def test_load_settings_symbols_is_a_non_empty_list():
    settings = load_settings()
    assert isinstance(settings["symbols"], list)
    assert len(settings["symbols"]) > 0


def test_get_postgres_conn_params_uses_settings_yaml_defaults(monkeypatch):
    monkeypatch.delenv("POSTGRES_HOST", raising=False)
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    settings = load_settings()
    params = get_postgres_conn_params(settings)
    assert params["host"] == settings["postgres"]["host"]
    assert params["dbname"] == settings["postgres"]["database"]


def test_get_postgres_conn_params_env_var_overrides_yaml(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "override-host")
    monkeypatch.setenv("POSTGRES_PASSWORD", "super-secret")
    settings = load_settings()
    params = get_postgres_conn_params(settings)
    assert params["host"] == "override-host"
    assert params["password"] == "super-secret"


def test_get_kafka_bootstrap_servers_falls_back_to_settings(monkeypatch):
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    settings = load_settings()
    assert get_kafka_bootstrap_servers(settings) == settings["kafka"]["bootstrap_servers"]


def test_get_kafka_bootstrap_servers_env_var_overrides_yaml(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "override-broker:9092")
    settings = load_settings()
    assert get_kafka_bootstrap_servers(settings) == "override-broker:9092"
