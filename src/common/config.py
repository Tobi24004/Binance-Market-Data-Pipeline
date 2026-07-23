"""Config loader: config/settings.yaml for structure, .env / real env vars for secrets.

Never put credentials in settings.yaml - it is committed to git. Only
POSTGRES_PASSWORD (and similar secrets) come from the environment.
"""
import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # python-dotenv is optional; real env vars still work
    pass

# src/common/config.py -> parents[2] is the project root.
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "settings.yaml"


def load_settings(config_path: Optional[os.PathLike] = None) -> Dict[str, Any]:
    """Load config/settings.yaml. Override the path via SETTINGS_PATH env var."""
    path = Path(config_path or os.environ.get("SETTINGS_PATH", DEFAULT_CONFIG_PATH))
    with open(path, "r", encoding="utf-8") as f:
        settings = yaml.safe_load(f) or {}
    return settings


def get_postgres_conn_params(settings: Dict[str, Any]) -> Dict[str, Any]:
    """Build psycopg2/JDBC-style connection params: env vars win over settings.yaml."""
    pg = settings.get("postgres", {})
    return {
        "host": os.environ.get("POSTGRES_HOST", pg.get("host", "localhost")),
        "port": int(os.environ.get("POSTGRES_PORT", pg.get("port", 5432))),
        "dbname": os.environ.get("POSTGRES_DB", pg.get("database", "binance")),
        "user": os.environ.get("POSTGRES_USER", pg.get("user", "binance")),
        "password": os.environ.get("POSTGRES_PASSWORD", ""),
    }


def get_postgres_jdbc_url(settings: Dict[str, Any]) -> str:
    params = get_postgres_conn_params(settings)
    return f"jdbc:postgresql://{params['host']}:{params['port']}/{params['dbname']}"


def get_kafka_bootstrap_servers(settings: Dict[str, Any]) -> str:
    return os.environ.get(
        "KAFKA_BOOTSTRAP_SERVERS", settings.get("kafka", {}).get("bootstrap_servers", "localhost:9092")
    )
