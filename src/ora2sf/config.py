from __future__ import annotations

"""Configuration loader and validator."""

import os
import yaml
from pathlib import Path
from dataclasses import dataclass, field


@dataclass
class OracleConfig:
    host: str
    port: int
    service_name: str
    user: str
    password: str

    @property
    def dsn(self) -> str:
        return f"{self.host}:{self.port}/{self.service_name}"


@dataclass
class SnowflakeConfig:
    account: str
    user: str
    password: str
    role: str
    database: str
    schema: str
    warehouse: str
    stage: str


@dataclass
class TableConfig:
    source_schema: str
    source_table: str
    target_table: str
    cdc_method: str = "full_only"  # scn | timestamp | full_only
    cdc_column: str | None = None
    parallel_chunks: int = 1


@dataclass
class Settings:
    batch_size: int = 500_000
    max_parallel: int = 4
    parquet_compression: str = "snappy"
    checkpoint_file: str = ".ora2sf_checkpoint.json"
    log_level: str = "INFO"


@dataclass
class AppConfig:
    oracle: OracleConfig
    snowflake: SnowflakeConfig
    tables: list[TableConfig]
    settings: Settings


def _resolve_env(value: str) -> str:
    """Resolve ${ENV_VAR} references in config values."""
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        env_key = value[2:-1]
        resolved = os.environ.get(env_key)
        if resolved is None:
            raise ValueError(f"Environment variable {env_key} not set (referenced in config)")
        return resolved
    return value


def load_config(config_path: str | Path) -> AppConfig:
    """Load and validate configuration from YAML file."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path) as f:
        raw = yaml.safe_load(f)

    # Resolve env vars in oracle/snowflake sections
    ora = raw["oracle"]
    sf = raw["snowflake"]

    oracle_cfg = OracleConfig(
        host=_resolve_env(ora["host"]),
        port=int(ora["port"]),
        service_name=_resolve_env(ora["service_name"]),
        user=_resolve_env(ora["user"]),
        password=_resolve_env(ora["password"]),
    )

    sf_cfg = SnowflakeConfig(
        account=_resolve_env(sf["account"]),
        user=_resolve_env(sf["user"]),
        password=_resolve_env(sf["password"]),
        role=sf.get("role", "SYSADMIN"),
        database=sf["database"],
        schema=sf["schema"],
        warehouse=sf["warehouse"],
        stage=sf.get("stage", "ORA_STAGE"),
    )

    tables = [
        TableConfig(
            source_schema=t["source_schema"],
            source_table=t["source_table"],
            target_table=t.get("target_table", t["source_table"]),
            cdc_method=t.get("cdc_method", "full_only"),
            cdc_column=t.get("cdc_column"),
            parallel_chunks=t.get("parallel_chunks", 1),
        )
        for t in raw.get("tables", [])
    ]

    settings_raw = raw.get("settings", {})
    settings = Settings(
        batch_size=settings_raw.get("batch_size", 500_000),
        max_parallel=settings_raw.get("max_parallel", 4),
        parquet_compression=settings_raw.get("parquet_compression", "snappy"),
        checkpoint_file=settings_raw.get("checkpoint_file", ".ora2sf_checkpoint.json"),
        log_level=settings_raw.get("log_level", "INFO"),
    )

    return AppConfig(oracle=oracle_cfg, snowflake=sf_cfg, tables=tables, settings=settings)
