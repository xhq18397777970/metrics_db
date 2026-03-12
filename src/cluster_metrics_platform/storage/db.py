"""TimescaleDB connection and migration helpers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row

DEFAULT_DATABASE_URL_ENV = "CLUSTER_METRICS_DATABASE_URL"
DEFAULT_TEST_DATABASE_URL_ENV = "TIMESCALE_TEST_DSN"


@dataclass(frozen=True, slots=True)
class DatabaseConfig:
    """Runtime database connection settings."""

    dsn: str
    application_name: str = "cluster-metrics-platform"
    connect_timeout: int = 10

    @classmethod
    def from_env(
        cls,
        env_var: str = DEFAULT_DATABASE_URL_ENV,
        *,
        default_dsn: str | None = None,
        application_name: str = "cluster-metrics-platform",
        connect_timeout: int = 10,
    ) -> "DatabaseConfig":
        dsn = os.getenv(env_var, default_dsn)
        if not dsn:
            raise ValueError(f"database dsn is required via env var {env_var}")
        return cls(
            dsn=dsn,
            application_name=application_name,
            connect_timeout=connect_timeout,
        )


def connect_db(
    config: DatabaseConfig,
    *,
    autocommit: bool = False,
) -> Connection:
    """Create a psycopg connection with dict-like row access."""

    return psycopg.connect(
        config.dsn,
        autocommit=autocommit,
        connect_timeout=config.connect_timeout,
        application_name=config.application_name,
        row_factory=dict_row,
    )


def read_sql_file(path: str | Path) -> str:
    """Load a SQL file from disk."""

    return Path(path).read_text(encoding="utf-8")


def apply_sql_file(connection: Connection, path: str | Path) -> None:
    """Execute a SQL migration file against an open connection."""

    sql_text = read_sql_file(path)
    with connection.cursor() as cursor:
        cursor.execute(sql_text)
