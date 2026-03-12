"""Shared pytest configuration for the cluster metrics platform."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg import sql

from cluster_metrics_platform.domain.models import TimeWindow
from cluster_metrics_platform.storage.db import (
    DEFAULT_TEST_DATABASE_URL_ENV,
    DatabaseConfig,
    apply_sql_file,
    connect_db,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = PROJECT_ROOT / "sql"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture
def sample_window() -> TimeWindow:
    return TimeWindow(
        bucket_time=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
        start_time=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
        end_time=datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc),
        window_seconds=300,
    )


@pytest.fixture(scope="session")
def timescale_test_dsn() -> str:
    dsn = os.getenv(DEFAULT_TEST_DATABASE_URL_ENV)
    if not dsn:
        pytest.skip(
            f"{DEFAULT_TEST_DATABASE_URL_ENV} is not set; "
            "skipping TimescaleDB integration tests.",
        )
    return dsn


@pytest.fixture
def timescale_connection(timescale_test_dsn: str):
    connection = connect_db(
        DatabaseConfig(
            dsn=timescale_test_dsn,
            application_name="cluster-metrics-platform-tests",
        ),
        autocommit=True,
    )
    schema_name = f"cmp_test_{uuid4().hex[:12]}"

    with connection.cursor() as cursor:
        cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
        cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
        cursor.execute(
            sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema_name))
        )

    apply_sql_file(connection, SQL_DIR / "001_init.sql")

    try:
        yield connection
    finally:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema_name))
            )
            cursor.execute(
                """
                DO $$
                BEGIN
                    IF to_regclass('metric_rollup_1d') IS NOT NULL THEN
                        PERFORM remove_continuous_aggregate_policy(
                            'metric_rollup_1d',
                            if_exists => TRUE
                        );
                        PERFORM remove_retention_policy(
                            'metric_rollup_1d',
                            if_exists => TRUE
                        );
                    END IF;

                    IF to_regclass('metric_rollup_1h') IS NOT NULL THEN
                        PERFORM remove_continuous_aggregate_policy(
                            'metric_rollup_1h',
                            if_exists => TRUE
                        );
                        PERFORM remove_retention_policy(
                            'metric_rollup_1h',
                            if_exists => TRUE
                        );
                    END IF;

                    IF to_regclass('metric_points') IS NOT NULL THEN
                        PERFORM remove_retention_policy(
                            'metric_points',
                            if_exists => TRUE
                        );
                    END IF;
                END
                $$;
                """
            )
            cursor.execute("SET search_path TO public")
            cursor.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema_name)
                )
            )
        connection.close()
