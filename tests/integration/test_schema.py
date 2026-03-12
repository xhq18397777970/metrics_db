"""Integration tests for TimescaleDB schema initialization."""

from __future__ import annotations

import pytest


@pytest.mark.integration
def test_init_migration_creates_tables_and_hypertable(timescale_connection) -> None:
    with timescale_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                to_regclass('metric_points') AS metric_points_regclass,
                to_regclass('collection_runs') AS collection_runs_regclass
            """
        )
        regclasses = cursor.fetchone()
        assert regclasses["metric_points_regclass"] == "metric_points"
        assert regclasses["collection_runs_regclass"] == "collection_runs"

        cursor.execute(
            """
            SELECT hypertable_name
            FROM timescaledb_information.hypertables
            WHERE hypertable_schema = current_schema()
              AND hypertable_name = 'metric_points'
            """
        )
        hypertable = cursor.fetchone()
        assert hypertable["hypertable_name"] == "metric_points"

        cursor.execute(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = current_schema()
              AND tablename = 'collection_runs'
            """
        )
        collection_run_indexes = {row["indexname"] for row in cursor.fetchall()}

        cursor.execute(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = current_schema()
              AND tablename = 'metric_points'
            """
        )
        metric_indexes = {row["indexname"] for row in cursor.fetchall()}

    assert {
        "idx_collection_runs_bucket_status",
        "idx_collection_runs_cluster_bucket",
        "idx_collection_runs_collector_bucket",
    }.issubset(collection_run_indexes)
    assert "idx_metric_points_cluster_metric_bucket" in metric_indexes
