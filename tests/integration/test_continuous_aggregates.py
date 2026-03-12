"""Integration tests for TimescaleDB continuous aggregates and policies."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from cluster_metrics_platform.domain.models import MetricPoint
from cluster_metrics_platform.storage.baseline_queries import initialize_rollups, refresh_rollups
from cluster_metrics_platform.storage.timescale_repo import TimescaleMetricsRepository


def _seed_hourly_points(connection) -> None:
    repository = TimescaleMetricsRepository(connection)
    start = datetime(2026, 3, 1, 10, 0, tzinfo=timezone.utc)
    points = []
    for offset_minutes, value in ((0, 10.0), (5, 20.0), (60, 30.0), (65, 40.0)):
        bucket_time = start + timedelta(minutes=offset_minutes)
        points.append(
            MetricPoint(
                cluster_name="cluster-a",
                bucket_time=bucket_time,
                window_start=bucket_time,
                window_end=bucket_time + timedelta(minutes=5),
                metric_name="cpu_avg",
                metric_value=value,
                source_tool="seed",
            )
        )
    repository.upsert_points(points)


@pytest.mark.integration
def test_rollups_refresh_and_policy_jobs_exist(timescale_connection) -> None:
    initialize_rollups(timescale_connection)

    with timescale_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT proc_name, COUNT(*) AS job_count
            FROM timescaledb_information.jobs
            WHERE hypertable_schema = current_schema()
            GROUP BY proc_name
            ORDER BY proc_name
            """
        )
        job_counts = {row["proc_name"]: row["job_count"] for row in cursor.fetchall()}

        cursor.execute(
            """
            SELECT remove_continuous_aggregate_policy(
                'metric_rollup_1h',
                if_exists => TRUE
            )
            """
        )
        cursor.execute(
            """
            SELECT remove_continuous_aggregate_policy(
                'metric_rollup_1d',
                if_exists => TRUE
            )
            """
        )

    _seed_hourly_points(timescale_connection)
    refresh_rollups(
        timescale_connection,
        start_time=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc),
        end_time=datetime(2026, 3, 3, 0, 0, tzinfo=timezone.utc),
    )

    with timescale_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT bucket_time, cluster_name, metric_name, sample_count, avg_value
            FROM metric_rollup_1h
            ORDER BY bucket_time
            """
        )
        hourly_rows = cursor.fetchall()

        cursor.execute(
            """
            SELECT bucket_time, cluster_name, metric_name, sample_count, avg_value
            FROM metric_rollup_1d
            ORDER BY bucket_time
            """
        )
        daily_rows = cursor.fetchall()

    assert hourly_rows == [
        {
            "bucket_time": datetime(2026, 3, 1, 10, 0, tzinfo=timezone.utc),
            "cluster_name": "cluster-a",
            "metric_name": "cpu_avg",
            "sample_count": 2,
            "avg_value": pytest.approx(15.0),
        },
        {
            "bucket_time": datetime(2026, 3, 1, 11, 0, tzinfo=timezone.utc),
            "cluster_name": "cluster-a",
            "metric_name": "cpu_avg",
            "sample_count": 2,
            "avg_value": pytest.approx(35.0),
        },
    ]
    assert daily_rows == [
        {
            "bucket_time": datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc),
            "cluster_name": "cluster-a",
            "metric_name": "cpu_avg",
            "sample_count": 4,
            "avg_value": pytest.approx(25.0),
        }
    ]
    assert job_counts["policy_refresh_continuous_aggregate"] == 2
    assert job_counts["policy_retention"] == 3
