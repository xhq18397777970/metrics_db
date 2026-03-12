"""Integration tests for the baseline query service."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from cluster_metrics_platform.domain.models import BaselineQuery, MetricPoint
from cluster_metrics_platform.services.baseline_service import BaselineService
from cluster_metrics_platform.storage.baseline_queries import initialize_rollups
from cluster_metrics_platform.storage.timescale_repo import TimescaleMetricsRepository


def _seed_baseline_points(connection) -> None:
    repository = TimescaleMetricsRepository(connection)
    base_day = datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc)
    points: list[MetricPoint] = []

    for day_offset, values in (
        (1, (10.0, 20.0)),
        (2, (30.0, 40.0)),
        (3, (50.0, 60.0)),
    ):
        window_day = base_day - timedelta(days=day_offset)
        for minute_offset, value in ((0, values[0]), (5, values[1])):
            bucket_time = window_day + timedelta(minutes=minute_offset)
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

    for minute_offset, value in ((0, 111.0), (5, 222.0)):
        bucket_time = base_day + timedelta(minutes=minute_offset)
        points.append(
            MetricPoint(
                cluster_name="cluster-a",
                bucket_time=bucket_time,
                window_start=bucket_time,
                window_end=bucket_time + timedelta(minutes=5),
                metric_name="qps_avg",
                metric_value=value,
                source_tool="seed",
            )
        )

    repository.upsert_points(points)


@pytest.mark.integration
def test_historical_range_baseline_returns_summary_and_points(timescale_connection) -> None:
    initialize_rollups(timescale_connection)
    _seed_baseline_points(timescale_connection)
    service = BaselineService(timescale_connection)

    response = service.query_baseline(
        BaselineQuery(
            cluster_name="cluster-a",
            metric_name="cpu_avg",
            start_time=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
            end_time=datetime(2026, 3, 12, 10, 10, tzinfo=timezone.utc),
            mode="historical_range",
            lookback_days=3,
        )
    )

    assert response.status == "success"
    assert response.baseline_summary == {
        "avg": pytest.approx(35.0),
        "p50": pytest.approx(35.0),
        "p95": pytest.approx(57.5),
    }
    assert response.points == (
        {
            "bucket_time": datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
            "avg": pytest.approx(30.0),
            "p50": pytest.approx(30.0),
            "p95": pytest.approx(48.0),
        },
        {
            "bucket_time": datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc),
            "avg": pytest.approx(40.0),
            "p50": pytest.approx(40.0),
            "p95": pytest.approx(58.0),
        },
    )


@pytest.mark.integration
def test_last_week_same_range_baseline_shifts_the_query_window(timescale_connection) -> None:
    initialize_rollups(timescale_connection)
    _seed_baseline_points(timescale_connection)
    service = BaselineService(timescale_connection)

    response = service.query_baseline(
        BaselineQuery(
            cluster_name="cluster-a",
            metric_name="qps_avg",
            start_time=datetime(2026, 3, 19, 10, 0, tzinfo=timezone.utc),
            end_time=datetime(2026, 3, 19, 10, 10, tzinfo=timezone.utc),
            mode="last_week_same_range",
        )
    )

    assert response.status == "success"
    assert response.baseline_summary == {
        "avg": pytest.approx(166.5),
        "p50": pytest.approx(166.5),
        "p95": pytest.approx(216.45),
    }
    assert response.points == (
        {
            "bucket_time": datetime(2026, 3, 19, 10, 0, tzinfo=timezone.utc),
            "avg": pytest.approx(111.0),
            "p50": pytest.approx(111.0),
            "p95": pytest.approx(111.0),
        },
        {
            "bucket_time": datetime(2026, 3, 19, 10, 5, tzinfo=timezone.utc),
            "avg": pytest.approx(222.0),
            "p50": pytest.approx(222.0),
            "p95": pytest.approx(222.0),
        },
    )


@pytest.mark.integration
def test_baseline_service_returns_no_data_when_no_history_exists(timescale_connection) -> None:
    initialize_rollups(timescale_connection)
    service = BaselineService(timescale_connection)

    response = service.query_baseline(
        BaselineQuery(
            cluster_name="cluster-a",
            metric_name="cpu_avg",
            start_time=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
            end_time=datetime(2026, 3, 12, 10, 10, tzinfo=timezone.utc),
            mode="historical_range",
            lookback_days=7,
        )
    )

    assert response.status == "no_data"
    assert response.baseline_summary == {}
    assert response.points == ()
