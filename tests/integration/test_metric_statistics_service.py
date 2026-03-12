"""Integration tests for the standalone metric statistics query service."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from api.metric_statistics_service import MetricStatisticsQuery, MetricStatisticsService
from cluster_metrics_platform.domain.models import MetricPoint
from cluster_metrics_platform.storage.timescale_repo import TimescaleMetricsRepository


def _seed_statistics_query_points(connection) -> None:
    repository = TimescaleMetricsRepository(connection)
    base_time = datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc)
    points = []

    for minute_offset, value in ((0, 10.0), (5, 20.0), (10, 30.0), (15, 40.0)):
        bucket_time = base_time + timedelta(minutes=minute_offset)
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
def test_metric_statistics_service_returns_variance_stddev_and_range(
    timescale_connection,
) -> None:
    _seed_statistics_query_points(timescale_connection)
    service = MetricStatisticsService(timescale_connection)

    response = service.query_statistics(
        MetricStatisticsQuery(
            cluster_name="cluster-a",
            metric_name="cpu_avg",
            start_time=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
            end_time=datetime(2026, 3, 12, 10, 15, tzinfo=timezone.utc),
        )
    )

    assert response.status == "success"
    assert response.sample_count == 3
    assert response.variance_value == 66.67
    assert response.standard_deviation == 8.16
    assert response.range_value == 20.0


@pytest.mark.integration
def test_metric_statistics_service_returns_no_data_for_empty_range(
    timescale_connection,
) -> None:
    service = MetricStatisticsService(timescale_connection)

    response = service.query_statistics(
        MetricStatisticsQuery(
            cluster_name="cluster-a",
            metric_name="cpu_avg",
            start_time=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
            end_time=datetime(2026, 3, 12, 10, 15, tzinfo=timezone.utc),
        )
    )

    assert response.status == "no_data"
    assert response.variance_value is None
    assert response.standard_deviation is None
    assert response.range_value is None
    assert response.sample_count == 0
