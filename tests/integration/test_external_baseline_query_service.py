"""Integration tests for the standalone baseline average query service."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from api.baseline_query_service import BaselineAverageQuery, BaselineQueryService
from cluster_metrics_platform.domain.models import MetricPoint
from cluster_metrics_platform.storage.timescale_repo import TimescaleMetricsRepository


def _seed_average_query_points(connection) -> None:
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
def test_standalone_baseline_query_service_returns_range_average(
    timescale_connection,
) -> None:
    _seed_average_query_points(timescale_connection)
    service = BaselineQueryService(timescale_connection)

    response = service.query_average(
        BaselineAverageQuery(
            cluster_name="cluster-a",
            metric_name="cpu_avg",
            start_time=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
            end_time=datetime(2026, 3, 12, 10, 15, tzinfo=timezone.utc),
        )
    )

    assert response.status == "success"
    assert response.baseline_value == pytest.approx(20.0)
    assert response.sample_count == 3


@pytest.mark.integration
def test_standalone_baseline_query_service_returns_no_data_for_empty_range(
    timescale_connection,
) -> None:
    service = BaselineQueryService(timescale_connection)

    response = service.query_average(
        BaselineAverageQuery(
            cluster_name="cluster-a",
            metric_name="cpu_avg",
            start_time=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
            end_time=datetime(2026, 3, 12, 10, 15, tzinfo=timezone.utc),
        )
    )

    assert response.status == "no_data"
    assert response.baseline_value is None
    assert response.sample_count == 0

