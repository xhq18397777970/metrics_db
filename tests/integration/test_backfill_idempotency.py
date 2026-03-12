"""Integration tests for backfill idempotency against TimescaleDB."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from cluster_metrics_platform.domain.models import (
    ClusterConfig,
    CollectorResult,
    MetricPoint,
    TimeWindow,
)
from cluster_metrics_platform.services.backfill_service import BackfillService
from cluster_metrics_platform.services.collection_service import CollectionService
from cluster_metrics_platform.storage.timescale_repo import TimescaleMetricsRepository


class FakeDispatcher:
    def __init__(self) -> None:
        self.calls: list[tuple[TimeWindow, list[str]]] = []

    async def run_window(
        self,
        window: TimeWindow,
        clusters: list[ClusterConfig],
    ):
        self.calls.append((window, [cluster.cluster_name for cluster in clusters]))
        results = []
        for cluster in clusters:
            result = CollectorResult(
                status="success",
                points=[
                    MetricPoint(
                        cluster_name=cluster.cluster_name,
                        bucket_time=window.bucket_time,
                        window_start=window.start_time,
                        window_end=window.end_time,
                        metric_name="cpu_avg",
                        metric_value=float(window.bucket_time.minute + 1),
                        source_tool="fake",
                    )
                ],
            )
            results.append(
                _dispatch_result(cluster.cluster_name, window, result.points[0])
            )
        from cluster_metrics_platform.orchestrator.models import DispatchSummary

        return DispatchSummary(window=window, results=tuple(results))


def _dispatch_result(cluster_name: str, window: TimeWindow, point: MetricPoint):
    from cluster_metrics_platform.orchestrator.models import DispatchTaskResult

    return DispatchTaskResult(
        cluster_name=cluster_name,
        collector_name="cpu",
        bucket_time=window.bucket_time,
        status="success",
        attempt_count=1,
        started_at=window.start_time,
        finished_at=window.end_time,
        points=(point,),
        error=None,
    )


def _cluster_loader() -> list[ClusterConfig]:
    return [
        ClusterConfig(group_name="g1", cluster_name="cluster-a"),
        ClusterConfig(group_name="g1", cluster_name="cluster-b"),
    ]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_backfill_reuses_collection_pipeline_without_duplicate_points(
    timescale_connection,
) -> None:
    repository = TimescaleMetricsRepository(timescale_connection)
    dispatcher = FakeDispatcher()
    collection_service = CollectionService(_cluster_loader, dispatcher, repository)
    backfill_service = BackfillService(collection_service)

    start_time = datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc)
    end_time = datetime(2026, 3, 12, 10, 10, tzinfo=timezone.utc)

    first = await backfill_service.backfill(start_time, end_time)
    second = await backfill_service.backfill(start_time, end_time)

    assert first.total_windows == 2
    assert second.total_windows == 2
    assert len(dispatcher.calls) == 4
    assert dispatcher.calls[0][1] == ["cluster-a", "cluster-b"]
    assert dispatcher.calls[1][1] == ["cluster-a", "cluster-b"]

    with timescale_connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS count FROM metric_points")
        count_row = cursor.fetchone()
        cursor.execute(
            """
            SELECT bucket_time, cluster_name, metric_value
            FROM metric_points
            ORDER BY bucket_time, cluster_name
            """
        )
        stored_rows = cursor.fetchall()

    assert count_row["count"] == 4
    assert stored_rows == [
        {
            "bucket_time": datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
            "cluster_name": "cluster-a",
            "metric_value": pytest.approx(1.0),
        },
        {
            "bucket_time": datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
            "cluster_name": "cluster-b",
            "metric_value": pytest.approx(1.0),
        },
        {
            "bucket_time": datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc),
            "cluster_name": "cluster-a",
            "metric_value": pytest.approx(6.0),
        },
        {
            "bucket_time": datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc),
            "cluster_name": "cluster-b",
            "metric_value": pytest.approx(6.0),
        },
    ]

    with timescale_connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS count FROM collection_runs")
        run_count = cursor.fetchone()

    assert run_count["count"] == 8
