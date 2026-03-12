"""Integration tests for the Timescale metrics repository."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from cluster_metrics_platform.domain.models import CollectionRun, MetricPoint
from cluster_metrics_platform.storage.timescale_repo import TimescaleMetricsRepository


def _build_point(*, metric_value: float, labels: dict[str, str]) -> MetricPoint:
    bucket_time = datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc)
    return MetricPoint(
        cluster_name="lf-lan-ha1",
        bucket_time=bucket_time,
        window_start=bucket_time,
        window_end=datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc),
        metric_name="http_code_count",
        metric_value=metric_value,
        labels=labels,
        source_tool="http_code",
    )


@pytest.mark.integration
def test_upsert_points_is_idempotent_for_equivalent_labels(timescale_connection) -> None:
    repository = TimescaleMetricsRepository(timescale_connection)
    first = _build_point(metric_value=12.0, labels={"class": "2xx", "service": "edge"})
    second = _build_point(metric_value=18.5, labels={"service": "edge", "class": "2xx"})

    assert first.labels_fingerprint == second.labels_fingerprint
    assert repository.upsert_points([first]) == 1
    assert repository.upsert_points([second]) == 1

    with timescale_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT cluster_name, metric_name, metric_value, labels, labels_fingerprint
            FROM metric_points
            """
        )
        rows = cursor.fetchall()

    assert len(rows) == 1
    assert rows[0]["cluster_name"] == "lf-lan-ha1"
    assert rows[0]["metric_name"] == "http_code_count"
    assert rows[0]["metric_value"] == pytest.approx(18.5)
    assert rows[0]["labels"] == {"class": "2xx", "service": "edge"}
    assert rows[0]["labels_fingerprint"] == first.labels_fingerprint


@pytest.mark.integration
def test_save_run_records_persists_collection_metadata(timescale_connection) -> None:
    repository = TimescaleMetricsRepository(timescale_connection)
    run = CollectionRun(
        run_id=uuid4(),
        cluster_name="lf-lan-ha1",
        collector_name="cpu",
        bucket_time=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
        status="failed",
        retry_count=2,
        started_at=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
        finished_at=datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc),
        error_code="timeout",
        error_message="collector execution timed out",
    )

    assert repository.save_run_records([run]) == 1

    with timescale_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                run_id,
                cluster_name,
                collector_name,
                status,
                retry_count,
                error_code,
                error_message
            FROM collection_runs
            WHERE run_id = %s
            """,
            (run.run_id,),
        )
        stored = cursor.fetchone()

    assert stored["run_id"] == run.run_id
    assert stored["cluster_name"] == "lf-lan-ha1"
    assert stored["collector_name"] == "cpu"
    assert stored["status"] == "failed"
    assert stored["retry_count"] == 2
    assert stored["error_code"] == "timeout"
    assert stored["error_message"] == "collector execution timed out"
