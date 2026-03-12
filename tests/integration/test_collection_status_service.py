from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from cluster_metrics_platform.domain.models import TimeWindow
from cluster_metrics_platform.orchestrator.models import DispatchSummary, DispatchTaskResult
from cluster_metrics_platform.services.collection_service import CollectionExecution
from cluster_metrics_platform.services.collection_status_service import CollectionStatusService


def _window() -> TimeWindow:
    return TimeWindow(
        bucket_time=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
        start_time=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
        end_time=datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc),
        window_seconds=300,
    )


def _summary(window: TimeWindow) -> DispatchSummary:
    return DispatchSummary(
        window=window,
        results=(
            DispatchTaskResult(
                cluster_name="cluster-a",
                collector_name="cpu",
                bucket_time=window.bucket_time,
                status="success",
                attempt_count=1,
                started_at=window.start_time,
                finished_at=window.end_time,
            ),
            DispatchTaskResult(
                cluster_name="cluster-a",
                collector_name="tp",
                bucket_time=window.bucket_time,
                status="failed",
                attempt_count=1,
                started_at=window.start_time,
                finished_at=window.end_time,
            ),
        ),
    )


@pytest.mark.integration
def test_collection_status_service_tracks_window_progress(timescale_connection) -> None:
    service = CollectionStatusService(timescale_connection)
    window = _window()
    started_at = datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc)

    service.begin_window(
        window=window,
        selected_cluster_count=70,
        total_tasks=280,
        started_at=started_at,
        step_minutes=5,
    )
    service.advance_window(
        window=window,
        selected_cluster_count=70,
        total_tasks=280,
        completed_tasks=140,
        success_count=130,
        partial_success_count=5,
        failed_count=5,
        started_at=started_at,
        step_minutes=5,
        last_error="tp timeout",
    )

    snapshot = service.get_dashboard_snapshot(limit=5)

    assert snapshot["scheduler"]["status"] == "running"
    assert snapshot["scheduler"]["completed_tasks"] == 140
    assert snapshot["scheduler"]["remaining_tasks"] == 140
    assert snapshot["windows"][0]["status"] == "running"
    assert snapshot["windows"][0]["remaining_tasks"] == 140
    assert snapshot["windows"][0]["last_error"] == "tp timeout"


@pytest.mark.integration
def test_collection_status_service_marks_completed_window_and_stale_scheduler(
    timescale_connection,
) -> None:
    service = CollectionStatusService(timescale_connection, stale_after_seconds=1)
    window = _window()
    started_at = datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc)
    execution = CollectionExecution(
        window=window,
        summary=_summary(window),
        loaded_cluster_count=70,
        selected_cluster_count=70,
        points_written=546,
        runs_written=280,
    )

    service.begin_window(
        window=window,
        selected_cluster_count=70,
        total_tasks=280,
        started_at=started_at,
        step_minutes=5,
    )
    service.complete_window(
        execution=execution,
        selected_cluster_count=70,
        started_at=started_at,
        step_minutes=5,
    )

    with timescale_connection.cursor() as cursor:
        cursor.execute(
            "UPDATE scheduler_runtime SET last_heartbeat_at = %s",
            (datetime.now(timezone.utc) - timedelta(seconds=5),),
        )

    snapshot = service.get_dashboard_snapshot(limit=5)

    assert snapshot["scheduler"]["status"] == "stopped"
    assert snapshot["scheduler"]["is_stale"] is True
    assert snapshot["windows"][0]["status"] == "completed_with_errors"
    assert snapshot["windows"][0]["points_written"] == 546
    assert snapshot["windows"][0]["runs_written"] == 280
