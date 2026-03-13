from __future__ import annotations

import json
from collections.abc import Collection
from datetime import datetime, timedelta, timezone

import pytest

from cluster_metrics_platform.domain.models import (
    ClusterConfig,
    CollectorError,
    MetricPoint,
    TimeWindow,
)
from cluster_metrics_platform.orchestrator.models import DispatchSummary, DispatchTaskResult
from cluster_metrics_platform.orchestrator.scheduler import ScheduledCollector
from cluster_metrics_platform.services.collection_service import CollectionService


class FakeRepository:
    def __init__(self) -> None:
        self.point_batches: list[list[MetricPoint]] = []
        self.run_batches: list[list] = []

    def upsert_points(self, points: list[MetricPoint]) -> int:
        self.point_batches.append(list(points))
        return len(points)

    def save_run_records(self, runs: list) -> int:
        self.run_batches.append(list(runs))
        return len(runs)


class FakeDispatcher:
    def __init__(self, summary_factory) -> None:
        self.summary_factory = summary_factory
        self.calls: list[tuple[TimeWindow, list[ClusterConfig]]] = []
        self.progress_callbacks = []

    async def run_window(
        self,
        window: TimeWindow,
        clusters: list[ClusterConfig],
        progress_callback=None,
    ) -> DispatchSummary:
        self.calls.append((window, list(clusters)))
        self.progress_callbacks.append(progress_callback)
        return self.summary_factory(window, clusters)

    def collector_count(self) -> int:
        return 1


class FakeStatusService:
    def __init__(self) -> None:
        self.begin_calls = []
        self.advance_calls = []
        self.complete_calls = []
        self.fail_calls = []

    def begin_window(self, **kwargs) -> None:
        self.begin_calls.append(kwargs)

    def advance_window(self, **kwargs) -> None:
        self.advance_calls.append(kwargs)

    def complete_window(self, **kwargs) -> None:
        self.complete_calls.append(kwargs)

    def fail_window(self, **kwargs) -> None:
        self.fail_calls.append(kwargs)


def _cluster_loader() -> list[ClusterConfig]:
    return [
        ClusterConfig(group_name="g1", cluster_name="cluster-a", application_name="应用A"),
        ClusterConfig(group_name="g1", cluster_name="cluster-b", application_name="应用A"),
        ClusterConfig(
            group_name="g1",
            cluster_name="cluster-disabled",
            application_name="应用A",
            enabled=False,
        ),
    ]


def _point(cluster_name: str, window: TimeWindow, value: float) -> MetricPoint:
    return MetricPoint(
        cluster_name=cluster_name,
        bucket_time=window.bucket_time,
        window_start=window.start_time,
        window_end=window.end_time,
        metric_name="cpu_avg",
        metric_value=value,
        source_tool="fake",
    )


def _dispatch_result(
    *,
    cluster_name: str,
    collector_name: str,
    window: TimeWindow,
    status: str = "success",
    attempt_count: int = 1,
    value: float = 1.0,
    error: CollectorError | None = None,
) -> DispatchTaskResult:
    return DispatchTaskResult(
        cluster_name=cluster_name,
        collector_name=collector_name,
        bucket_time=window.bucket_time,
        status=status,
        attempt_count=attempt_count,
        started_at=window.start_time,
        finished_at=window.end_time,
        points=(_point(cluster_name, window, value),) if status != "failed" else (),
        error=error,
    )


def _summary_factory(window: TimeWindow, clusters: list[ClusterConfig]) -> DispatchSummary:
    results = tuple(
        _dispatch_result(
            cluster_name=cluster.cluster_name,
            collector_name="cpu",
            window=window,
            value=float(index + 1),
        )
        for index, cluster in enumerate(clusters)
    )
    return DispatchSummary(window=window, results=results)


@pytest.mark.asyncio
async def test_collection_service_loads_filters_and_persists(sample_window) -> None:
    repository = FakeRepository()
    dispatcher = FakeDispatcher(_summary_factory)
    status_service = FakeStatusService()
    service = CollectionService(
        _cluster_loader,
        dispatcher,
        repository,
        status_service=status_service,
    )

    execution = await service.collect_window(
        sample_window,
        cluster_names={"cluster-b", "cluster-disabled"},
    )

    assert execution.loaded_cluster_count == 3
    assert execution.selected_cluster_count == 1
    assert execution.points_written == 1
    assert execution.runs_written == 1

    dispatched_window, dispatched_clusters = dispatcher.calls[0]
    assert dispatched_window == sample_window
    assert [cluster.cluster_name for cluster in dispatched_clusters] == ["cluster-b"]

    assert len(repository.point_batches) == 1
    assert repository.point_batches[0][0].cluster_name == "cluster-b"
    assert repository.point_batches[0][0].application_name == "应用A"
    run_record = repository.run_batches[0][0]
    assert run_record.cluster_name == "cluster-b"
    assert run_record.application_name == "应用A"
    assert run_record.collector_name == "cpu"
    assert run_record.retry_count == 0
    assert status_service.begin_calls[0]["selected_cluster_count"] == 1
    assert status_service.complete_calls[0]["execution"].points_written == 1
    assert status_service.fail_calls == []


@pytest.mark.asyncio
async def test_collection_service_persists_failed_run_records(sample_window) -> None:
    repository = FakeRepository()

    def failed_summary(window: TimeWindow, clusters: list[ClusterConfig]) -> DispatchSummary:
        return DispatchSummary(
            window=window,
            results=(
                _dispatch_result(
                    cluster_name=clusters[0].cluster_name,
                    collector_name="cpu",
                    window=window,
                    status="failed",
                    attempt_count=2,
                    error=CollectorError(message="timed out", code="timeout"),
                ),
            ),
        )

    dispatcher = FakeDispatcher(failed_summary)
    status_service = FakeStatusService()
    service = CollectionService(
        _cluster_loader,
        dispatcher,
        repository,
        status_service=status_service,
    )

    execution = await service.collect_window(sample_window, cluster_names={"cluster-a"})

    assert execution.points_written == 0
    assert execution.runs_written == 1
    assert repository.point_batches == [[]]
    run_record = repository.run_batches[0][0]
    assert run_record.status == "failed"
    assert run_record.application_name == "应用A"
    assert run_record.retry_count == 1
    assert run_record.error_code == "timeout"
    assert run_record.error_message == "timed out"
    assert status_service.begin_calls[0]["total_tasks"] == 1
    assert status_service.complete_calls[0]["execution"].summary.failed_count == 1


@pytest.mark.asyncio
async def test_collection_service_marks_window_failed_on_dispatch_exception(sample_window) -> None:
    repository = FakeRepository()
    status_service = FakeStatusService()

    class RaisingDispatcher(FakeDispatcher):
        async def run_window(self, window, clusters, progress_callback=None):
            raise RuntimeError("dispatch exploded")

    dispatcher = RaisingDispatcher(_summary_factory)
    service = CollectionService(
        _cluster_loader,
        dispatcher,
        repository,
        status_service=status_service,
    )

    with pytest.raises(RuntimeError, match="dispatch exploded"):
        await service.collect_window(sample_window, cluster_names={"cluster-a"})

    assert status_service.begin_calls[0]["selected_cluster_count"] == 1
    assert status_service.fail_calls[0]["error_message"] == "dispatch exploded"


@pytest.mark.asyncio
async def test_collection_service_reloads_cluster_config_for_next_window(
    sample_window,
    tmp_path,
) -> None:
    config_path = tmp_path / "cluster.json"
    config_path.write_text(
        json.dumps(
            {"app-a": {"application_name": "应用A", "clusters": ["cluster-a"]}}
        ),
        encoding="utf-8",
    )

    from cluster_metrics_platform.config.cluster_loader import load_clusters

    repository = FakeRepository()
    dispatcher = FakeDispatcher(_summary_factory)
    service = CollectionService(
        lambda: load_clusters(config_path),
        dispatcher,
        repository,
    )

    first_execution = await service.collect_window(sample_window)

    config_path.write_text(
        json.dumps(
            {"app-b": {"application_name": "应用B", "clusters": ["cluster-b"]}}
        ),
        encoding="utf-8",
    )

    next_window = TimeWindow(
        bucket_time=sample_window.bucket_time + timedelta(minutes=5),
        start_time=sample_window.start_time + timedelta(minutes=5),
        end_time=sample_window.end_time + timedelta(minutes=5),
        window_seconds=sample_window.window_seconds,
    )

    second_execution = await service.collect_window(next_window)

    assert first_execution.selected_cluster_count == 1
    assert second_execution.selected_cluster_count == 1
    assert [cluster.cluster_name for _, clusters in dispatcher.calls for cluster in clusters] == [
        "cluster-a",
        "cluster-b",
    ]
    assert repository.point_batches[0][0].application_name == "应用A"
    assert repository.point_batches[1][0].application_name == "应用B"


@pytest.mark.asyncio
async def test_scheduled_collector_uses_closed_window_without_completion_drift() -> None:
    captured: list[tuple[TimeWindow, Collection[str] | None]] = []
    base_now = datetime(2026, 3, 12, 10, 7, tzinfo=timezone.utc)
    now_calls = 0

    def now_provider() -> datetime:
        nonlocal now_calls
        now_calls += 1
        return base_now + timedelta(minutes=10 * (now_calls - 1))

    async def collect_window(window: TimeWindow, cluster_names: Collection[str] | None):
        captured.append((window, cluster_names))
        return {"bucket_time": window.bucket_time}

    scheduler = ScheduledCollector(collect_window=collect_window, now_provider=now_provider)

    result = await scheduler.collect_once({"cluster-a"})

    assert now_calls == 1
    assert result == {"bucket_time": datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc)}
    scheduled_window, cluster_names = captured[0]
    assert scheduled_window.start_time == datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc)
    assert scheduled_window.end_time == datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc)
    assert cluster_names == {"cluster-a"}
