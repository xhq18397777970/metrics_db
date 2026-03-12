from __future__ import annotations

import threading
import time

import pytest

from cluster_metrics_platform.collectors.base import Collector
from cluster_metrics_platform.collectors.registry import CollectorRegistry
from cluster_metrics_platform.config.cluster_loader import ClusterConfig
from cluster_metrics_platform.domain.models import CollectorResult, MetricPoint
from cluster_metrics_platform.orchestrator.dispatcher import Dispatcher


class FakeCollector(Collector):
    def __init__(self, name: str, handler) -> None:
        self.name = name
        self._handler = handler

    def collect(self, cluster: str, window):
        return self._handler(cluster, window)


def _clusters(count: int) -> list[ClusterConfig]:
    return [
        ClusterConfig(group_name="group-a", cluster_name=f"cluster-{index}")
        for index in range(count)
    ]


def _point(cluster: str, window, metric_name: str = "cpu_avg", value: float = 1.0) -> MetricPoint:
    return MetricPoint(
        cluster_name=cluster,
        bucket_time=window.bucket_time,
        window_start=window.start_time,
        window_end=window.end_time,
        metric_name=metric_name,
        metric_value=value,
        source_tool="fake",
    )


@pytest.mark.asyncio
async def test_dispatcher_limits_concurrency(sample_window) -> None:
    active = 0
    max_seen = 0
    lock = threading.Lock()

    def handler(cluster: str, window) -> CollectorResult:
        nonlocal active, max_seen
        with lock:
            active += 1
            max_seen = max(max_seen, active)
        time.sleep(0.02)
        with lock:
            active -= 1
        return CollectorResult(status="success", points=[_point(cluster, window)])

    registry = CollectorRegistry()
    registry.register(FakeCollector("slow", handler))
    dispatcher = Dispatcher(registry=registry, max_concurrency=2, task_timeout_seconds=1.0)

    summary = await dispatcher.run_window(sample_window, _clusters(5))

    assert summary.total_tasks == 5
    assert summary.success_count == 5
    assert max_seen <= 2


@pytest.mark.asyncio
async def test_dispatcher_retries_failed_collectors(sample_window) -> None:
    attempts: dict[str, int] = {}

    def handler(cluster: str, window) -> CollectorResult:
        attempts[cluster] = attempts.get(cluster, 0) + 1
        if attempts[cluster] == 1:
            return CollectorResult(status="failed")
        return CollectorResult(status="success", points=[_point(cluster, window, value=2.0)])

    registry = CollectorRegistry()
    registry.register(FakeCollector("retrying", handler))
    dispatcher = Dispatcher(registry=registry, retry_limit=1, task_timeout_seconds=1.0)

    summary = await dispatcher.run_window(sample_window, _clusters(1))
    result = summary.results[0]

    assert summary.success_count == 1
    assert result.attempt_count == 2
    assert attempts["cluster-0"] == 2
    assert result.points[0].metric_value == 2.0


@pytest.mark.asyncio
async def test_dispatcher_marks_timeouts_as_failed(sample_window) -> None:
    def handler(cluster: str, window) -> CollectorResult:
        time.sleep(0.05)
        return CollectorResult(status="success", points=[_point(cluster, window)])

    registry = CollectorRegistry()
    registry.register(FakeCollector("timeout", handler))
    dispatcher = Dispatcher(registry=registry, task_timeout_seconds=0.01)

    summary = await dispatcher.run_window(sample_window, _clusters(1))
    result = summary.results[0]

    assert summary.failed_count == 1
    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "timeout"


@pytest.mark.asyncio
async def test_dispatcher_counts_partial_success(sample_window) -> None:
    def handler(cluster: str, window) -> CollectorResult:
        return CollectorResult(
            status="partial_success",
            points=[_point(cluster, window)],
        )

    registry = CollectorRegistry()
    registry.register(FakeCollector("partial", handler))
    dispatcher = Dispatcher(registry=registry, task_timeout_seconds=1.0)

    summary = await dispatcher.run_window(sample_window, _clusters(1))
    result = summary.results[0]

    assert summary.partial_success_count == 1
    assert summary.total_points == 1
    assert result.status == "partial_success"


@pytest.mark.asyncio
async def test_dispatcher_continues_when_one_collector_or_cluster_fails(sample_window) -> None:
    def flaky_handler(cluster: str, window) -> CollectorResult:
        if cluster == "cluster-0":
            return CollectorResult(status="failed")
        return CollectorResult(status="success", points=[_point(cluster, window, value=3.0)])

    def stable_handler(cluster: str, window) -> CollectorResult:
        return CollectorResult(
            status="success",
            points=[_point(cluster, window, metric_name="qps_avg")],
        )

    registry = CollectorRegistry()
    registry.register(FakeCollector("flaky", flaky_handler))
    registry.register(FakeCollector("stable", stable_handler))
    dispatcher = Dispatcher(registry=registry, task_timeout_seconds=1.0)

    summary = await dispatcher.run_window(sample_window, _clusters(2))

    assert summary.total_tasks == 4
    assert summary.failed_count == 1
    assert summary.success_count == 3
    stable_results = [result for result in summary.results if result.collector_name == "stable"]
    assert len(stable_results) == 2


@pytest.mark.asyncio
async def test_dispatcher_emits_progress_updates(sample_window) -> None:
    progress_events = []

    def handler(cluster: str, window) -> CollectorResult:
        return CollectorResult(status="success", points=[_point(cluster, window)])

    registry = CollectorRegistry()
    registry.register(FakeCollector("cpu", handler))
    dispatcher = Dispatcher(registry=registry, task_timeout_seconds=1.0)

    summary = await dispatcher.run_window(
        sample_window,
        _clusters(2),
        progress_callback=progress_events.append,
    )

    assert summary.total_tasks == 2
    assert [event.completed_tasks for event in progress_events] == [1, 2]
    assert progress_events[-1].success_count == 2
