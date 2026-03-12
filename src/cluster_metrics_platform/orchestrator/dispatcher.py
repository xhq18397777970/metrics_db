"""Concurrent dispatcher for collector execution."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from cluster_metrics_platform.collectors.base import Collector
from cluster_metrics_platform.collectors.registry import CollectorRegistry
from cluster_metrics_platform.domain.models import (
    ClusterConfig,
    CollectorError,
    CollectorResult,
    TimeWindow,
)
from cluster_metrics_platform.orchestrator.models import DispatchSummary, DispatchTaskResult


class Dispatcher:
    """Execute cluster/collector work for a single time window."""

    def __init__(
        self,
        registry: CollectorRegistry,
        max_concurrency: int = 10,
        retry_limit: int = 0,
        task_timeout_seconds: float = 30.0,
    ) -> None:
        if max_concurrency <= 0:
            raise ValueError("max_concurrency must be positive")
        if retry_limit < 0:
            raise ValueError("retry_limit cannot be negative")
        if task_timeout_seconds <= 0:
            raise ValueError("task_timeout_seconds must be positive")

        self._registry = registry
        self._retry_limit = retry_limit
        self._task_timeout_seconds = task_timeout_seconds
        self._semaphore = asyncio.Semaphore(max_concurrency)

    async def run_window(
        self,
        window: TimeWindow,
        clusters: list[ClusterConfig],
    ) -> DispatchSummary:
        collectors = self._registry.enabled_collectors()
        tasks = [
            asyncio.create_task(self._dispatch_one(window, cluster, collector))
            for cluster in clusters
            if cluster.enabled
            for collector in collectors
        ]
        if not tasks:
            return DispatchSummary(window=window)

        results = await asyncio.gather(*tasks)
        return DispatchSummary(window=window, results=tuple(results))

    async def _dispatch_one(
        self,
        window: TimeWindow,
        cluster: ClusterConfig,
        collector: Collector,
    ) -> DispatchTaskResult:
        started_at = _utc_now()
        attempt_count = 0
        last_result: CollectorResult | None = None
        last_error: CollectorError | None = None

        while attempt_count <= self._retry_limit:
            attempt_count += 1
            collector_result, runtime_error = await self._run_attempt(
                collector=collector,
                cluster_name=cluster.cluster_name,
                window=window,
            )
            if runtime_error is not None:
                last_error = runtime_error
                if attempt_count <= self._retry_limit:
                    continue
                return DispatchTaskResult(
                    cluster_name=cluster.cluster_name,
                    collector_name=collector.name,
                    bucket_time=window.bucket_time,
                    status="failed",
                    attempt_count=attempt_count,
                    started_at=started_at,
                    finished_at=_utc_now(),
                    error=last_error,
                )

            last_result = collector_result
            if last_result.status == "failed" and attempt_count <= self._retry_limit:
                last_error = last_result.error
                continue

            return DispatchTaskResult(
                cluster_name=cluster.cluster_name,
                collector_name=collector.name,
                bucket_time=window.bucket_time,
                status=last_result.status,
                attempt_count=attempt_count,
                started_at=started_at,
                finished_at=_utc_now(),
                points=tuple(last_result.points),
                error=last_result.error,
            )

        return DispatchTaskResult(
            cluster_name=cluster.cluster_name,
            collector_name=collector.name,
            bucket_time=window.bucket_time,
            status="failed",
            attempt_count=attempt_count,
            started_at=started_at,
            finished_at=_utc_now(),
            error=last_error,
        )

    async def _run_attempt(
        self,
        collector: Collector,
        cluster_name: str,
        window: TimeWindow,
    ) -> tuple[CollectorResult | None, CollectorError | None]:
        async with self._semaphore:
            try:
                result = await asyncio.wait_for(
                    asyncio.to_thread(collector.collect, cluster_name, window),
                    timeout=self._task_timeout_seconds,
                )
            except asyncio.TimeoutError:
                return None, CollectorError(
                    message="collector execution timed out",
                    code="timeout",
                )
            except Exception as exc:  # pragma: no cover - defensive path
                return None, CollectorError(
                    message=f"collector execution raised an exception: {exc}",
                    code="collector_exception",
                )

        if not isinstance(result, CollectorResult):
            return None, CollectorError(
                message="collector returned an invalid result object",
                code="malformed_result",
            )
        return result, None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
