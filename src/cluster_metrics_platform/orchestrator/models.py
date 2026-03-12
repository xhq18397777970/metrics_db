"""Orchestration result models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from cluster_metrics_platform.domain.models import CollectorError, MetricPoint, TimeWindow


@dataclass(frozen=True, slots=True)
class DispatchTaskResult:
    """Execution outcome for one cluster/collector/window tuple."""

    cluster_name: str
    collector_name: str
    bucket_time: datetime
    status: str
    attempt_count: int
    started_at: datetime
    finished_at: datetime
    points: tuple[MetricPoint, ...] = ()
    error: CollectorError | None = None


@dataclass(frozen=True, slots=True)
class DispatchSummary:
    """Aggregate execution result for one time window."""

    window: TimeWindow
    results: tuple[DispatchTaskResult, ...] = field(default_factory=tuple)

    @property
    def total_tasks(self) -> int:
        return len(self.results)

    @property
    def success_count(self) -> int:
        return sum(result.status == "success" for result in self.results)

    @property
    def partial_success_count(self) -> int:
        return sum(result.status == "partial_success" for result in self.results)

    @property
    def failed_count(self) -> int:
        return sum(result.status == "failed" for result in self.results)

    @property
    def total_points(self) -> int:
        return sum(len(result.points) for result in self.results)

    def all_points(self) -> list[MetricPoint]:
        return [point for result in self.results for point in result.points]


@dataclass(frozen=True, slots=True)
class DispatchProgress:
    """Live progress counters for one window dispatch cycle."""

    window: TimeWindow
    total_tasks: int
    completed_tasks: int
    success_count: int
    partial_success_count: int
    failed_count: int
    latest_result: DispatchTaskResult
