"""Collector interface and shared helpers."""

from __future__ import annotations

from abc import ABC, abstractmethod

from cluster_metrics_platform.domain.models import (
    CollectorError,
    CollectorResult,
    MetricPoint,
    TimeWindow,
)


class Collector(ABC):
    """Abstract collector that normalizes one tool into metric points."""

    name: str

    @abstractmethod
    def collect(self, cluster: str, window: TimeWindow) -> CollectorResult:
        """Collect metrics for one cluster and one time window."""

    def _failure(self, message: str, code: str = "collector_error") -> CollectorResult:
        return CollectorResult(
            status="failed",
            error=CollectorError(message=message, code=code),
        )

    def _partial_success(
        self,
        points: list[MetricPoint],
        message: str,
        code: str = "partial_success",
    ) -> CollectorResult:
        return CollectorResult(
            status="partial_success",
            points=points,
            error=CollectorError(message=message, code=code),
        )

    def _success(self, points: list[MetricPoint]) -> CollectorResult:
        return CollectorResult(status="success", points=points)
