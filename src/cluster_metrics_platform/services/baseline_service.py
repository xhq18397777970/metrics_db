"""Database-backed baseline query service."""

from __future__ import annotations

from cluster_metrics_platform.domain.models import BaselineQuery, BaselineResponse
from cluster_metrics_platform.storage.baseline_queries import (
    fetch_baseline_points,
    fetch_baseline_summary,
    initialize_rollups,
    refresh_rollups,
)

SUPPORTED_AGGREGATIONS = {"avg", "p50", "p95"}
SUPPORTED_MODES = {"historical_range", "last_week_same_range"}


class BaselineService:
    """Resolve baseline requests using database-side aggregation."""

    def __init__(self, connection) -> None:
        self._connection = connection

    def initialize_rollups(self) -> None:
        """Create rollups and policies if they do not already exist."""

        initialize_rollups(self._connection)

    def refresh_rollups(self, start_time=None, end_time=None) -> None:
        """Refresh the configured rollups for a target time span."""

        refresh_rollups(self._connection, start_time=start_time, end_time=end_time)

    def query_baseline(self, request: BaselineQuery) -> BaselineResponse:
        """Return baseline summary and per-bucket series for a supported mode."""

        self._validate_request(request)
        summary_row = fetch_baseline_summary(self._connection, request)
        if not summary_row or not summary_row.get("sample_count"):
            return BaselineResponse(
                cluster_name=request.cluster_name,
                metric_name=request.metric_name,
                mode=request.mode,
                start_time=request.start_time,
                end_time=request.end_time,
                baseline_summary={},
                points=(),
                status="no_data",
            )

        points = tuple(
            _filter_aggregations(row, request.aggregations, include_bucket_time=True)
            for row in fetch_baseline_points(self._connection, request)
        )
        summary = _filter_aggregations(summary_row, request.aggregations)

        return BaselineResponse(
            cluster_name=request.cluster_name,
            metric_name=request.metric_name,
            mode=request.mode,
            start_time=request.start_time,
            end_time=request.end_time,
            baseline_summary=summary,
            points=points,
            status="success",
        )

    @staticmethod
    def _validate_request(request: BaselineQuery) -> None:
        if request.mode not in SUPPORTED_MODES:
            raise ValueError(f"unsupported baseline mode: {request.mode}")
        if request.end_time <= request.start_time:
            raise ValueError("end_time must be greater than start_time")
        if request.lookback_days <= 0:
            raise ValueError("lookback_days must be positive")
        unsupported = set(request.aggregations) - SUPPORTED_AGGREGATIONS
        if unsupported:
            raise ValueError(f"unsupported aggregations: {sorted(unsupported)}")


def _filter_aggregations(
    row: dict[str, object],
    aggregations: tuple[str, ...],
    *,
    include_bucket_time: bool = False,
) -> dict[str, float | object]:
    filtered: dict[str, float | object] = {}
    if include_bucket_time:
        filtered["bucket_time"] = row["bucket_time"]

    for aggregation in aggregations:
        value = row.get(aggregation)
        if value is not None:
            filtered[aggregation] = float(value)

    return filtered
