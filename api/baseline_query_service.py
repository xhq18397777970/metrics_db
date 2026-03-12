"""Standalone baseline average query service backed by TimescaleDB."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

BASELINE_AVERAGE_SQL = """
SELECT
    AVG(metric_value) AS baseline_value,
    COUNT(*) AS sample_count
FROM metric_points
WHERE cluster_name = %(cluster_name)s
  AND metric_name = %(metric_name)s
  AND bucket_time >= %(start_time)s::timestamptz
  AND bucket_time < %(end_time)s::timestamptz
"""


@dataclass(frozen=True, slots=True)
class BaselineAverageQuery:
    """Request model for a single baseline average lookup."""

    cluster_name: str
    metric_name: str
    start_time: datetime
    end_time: datetime


@dataclass(frozen=True, slots=True)
class BaselineAverageResponse:
    """Response model for a baseline average lookup."""

    cluster_name: str
    metric_name: str
    start_time: datetime
    end_time: datetime
    baseline_value: float | None
    sample_count: int
    status: str = "success"


class BaselineQueryService:
    """Read baseline averages from the metric_points hypertable."""

    def __init__(self, connection) -> None:
        self._connection = connection

    def query_average(self, request: BaselineAverageQuery) -> BaselineAverageResponse:
        """Return the average metric value for one cluster and time range."""

        self._validate_request(request)
        with self._connection.cursor() as cursor:
            cursor.execute(
                BASELINE_AVERAGE_SQL,
                {
                    "cluster_name": request.cluster_name,
                    "metric_name": request.metric_name,
                    "start_time": request.start_time,
                    "end_time": request.end_time,
                },
            )
            row = cursor.fetchone() or {}

        sample_count = int(row.get("sample_count") or 0)
        baseline_value = row.get("baseline_value")
        if sample_count == 0 or baseline_value is None:
            return BaselineAverageResponse(
                cluster_name=request.cluster_name,
                metric_name=request.metric_name,
                start_time=request.start_time,
                end_time=request.end_time,
                baseline_value=None,
                sample_count=0,
                status="no_data",
            )

        return BaselineAverageResponse(
            cluster_name=request.cluster_name,
            metric_name=request.metric_name,
            start_time=request.start_time,
            end_time=request.end_time,
            baseline_value=float(baseline_value),
            sample_count=sample_count,
            status="success",
        )

    @staticmethod
    def _validate_request(request: BaselineAverageQuery) -> None:
        if not request.cluster_name:
            raise ValueError("cluster_name must not be empty")
        if not request.metric_name:
            raise ValueError("metric_name must not be empty")
        if request.end_time <= request.start_time:
            raise ValueError("end_time must be greater than start_time")

