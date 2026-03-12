"""Repository helpers for TimescaleDB-backed metric storage."""

from __future__ import annotations

from psycopg import Connection
from psycopg.types.json import Jsonb

from cluster_metrics_platform.domain.models import CollectionRun, MetricPoint

UPSERT_METRIC_POINTS_SQL = """
INSERT INTO metric_points (
    bucket_time,
    window_start,
    window_end,
    cluster_name,
    metric_name,
    metric_value,
    labels,
    labels_fingerprint,
    source_tool,
    collected_at
)
VALUES (
    %(bucket_time)s,
    %(window_start)s,
    %(window_end)s,
    %(cluster_name)s,
    %(metric_name)s,
    %(metric_value)s,
    %(labels)s,
    %(labels_fingerprint)s,
    %(source_tool)s,
    %(collected_at)s
)
ON CONFLICT (bucket_time, cluster_name, metric_name, labels_fingerprint)
DO UPDATE SET
    window_start = EXCLUDED.window_start,
    window_end = EXCLUDED.window_end,
    metric_value = EXCLUDED.metric_value,
    labels = EXCLUDED.labels,
    source_tool = EXCLUDED.source_tool,
    collected_at = EXCLUDED.collected_at
"""

UPSERT_COLLECTION_RUNS_SQL = """
INSERT INTO collection_runs (
    run_id,
    bucket_time,
    cluster_name,
    collector_name,
    status,
    retry_count,
    started_at,
    finished_at,
    error_code,
    error_message
)
VALUES (
    %(run_id)s,
    %(bucket_time)s,
    %(cluster_name)s,
    %(collector_name)s,
    %(status)s,
    %(retry_count)s,
    %(started_at)s,
    %(finished_at)s,
    %(error_code)s,
    %(error_message)s
)
ON CONFLICT (run_id)
DO UPDATE SET
    bucket_time = EXCLUDED.bucket_time,
    cluster_name = EXCLUDED.cluster_name,
    collector_name = EXCLUDED.collector_name,
    status = EXCLUDED.status,
    retry_count = EXCLUDED.retry_count,
    started_at = EXCLUDED.started_at,
    finished_at = EXCLUDED.finished_at,
    error_code = EXCLUDED.error_code,
    error_message = EXCLUDED.error_message
"""


class TimescaleMetricsRepository:
    """Persist metric points and collection execution records."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def upsert_points(self, points: list[MetricPoint]) -> int:
        if not points:
            return 0

        with self._connection.cursor() as cursor:
            cursor.executemany(
                UPSERT_METRIC_POINTS_SQL,
                [self._serialize_point(point) for point in points],
            )

        self._commit_if_needed()
        return len(points)

    def save_run_records(self, runs: list[CollectionRun]) -> int:
        if not runs:
            return 0

        with self._connection.cursor() as cursor:
            cursor.executemany(
                UPSERT_COLLECTION_RUNS_SQL,
                [self._serialize_run(run) for run in runs],
            )

        self._commit_if_needed()
        return len(runs)

    def _commit_if_needed(self) -> None:
        if not self._connection.autocommit:
            self._connection.commit()

    @staticmethod
    def _serialize_point(point: MetricPoint) -> dict[str, object]:
        return {
            "bucket_time": point.bucket_time,
            "window_start": point.window_start,
            "window_end": point.window_end,
            "cluster_name": point.cluster_name,
            "metric_name": point.metric_name,
            "metric_value": point.metric_value,
            "labels": Jsonb(point.labels),
            "labels_fingerprint": point.labels_fingerprint,
            "source_tool": point.source_tool,
            "collected_at": point.collected_at or point.window_end,
        }

    @staticmethod
    def _serialize_run(run: CollectionRun) -> dict[str, object]:
        return {
            "run_id": run.run_id,
            "bucket_time": run.bucket_time,
            "cluster_name": run.cluster_name,
            "collector_name": run.collector_name,
            "status": run.status,
            "retry_count": run.retry_count,
            "started_at": run.started_at,
            "finished_at": run.finished_at,
            "error_code": run.error_code,
            "error_message": run.error_message,
        }
