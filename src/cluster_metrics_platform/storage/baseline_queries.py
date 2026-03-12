"""SQL helpers for rollup management and baseline retrieval."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from psycopg import Connection

from cluster_metrics_platform.domain.models import BaselineQuery
from cluster_metrics_platform.storage.db import apply_sql_file

ROLLUP_SQL_PATH = Path(__file__).resolve().parents[3] / "sql" / "002_rollups.sql"

HISTORICAL_POINTS_SQL = """
WITH requested_slots AS (
    SELECT
        series AS bucket_time,
        ((series AT TIME ZONE %(query_timezone)s))::time AS slot_clock
    FROM generate_series(
        %(start_time)s::timestamptz,
        %(end_time)s::timestamptz - INTERVAL '5 minutes',
        INTERVAL '5 minutes'
    ) AS series
),
historical_matches AS (
    SELECT
        slots.bucket_time,
        points.metric_value
    FROM requested_slots AS slots
    LEFT JOIN metric_points AS points
        ON points.cluster_name = %(cluster_name)s
       AND points.metric_name = %(metric_name)s
       AND points.bucket_time >= %(history_start)s::timestamptz
       AND points.bucket_time < %(history_end)s::timestamptz
       AND ((points.bucket_time AT TIME ZONE %(query_timezone)s))::time = slots.slot_clock
)
SELECT
    bucket_time,
    AVG(metric_value) AS avg,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY metric_value) AS p50,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY metric_value) AS p95
FROM historical_matches
GROUP BY bucket_time
HAVING COUNT(metric_value) > 0
ORDER BY bucket_time
"""

HISTORICAL_SUMMARY_SQL = """
WITH requested_slots AS (
    SELECT
        ((series AT TIME ZONE %(query_timezone)s))::time AS slot_clock
    FROM generate_series(
        %(start_time)s::timestamptz,
        %(end_time)s::timestamptz - INTERVAL '5 minutes',
        INTERVAL '5 minutes'
    ) AS series
),
historical_matches AS (
    SELECT points.metric_value
    FROM requested_slots AS slots
    JOIN metric_points AS points
        ON points.cluster_name = %(cluster_name)s
       AND points.metric_name = %(metric_name)s
       AND points.bucket_time >= %(history_start)s::timestamptz
       AND points.bucket_time < %(history_end)s::timestamptz
       AND ((points.bucket_time AT TIME ZONE %(query_timezone)s))::time = slots.slot_clock
)
SELECT
    AVG(metric_value) AS avg,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY metric_value) AS p50,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY metric_value) AS p95,
    COUNT(*) AS sample_count
FROM historical_matches
"""

LAST_WEEK_POINTS_SQL = """
WITH shifted_points AS (
    SELECT
        bucket_time + INTERVAL '7 days' AS bucket_time,
        metric_value
    FROM metric_points
    WHERE cluster_name = %(cluster_name)s
      AND metric_name = %(metric_name)s
      AND bucket_time >= %(shifted_start)s::timestamptz
      AND bucket_time < %(shifted_end)s::timestamptz
)
SELECT
    bucket_time,
    AVG(metric_value) AS avg,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY metric_value) AS p50,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY metric_value) AS p95
FROM shifted_points
GROUP BY bucket_time
ORDER BY bucket_time
"""

LAST_WEEK_SUMMARY_SQL = """
SELECT
    AVG(metric_value) AS avg,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY metric_value) AS p50,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY metric_value) AS p95,
    COUNT(*) AS sample_count
FROM metric_points
WHERE cluster_name = %(cluster_name)s
  AND metric_name = %(metric_name)s
  AND bucket_time >= %(shifted_start)s::timestamptz
  AND bucket_time < %(shifted_end)s::timestamptz
"""


def initialize_rollups(
    connection: Connection,
    sql_path: str | Path = ROLLUP_SQL_PATH,
) -> None:
    """Create continuous aggregates and attach Timescale policies."""

    apply_sql_file(connection, sql_path)
    _commit_if_needed(connection)


def refresh_rollups(
    connection: Connection,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> None:
    """Refresh hourly rollups before daily rollups for the requested range."""

    with connection.cursor() as cursor:
        cursor.execute(
            "CALL refresh_continuous_aggregate('metric_rollup_1h', %s, %s)",
            (start_time, end_time),
        )
        cursor.execute(
            "CALL refresh_continuous_aggregate('metric_rollup_1d', %s, %s)",
            (start_time, end_time),
        )

    _commit_if_needed(connection)


def fetch_baseline_points(connection: Connection, query: BaselineQuery) -> list[dict[str, object]]:
    """Return per-bucket baseline values for the requested mode."""

    sql_text, params = _build_points_query(query)
    with connection.cursor() as cursor:
        cursor.execute(sql_text, params)
        return list(cursor.fetchall())


def fetch_baseline_summary(connection: Connection, query: BaselineQuery) -> dict[str, object]:
    """Return the aggregate baseline summary for the requested mode."""

    sql_text, params = _build_summary_query(query)
    with connection.cursor() as cursor:
        cursor.execute(sql_text, params)
        row = cursor.fetchone()

    return dict(row or {})


def _build_points_query(query: BaselineQuery) -> tuple[str, dict[str, object]]:
    if query.mode == "historical_range":
        return HISTORICAL_POINTS_SQL, _historical_params(query)
    if query.mode == "last_week_same_range":
        return LAST_WEEK_POINTS_SQL, _last_week_params(query)
    raise ValueError(f"unsupported baseline mode: {query.mode}")


def _build_summary_query(query: BaselineQuery) -> tuple[str, dict[str, object]]:
    if query.mode == "historical_range":
        return HISTORICAL_SUMMARY_SQL, _historical_params(query)
    if query.mode == "last_week_same_range":
        return LAST_WEEK_SUMMARY_SQL, _last_week_params(query)
    raise ValueError(f"unsupported baseline mode: {query.mode}")


def _historical_params(query: BaselineQuery) -> dict[str, object]:
    return {
        "cluster_name": query.cluster_name,
        "metric_name": query.metric_name,
        "start_time": query.start_time,
        "end_time": query.end_time,
        "history_start": query.start_time - timedelta(days=query.lookback_days),
        "history_end": query.start_time,
        "query_timezone": _fixed_offset_timezone(query.start_time),
    }


def _last_week_params(query: BaselineQuery) -> dict[str, object]:
    return {
        "cluster_name": query.cluster_name,
        "metric_name": query.metric_name,
        "shifted_start": query.start_time - timedelta(days=7),
        "shifted_end": query.end_time - timedelta(days=7),
    }


def _fixed_offset_timezone(value: datetime) -> str:
    offset = value.utcoffset()
    if offset is None:
        return "UTC"
    total_minutes = int(offset.total_seconds() // 60)
    sign = "+" if total_minutes >= 0 else "-"
    absolute_minutes = abs(total_minutes)
    hours, minutes = divmod(absolute_minutes, 60)
    return f"{sign}{hours:02}:{minutes:02}"


def _commit_if_needed(connection: Connection) -> None:
    if not connection.autocommit:
        connection.commit()
