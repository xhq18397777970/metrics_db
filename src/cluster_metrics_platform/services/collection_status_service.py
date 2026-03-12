"""Track live collection progress and expose dashboard snapshots."""

from __future__ import annotations

from datetime import datetime, timezone

UPSERT_WINDOW_STATUS_SQL = """
INSERT INTO collection_window_status (
    bucket_time,
    window_start,
    window_end,
    status,
    selected_cluster_count,
    total_tasks,
    completed_tasks,
    remaining_tasks,
    success_count,
    partial_success_count,
    failed_count,
    points_written,
    runs_written,
    started_at,
    updated_at,
    finished_at,
    last_error
)
VALUES (
    %(bucket_time)s,
    %(window_start)s,
    %(window_end)s,
    %(status)s,
    %(selected_cluster_count)s,
    %(total_tasks)s,
    %(completed_tasks)s,
    %(remaining_tasks)s,
    %(success_count)s,
    %(partial_success_count)s,
    %(failed_count)s,
    %(points_written)s,
    %(runs_written)s,
    %(started_at)s,
    %(updated_at)s,
    %(finished_at)s,
    %(last_error)s
)
ON CONFLICT (bucket_time)
DO UPDATE SET
    window_start = EXCLUDED.window_start,
    window_end = EXCLUDED.window_end,
    status = EXCLUDED.status,
    selected_cluster_count = EXCLUDED.selected_cluster_count,
    total_tasks = EXCLUDED.total_tasks,
    completed_tasks = EXCLUDED.completed_tasks,
    remaining_tasks = EXCLUDED.remaining_tasks,
    success_count = EXCLUDED.success_count,
    partial_success_count = EXCLUDED.partial_success_count,
    failed_count = EXCLUDED.failed_count,
    points_written = EXCLUDED.points_written,
    runs_written = EXCLUDED.runs_written,
    started_at = EXCLUDED.started_at,
    updated_at = EXCLUDED.updated_at,
    finished_at = EXCLUDED.finished_at,
    last_error = EXCLUDED.last_error
"""

UPSERT_SCHEDULER_RUNTIME_SQL = """
INSERT INTO scheduler_runtime (
    scheduler_id,
    status,
    step_minutes,
    active_bucket_time,
    active_window_start,
    active_window_end,
    selected_cluster_count,
    total_tasks,
    completed_tasks,
    success_count,
    partial_success_count,
    failed_count,
    last_points_written,
    last_runs_written,
    last_started_at,
    last_finished_at,
    last_heartbeat_at,
    updated_at,
    last_error
)
VALUES (
    %(scheduler_id)s,
    %(status)s,
    %(step_minutes)s,
    %(active_bucket_time)s,
    %(active_window_start)s,
    %(active_window_end)s,
    %(selected_cluster_count)s,
    %(total_tasks)s,
    %(completed_tasks)s,
    %(success_count)s,
    %(partial_success_count)s,
    %(failed_count)s,
    %(last_points_written)s,
    %(last_runs_written)s,
    %(last_started_at)s,
    %(last_finished_at)s,
    %(last_heartbeat_at)s,
    %(updated_at)s,
    %(last_error)s
)
ON CONFLICT (scheduler_id)
DO UPDATE SET
    status = EXCLUDED.status,
    step_minutes = EXCLUDED.step_minutes,
    active_bucket_time = EXCLUDED.active_bucket_time,
    active_window_start = EXCLUDED.active_window_start,
    active_window_end = EXCLUDED.active_window_end,
    selected_cluster_count = EXCLUDED.selected_cluster_count,
    total_tasks = EXCLUDED.total_tasks,
    completed_tasks = EXCLUDED.completed_tasks,
    success_count = EXCLUDED.success_count,
    partial_success_count = EXCLUDED.partial_success_count,
    failed_count = EXCLUDED.failed_count,
    last_points_written = EXCLUDED.last_points_written,
    last_runs_written = EXCLUDED.last_runs_written,
    last_started_at = EXCLUDED.last_started_at,
    last_finished_at = EXCLUDED.last_finished_at,
    last_heartbeat_at = EXCLUDED.last_heartbeat_at,
    updated_at = EXCLUDED.updated_at,
    last_error = EXCLUDED.last_error
"""

GET_SCHEDULER_RUNTIME_SQL = """
SELECT
    scheduler_id,
    status,
    step_minutes,
    active_bucket_time,
    active_window_start,
    active_window_end,
    selected_cluster_count,
    total_tasks,
    completed_tasks,
    success_count,
    partial_success_count,
    failed_count,
    last_points_written,
    last_runs_written,
    last_started_at,
    last_finished_at,
    last_heartbeat_at,
    updated_at,
    last_error
FROM scheduler_runtime
WHERE scheduler_id = %(scheduler_id)s
"""

LIST_WINDOW_STATUS_SQL = """
SELECT
    bucket_time,
    window_start,
    window_end,
    status,
    selected_cluster_count,
    total_tasks,
    completed_tasks,
    remaining_tasks,
    success_count,
    partial_success_count,
    failed_count,
    points_written,
    runs_written,
    started_at,
    updated_at,
    finished_at,
    last_error
FROM collection_window_status
ORDER BY bucket_time DESC
LIMIT %(limit)s
"""


class CollectionStatusService:
    """Persist and retrieve scheduler/window execution progress."""

    def __init__(
        self,
        connection,
        *,
        scheduler_id: str = "default",
        stale_after_seconds: int = 600,
    ) -> None:
        self._connection = connection
        self._scheduler_id = scheduler_id
        self._stale_after_seconds = stale_after_seconds

    def mark_scheduler_idle(
        self,
        *,
        step_minutes: int,
        last_finished_at: datetime | None = None,
    ) -> None:
        now = _utc_now()
        existing = self._fetch_scheduler_runtime()
        self._upsert_scheduler_runtime(
            {
                "scheduler_id": self._scheduler_id,
                "status": "idle",
                "step_minutes": step_minutes,
                "active_bucket_time": None,
                "active_window_start": None,
                "active_window_end": None,
                "selected_cluster_count": 0,
                "total_tasks": 0,
                "completed_tasks": 0,
                "success_count": 0,
                "partial_success_count": 0,
                "failed_count": 0,
                "last_points_written": existing["last_points_written"] if existing else 0,
                "last_runs_written": existing["last_runs_written"] if existing else 0,
                "last_started_at": existing["last_started_at"] if existing else None,
                "last_finished_at": (
                    last_finished_at
                    or (existing["last_finished_at"] if existing else None)
                ),
                "last_heartbeat_at": now,
                "updated_at": now,
                "last_error": existing["last_error"] if existing else None,
            }
        )

    def mark_scheduler_stopped(self, *, step_minutes: int, last_error: str | None = None) -> None:
        now = _utc_now()
        existing = self._fetch_scheduler_runtime()
        self._upsert_scheduler_runtime(
            {
                "scheduler_id": self._scheduler_id,
                "status": "stopped",
                "step_minutes": step_minutes,
                "active_bucket_time": None,
                "active_window_start": None,
                "active_window_end": None,
                "selected_cluster_count": 0,
                "total_tasks": 0,
                "completed_tasks": 0,
                "success_count": 0,
                "partial_success_count": 0,
                "failed_count": 0,
                "last_points_written": existing["last_points_written"] if existing else 0,
                "last_runs_written": existing["last_runs_written"] if existing else 0,
                "last_started_at": existing["last_started_at"] if existing else None,
                "last_finished_at": existing["last_finished_at"] if existing else now,
                "last_heartbeat_at": now,
                "updated_at": now,
                "last_error": last_error or (existing["last_error"] if existing else None),
            }
        )

    def begin_window(
        self,
        *,
        window,
        selected_cluster_count: int,
        total_tasks: int,
        started_at: datetime,
        step_minutes: int,
    ) -> None:
        now = _utc_now()
        payload = {
            "bucket_time": window.bucket_time,
            "window_start": window.start_time,
            "window_end": window.end_time,
            "status": "running",
            "selected_cluster_count": selected_cluster_count,
            "total_tasks": total_tasks,
            "completed_tasks": 0,
            "remaining_tasks": total_tasks,
            "success_count": 0,
            "partial_success_count": 0,
            "failed_count": 0,
            "points_written": 0,
            "runs_written": 0,
            "started_at": started_at,
            "updated_at": now,
            "finished_at": None,
            "last_error": None,
        }
        self._upsert_window_status(payload)
        self._upsert_scheduler_runtime(
            {
                "scheduler_id": self._scheduler_id,
                "status": "running",
                "step_minutes": step_minutes,
                "active_bucket_time": window.bucket_time,
                "active_window_start": window.start_time,
                "active_window_end": window.end_time,
                "selected_cluster_count": selected_cluster_count,
                "total_tasks": total_tasks,
                "completed_tasks": 0,
                "success_count": 0,
                "partial_success_count": 0,
                "failed_count": 0,
                "last_points_written": 0,
                "last_runs_written": 0,
                "last_started_at": started_at,
                "last_finished_at": None,
                "last_heartbeat_at": now,
                "updated_at": now,
                "last_error": None,
            }
        )

    def advance_window(
        self,
        *,
        window,
        selected_cluster_count: int,
        total_tasks: int,
        completed_tasks: int,
        success_count: int,
        partial_success_count: int,
        failed_count: int,
        started_at: datetime,
        step_minutes: int,
        last_error: str | None = None,
    ) -> None:
        now = _utc_now()
        remaining_tasks = max(total_tasks - completed_tasks, 0)
        payload = {
            "bucket_time": window.bucket_time,
            "window_start": window.start_time,
            "window_end": window.end_time,
            "status": "running",
            "selected_cluster_count": selected_cluster_count,
            "total_tasks": total_tasks,
            "completed_tasks": completed_tasks,
            "remaining_tasks": remaining_tasks,
            "success_count": success_count,
            "partial_success_count": partial_success_count,
            "failed_count": failed_count,
            "points_written": 0,
            "runs_written": 0,
            "started_at": started_at,
            "updated_at": now,
            "finished_at": None,
            "last_error": last_error,
        }
        self._upsert_window_status(payload)
        self._upsert_scheduler_runtime(
            {
                "scheduler_id": self._scheduler_id,
                "status": "running",
                "step_minutes": step_minutes,
                "active_bucket_time": window.bucket_time,
                "active_window_start": window.start_time,
                "active_window_end": window.end_time,
                "selected_cluster_count": selected_cluster_count,
                "total_tasks": total_tasks,
                "completed_tasks": completed_tasks,
                "success_count": success_count,
                "partial_success_count": partial_success_count,
                "failed_count": failed_count,
                "last_points_written": 0,
                "last_runs_written": 0,
                "last_started_at": started_at,
                "last_finished_at": None,
                "last_heartbeat_at": now,
                "updated_at": now,
                "last_error": last_error,
            }
        )

    def complete_window(
        self,
        *,
        execution,
        selected_cluster_count: int,
        started_at: datetime,
        step_minutes: int,
    ) -> None:
        now = _utc_now()
        summary = execution.summary
        status = "completed"
        if summary.failed_count or summary.partial_success_count:
            status = "completed_with_errors"

        payload = {
            "bucket_time": execution.window.bucket_time,
            "window_start": execution.window.start_time,
            "window_end": execution.window.end_time,
            "status": status,
            "selected_cluster_count": selected_cluster_count,
            "total_tasks": summary.total_tasks,
            "completed_tasks": summary.total_tasks,
            "remaining_tasks": 0,
            "success_count": summary.success_count,
            "partial_success_count": summary.partial_success_count,
            "failed_count": summary.failed_count,
            "points_written": execution.points_written,
            "runs_written": execution.runs_written,
            "started_at": started_at,
            "updated_at": now,
            "finished_at": now,
            "last_error": _summary_error(summary),
        }
        self._upsert_window_status(payload)
        self._upsert_scheduler_runtime(
            {
                "scheduler_id": self._scheduler_id,
                "status": "idle",
                "step_minutes": step_minutes,
                "active_bucket_time": None,
                "active_window_start": None,
                "active_window_end": None,
                "selected_cluster_count": 0,
                "total_tasks": 0,
                "completed_tasks": 0,
                "success_count": 0,
                "partial_success_count": 0,
                "failed_count": 0,
                "last_points_written": execution.points_written,
                "last_runs_written": execution.runs_written,
                "last_started_at": started_at,
                "last_finished_at": now,
                "last_heartbeat_at": now,
                "updated_at": now,
                "last_error": _summary_error(summary),
            }
        )

    def fail_window(
        self,
        *,
        window,
        selected_cluster_count: int,
        total_tasks: int,
        started_at: datetime,
        step_minutes: int,
        error_message: str,
    ) -> None:
        now = _utc_now()
        payload = {
            "bucket_time": window.bucket_time,
            "window_start": window.start_time,
            "window_end": window.end_time,
            "status": "failed",
            "selected_cluster_count": selected_cluster_count,
            "total_tasks": total_tasks,
            "completed_tasks": 0,
            "remaining_tasks": total_tasks,
            "success_count": 0,
            "partial_success_count": 0,
            "failed_count": total_tasks,
            "points_written": 0,
            "runs_written": 0,
            "started_at": started_at,
            "updated_at": now,
            "finished_at": now,
            "last_error": error_message,
        }
        self._upsert_window_status(payload)
        self._upsert_scheduler_runtime(
            {
                "scheduler_id": self._scheduler_id,
                "status": "idle",
                "step_minutes": step_minutes,
                "active_bucket_time": None,
                "active_window_start": None,
                "active_window_end": None,
                "selected_cluster_count": 0,
                "total_tasks": 0,
                "completed_tasks": 0,
                "success_count": 0,
                "partial_success_count": 0,
                "failed_count": 0,
                "last_points_written": 0,
                "last_runs_written": 0,
                "last_started_at": started_at,
                "last_finished_at": now,
                "last_heartbeat_at": now,
                "updated_at": now,
                "last_error": error_message,
            }
        )

    def get_dashboard_snapshot(self, *, limit: int = 12) -> dict[str, object]:
        if limit <= 0 or limit > 50:
            raise ValueError("limit must be between 1 and 50")

        scheduler_row = self._fetch_scheduler_runtime()
        window_rows = self._fetch_window_rows(limit=limit)
        return {
            "scheduler": self._serialize_scheduler_row(scheduler_row),
            "windows": tuple(self._serialize_window_row(row) for row in window_rows),
        }

    def _fetch_scheduler_runtime(self):
        with self._connection.cursor() as cursor:
            cursor.execute(
                GET_SCHEDULER_RUNTIME_SQL,
                {"scheduler_id": self._scheduler_id},
            )
            return cursor.fetchone()

    def _fetch_window_rows(self, *, limit: int):
        with self._connection.cursor() as cursor:
            cursor.execute(LIST_WINDOW_STATUS_SQL, {"limit": limit})
            return list(cursor.fetchall())

    def _serialize_scheduler_row(self, row) -> dict[str, object]:
        if row is None:
            return {
                "status": "stopped",
                "is_stale": True,
                "step_minutes": 5,
                "active_bucket_time": None,
                "active_window_start": None,
                "active_window_end": None,
                "selected_cluster_count": 0,
                "total_tasks": 0,
                "completed_tasks": 0,
                "remaining_tasks": 0,
                "success_count": 0,
                "partial_success_count": 0,
                "failed_count": 0,
                "last_points_written": 0,
                "last_runs_written": 0,
                "last_started_at": None,
                "last_finished_at": None,
                "last_heartbeat_at": None,
                "updated_at": None,
                "last_error": None,
            }

        now = _utc_now()
        last_heartbeat_at = row["last_heartbeat_at"]
        is_stale = (
            last_heartbeat_at is None
            or (now - last_heartbeat_at).total_seconds() > self._stale_after_seconds
        )
        status = "stopped" if is_stale else row["status"]

        return {
            "status": status,
            "is_stale": is_stale,
            "step_minutes": row["step_minutes"],
            "active_bucket_time": row["active_bucket_time"],
            "active_window_start": row["active_window_start"],
            "active_window_end": row["active_window_end"],
            "selected_cluster_count": row["selected_cluster_count"],
            "total_tasks": row["total_tasks"],
            "completed_tasks": row["completed_tasks"],
            "remaining_tasks": max(row["total_tasks"] - row["completed_tasks"], 0),
            "success_count": row["success_count"],
            "partial_success_count": row["partial_success_count"],
            "failed_count": row["failed_count"],
            "last_points_written": row["last_points_written"],
            "last_runs_written": row["last_runs_written"],
            "last_started_at": row["last_started_at"],
            "last_finished_at": row["last_finished_at"],
            "last_heartbeat_at": row["last_heartbeat_at"],
            "updated_at": row["updated_at"],
            "last_error": row["last_error"],
        }

    @staticmethod
    def _serialize_window_row(row) -> dict[str, object]:
        return {
            "bucket_time": row["bucket_time"],
            "window_start": row["window_start"],
            "window_end": row["window_end"],
            "status": row["status"],
            "selected_cluster_count": row["selected_cluster_count"],
            "total_tasks": row["total_tasks"],
            "completed_tasks": row["completed_tasks"],
            "remaining_tasks": row["remaining_tasks"],
            "success_count": row["success_count"],
            "partial_success_count": row["partial_success_count"],
            "failed_count": row["failed_count"],
            "points_written": row["points_written"],
            "runs_written": row["runs_written"],
            "started_at": row["started_at"],
            "updated_at": row["updated_at"],
            "finished_at": row["finished_at"],
            "last_error": row["last_error"],
            "completion_rate": _completion_rate(
                row["completed_tasks"],
                row["total_tasks"],
            ),
        }

    def _upsert_window_status(self, payload: dict[str, object]) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(UPSERT_WINDOW_STATUS_SQL, payload)
        self._commit_if_needed()

    def _upsert_scheduler_runtime(self, payload: dict[str, object]) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(UPSERT_SCHEDULER_RUNTIME_SQL, payload)
        self._commit_if_needed()

    def _commit_if_needed(self) -> None:
        if not self._connection.autocommit:
            self._connection.commit()


def _completion_rate(completed_tasks: int, total_tasks: int) -> float:
    if total_tasks <= 0:
        return 0.0
    return round((completed_tasks / total_tasks) * 100, 2)


def _summary_error(summary) -> str | None:
    errors = [
        result.error.message
        for result in summary.results
        if result.error is not None and result.error.message
    ]
    return errors[0] if errors else None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
