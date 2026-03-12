"""Read-only service for browsing recently collected metric points."""

from __future__ import annotations

from math import ceil

from cluster_metrics_platform.storage.timescale_repo import TimescaleMetricsRepository

LATEST_ROWS_LIMIT = 5000
DEFAULT_PAGE_SIZE = 100


class MetricsTableService:
    """Return recent metric rows for the lightweight web dashboard."""

    def __init__(self, repository: TimescaleMetricsRepository) -> None:
        self._repository = repository

    def list_recent_points(
        self,
        *,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> dict[str, object]:
        if page <= 0:
            raise ValueError("page must be greater than 0")
        if page_size <= 0 or page_size > LATEST_ROWS_LIMIT:
            raise ValueError("page_size must be between 1 and 5000")

        total_rows = self._repository.count_recent_points(visible_limit=LATEST_ROWS_LIMIT)
        total_pages = ceil(total_rows / page_size) if total_rows else 0
        rows = self._repository.list_recent_points(
            page=page,
            page_size=page_size,
            visible_limit=LATEST_ROWS_LIMIT,
        )

        start_row = ((page - 1) * page_size) + 1 if rows else 0
        end_row = start_row + len(rows) - 1 if rows else 0

        return {
            "page": page,
            "page_size": page_size,
            "total_rows": total_rows,
            "total_pages": total_pages,
            "max_rows": LATEST_ROWS_LIMIT,
            "start_row": start_row,
            "end_row": end_row,
            "rows": rows,
        }
