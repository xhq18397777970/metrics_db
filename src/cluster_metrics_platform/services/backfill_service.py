"""Application service for sequential historical backfill."""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime

from cluster_metrics_platform.domain.models import TimeWindow
from cluster_metrics_platform.domain.time_window import iter_windows
from cluster_metrics_platform.services.collection_service import CollectionExecution


@dataclass(frozen=True, slots=True)
class BackfillExecution:
    """Summary of a backfill run across multiple canonical windows."""

    windows: tuple[TimeWindow, ...] = ()
    executions: tuple[CollectionExecution, ...] = ()

    @property
    def total_windows(self) -> int:
        return len(self.windows)

    @property
    def total_points_written(self) -> int:
        return sum(execution.points_written for execution in self.executions)

    @property
    def total_runs_written(self) -> int:
        return sum(execution.runs_written for execution in self.executions)


class BackfillService:
    """Advance over historical buckets one window at a time."""

    def __init__(self, collection_service, step_minutes: int = 5) -> None:
        self._collection_service = collection_service
        self._step_minutes = step_minutes

    async def backfill(
        self,
        start_time: datetime,
        end_time: datetime,
        cluster_names: Collection[str] | None = None,
    ) -> BackfillExecution:
        executions: list[CollectionExecution] = []
        windows = tuple(
            iter_windows(start_time, end_time, step_minutes=self._step_minutes)
        )

        for window in windows:
            executions.append(
                await self._collection_service.collect_window(window, cluster_names)
            )

        return BackfillExecution(
            windows=windows,
            executions=tuple(executions),
        )
