"""Application service for one-window metric collection."""

from __future__ import annotations

from collections.abc import Callable, Collection
from dataclasses import dataclass
from uuid import uuid4

from cluster_metrics_platform.domain.models import ClusterConfig, CollectionRun, TimeWindow
from cluster_metrics_platform.orchestrator.models import DispatchSummary

ClusterLoader = Callable[[], list[ClusterConfig]]


@dataclass(frozen=True, slots=True)
class CollectionExecution:
    """Result of collecting and persisting one canonical time window."""

    window: TimeWindow
    summary: DispatchSummary
    loaded_cluster_count: int
    selected_cluster_count: int
    points_written: int
    runs_written: int


class CollectionService:
    """Load clusters, dispatch collectors, and persist normalized results."""

    def __init__(self, cluster_loader: ClusterLoader, dispatcher, repository) -> None:
        self._cluster_loader = cluster_loader
        self._dispatcher = dispatcher
        self._repository = repository

    async def collect_window(
        self,
        window: TimeWindow,
        cluster_names: Collection[str] | None = None,
    ) -> CollectionExecution:
        loaded_clusters = self._cluster_loader()
        selected_clusters = _select_clusters(loaded_clusters, cluster_names)

        summary = await self._dispatcher.run_window(window, selected_clusters)
        points_written = self._repository.upsert_points(summary.all_points())
        run_records = _build_run_records(summary)
        runs_written = self._repository.save_run_records(run_records)

        return CollectionExecution(
            window=window,
            summary=summary,
            loaded_cluster_count=len(loaded_clusters),
            selected_cluster_count=len(selected_clusters),
            points_written=points_written,
            runs_written=runs_written,
        )


def _select_clusters(
    clusters: list[ClusterConfig],
    cluster_names: Collection[str] | None,
) -> list[ClusterConfig]:
    if not cluster_names:
        return [cluster for cluster in clusters if cluster.enabled]

    selected_names = set(cluster_names)
    return [
        cluster
        for cluster in clusters
        if cluster.enabled and cluster.cluster_name in selected_names
    ]


def _build_run_records(summary: DispatchSummary) -> list[CollectionRun]:
    return [
        CollectionRun(
            run_id=uuid4(),
            cluster_name=result.cluster_name,
            collector_name=result.collector_name,
            bucket_time=result.bucket_time,
            status=result.status,
            retry_count=max(result.attempt_count - 1, 0),
            started_at=result.started_at,
            finished_at=result.finished_at,
            error_code=result.error.code if result.error else None,
            error_message=result.error.message if result.error else None,
        )
        for result in summary.results
    ]
