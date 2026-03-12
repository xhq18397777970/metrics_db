"""Application bootstrap wiring."""

from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from pathlib import Path

from cluster_metrics_platform.api.app import create_app
from cluster_metrics_platform.collectors.base import Collector
from cluster_metrics_platform.collectors.cpu_collector import CpuCollector
from cluster_metrics_platform.collectors.http_code_collector import HttpCodeCollector
from cluster_metrics_platform.collectors.qps_collector import QpsCollector
from cluster_metrics_platform.collectors.registry import CollectorRegistry
from cluster_metrics_platform.collectors.tp_collector import TpCollector
from cluster_metrics_platform.config.cluster_loader import load_clusters
from cluster_metrics_platform.orchestrator.dispatcher import Dispatcher
from cluster_metrics_platform.services.backfill_service import BackfillService
from cluster_metrics_platform.services.baseline_service import BaselineService
from cluster_metrics_platform.services.collection_service import CollectionService
from cluster_metrics_platform.settings import AppSettings
from cluster_metrics_platform.storage.baseline_queries import initialize_rollups
from cluster_metrics_platform.storage.db import apply_sql_file, connect_db
from cluster_metrics_platform.storage.timescale_repo import TimescaleMetricsRepository

INIT_SQL_PATH = Path(__file__).resolve().parents[2] / "sql" / "001_init.sql"


@dataclass(slots=True)
class ApplicationContext:
    """Bundle of bootstrapped services and infrastructure objects."""

    settings: AppSettings
    connection: object
    registry: CollectorRegistry
    dispatcher: Dispatcher
    repository: TimescaleMetricsRepository
    collection_service: CollectionService
    backfill_service: BackfillService
    baseline_service: BaselineService
    api_app: object
    owns_connection: bool = False

    def close(self) -> None:
        """Close the owned database connection."""

        if self.owns_connection:
            self.connection.close()


def build_default_collectors() -> list[Collector]:
    """Return the built-in collector set used by default."""

    return [
        CpuCollector(),
        QpsCollector(),
        HttpCodeCollector(),
        TpCollector(),
    ]


def create_application(
    settings: AppSettings | None = None,
    *,
    connection=None,
    collectors: list[Collector] | None = None,
) -> ApplicationContext:
    """Create the default application wiring for collection and query paths."""

    resolved_settings = settings or AppSettings.from_env()
    owns_connection = connection is None
    resolved_connection = connection or connect_db(resolved_settings.database)

    if resolved_settings.initialize_storage:
        apply_sql_file(resolved_connection, INIT_SQL_PATH)
        initialize_rollups(resolved_connection)

    registry = CollectorRegistry(enabled_names=resolved_settings.enabled_collectors)
    for collector in collectors or build_default_collectors():
        registry.register(collector)

    dispatcher = Dispatcher(
        registry=registry,
        max_concurrency=resolved_settings.dispatcher_max_concurrency,
        retry_limit=resolved_settings.dispatcher_retry_limit,
        task_timeout_seconds=resolved_settings.dispatcher_timeout_seconds,
    )
    repository = TimescaleMetricsRepository(resolved_connection)
    cluster_loader = partial(load_clusters, resolved_settings.cluster_config_path)
    collection_service = CollectionService(cluster_loader, dispatcher, repository)
    backfill_service = BackfillService(collection_service)
    baseline_service = BaselineService(resolved_connection)
    api_app = create_app(baseline_service)

    return ApplicationContext(
        settings=resolved_settings,
        connection=resolved_connection,
        registry=registry,
        dispatcher=dispatcher,
        repository=repository,
        collection_service=collection_service,
        backfill_service=backfill_service,
        baseline_service=baseline_service,
        api_app=api_app,
        owns_connection=owns_connection,
    )
