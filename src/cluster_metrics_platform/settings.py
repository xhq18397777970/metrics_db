"""Runtime settings for the cluster metrics platform."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from cluster_metrics_platform.storage.db import DatabaseConfig

DEFAULT_ENABLED_COLLECTORS = ("cpu", "qps", "http_code", "tp")
DEFAULT_CLUSTER_CONFIG_ENV = "CLUSTER_METRICS_CLUSTER_CONFIG"
DEFAULT_ENABLED_COLLECTORS_ENV = "CLUSTER_METRICS_ENABLED_COLLECTORS"
DEFAULT_INIT_STORAGE_ENV = "CLUSTER_METRICS_INIT_STORAGE"
DEFAULT_DISPATCH_MAX_CONCURRENCY_ENV = "CLUSTER_METRICS_MAX_CONCURRENCY"
DEFAULT_DISPATCH_RETRY_LIMIT_ENV = "CLUSTER_METRICS_RETRY_LIMIT"
DEFAULT_DISPATCH_TIMEOUT_ENV = "CLUSTER_METRICS_TIMEOUT_SECONDS"


@dataclass(frozen=True, slots=True)
class AppSettings:
    """Configuration required to bootstrap the application."""

    cluster_config_path: Path
    database: DatabaseConfig
    enabled_collectors: tuple[str, ...] = DEFAULT_ENABLED_COLLECTORS
    dispatcher_max_concurrency: int = 10
    dispatcher_retry_limit: int = 0
    dispatcher_timeout_seconds: float = 30.0
    initialize_storage: bool = False

    @classmethod
    def from_env(cls) -> "AppSettings":
        cluster_path = Path(
            os.getenv(
                DEFAULT_CLUSTER_CONFIG_ENV,
                Path(__file__).resolve().parents[2] / "cluster.json",
            )
        )
        enabled_collectors = _parse_enabled_collectors(
            os.getenv(DEFAULT_ENABLED_COLLECTORS_ENV)
        )
        return cls(
            cluster_config_path=cluster_path,
            database=DatabaseConfig.from_env(),
            enabled_collectors=enabled_collectors,
            dispatcher_max_concurrency=int(
                os.getenv(DEFAULT_DISPATCH_MAX_CONCURRENCY_ENV, "10")
            ),
            dispatcher_retry_limit=int(os.getenv(DEFAULT_DISPATCH_RETRY_LIMIT_ENV, "0")),
            dispatcher_timeout_seconds=float(
                os.getenv(DEFAULT_DISPATCH_TIMEOUT_ENV, "30.0")
            ),
            initialize_storage=_parse_bool(
                os.getenv(DEFAULT_INIT_STORAGE_ENV),
                default=False,
            ),
        )


def _parse_enabled_collectors(raw_value: str | None) -> tuple[str, ...]:
    if not raw_value:
        return DEFAULT_ENABLED_COLLECTORS
    return tuple(
        name.strip()
        for name in raw_value.split(",")
        if name.strip()
    )


def _parse_bool(raw_value: str | None, *, default: bool) -> bool:
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}
