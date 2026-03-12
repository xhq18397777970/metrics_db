"""Core domain models for the cluster metrics platform."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from hashlib import sha256
from typing import Any, Mapping
from uuid import UUID


def build_labels_fingerprint(labels: Mapping[str, str] | None = None) -> str:
    """Return a stable fingerprint for metric labels."""
    normalized_labels = dict(labels or {})
    serialized = json.dumps(
        normalized_labels,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return sha256(serialized.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class TimeWindow:
    """Canonical time bucket used across collection and querying."""

    bucket_time: datetime
    start_time: datetime
    end_time: datetime
    window_seconds: int


@dataclass(frozen=True, slots=True)
class ClusterConfig:
    """Cluster definition loaded from static configuration."""

    group_name: str
    cluster_name: str
    enabled: bool = True


@dataclass(frozen=True, slots=True)
class CollectorError:
    """Normalized collector error payload."""

    message: str
    code: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class MetricPoint:
    """Normalized metric payload ready for storage."""

    cluster_name: str
    bucket_time: datetime
    window_start: datetime
    window_end: datetime
    metric_name: str
    metric_value: float
    labels: dict[str, str] = field(default_factory=dict)
    labels_fingerprint: str = ""
    source_tool: str = ""
    collected_at: datetime | None = None

    def __post_init__(self) -> None:
        self.labels = dict(self.labels)
        if not self.labels_fingerprint:
            self.labels_fingerprint = build_labels_fingerprint(self.labels)
        if self.collected_at is None:
            self.collected_at = self.window_end


@dataclass(slots=True)
class CollectorResult:
    """Result of one collector execution for one cluster window."""

    status: str
    points: list[MetricPoint] = field(default_factory=list)
    error: CollectorError | None = None


@dataclass(frozen=True, slots=True)
class CollectionRun:
    """Execution record for one collector over one cluster window."""

    run_id: UUID
    cluster_name: str
    collector_name: str
    bucket_time: datetime
    status: str
    retry_count: int
    started_at: datetime
    finished_at: datetime
    error_code: str | None = None
    error_message: str | None = None


@dataclass(frozen=True, slots=True)
class BaselineQuery:
    """Baseline query request model."""

    cluster_name: str
    metric_name: str
    start_time: datetime
    end_time: datetime
    mode: str
    lookback_days: int = 7
    aggregations: tuple[str, ...] = ("avg", "p50", "p95")


@dataclass(frozen=True, slots=True)
class BaselineResponse:
    """Baseline query response model."""

    cluster_name: str
    metric_name: str
    mode: str
    start_time: datetime
    end_time: datetime
    baseline_summary: dict[str, float]
    points: tuple[dict[str, Any], ...] = ()
    status: str = "success"
