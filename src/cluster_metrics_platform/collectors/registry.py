"""Collector registration and discovery helpers."""

from __future__ import annotations

from collections.abc import Iterable

from cluster_metrics_platform.collectors.base import Collector


class CollectorRegistry:
    """Registry that stores collector instances in registration order."""

    def __init__(self, enabled_names: Iterable[str] | None = None) -> None:
        self._collectors: dict[str, Collector] = {}
        self._enabled_names = set(enabled_names) if enabled_names is not None else None

    def register(self, collector: Collector) -> None:
        if not getattr(collector, "name", ""):
            raise ValueError("collector name must be defined")
        if collector.name in self._collectors:
            raise ValueError(f"collector {collector.name} is already registered")
        self._collectors[collector.name] = collector

    def enabled_collectors(self) -> list[Collector]:
        if self._enabled_names is None:
            return list(self._collectors.values())
        return [
            collector
            for name, collector in self._collectors.items()
            if name in self._enabled_names
        ]

    def __len__(self) -> int:
        return len(self._collectors)

