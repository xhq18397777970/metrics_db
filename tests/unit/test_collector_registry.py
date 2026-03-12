from __future__ import annotations

import pytest

from cluster_metrics_platform.collectors.base import Collector
from cluster_metrics_platform.collectors.registry import CollectorRegistry
from cluster_metrics_platform.domain.models import CollectorResult, TimeWindow


class DummyCollector(Collector):
    def __init__(self, name: str) -> None:
        self.name = name

    def collect(self, cluster: str, window: TimeWindow) -> CollectorResult:
        return self._success([])


def test_registry_returns_collectors_in_registration_order() -> None:
    registry = CollectorRegistry()
    first = DummyCollector("first")
    second = DummyCollector("second")

    registry.register(first)
    registry.register(second)

    assert registry.enabled_collectors() == [first, second]


def test_registry_filters_enabled_collectors() -> None:
    registry = CollectorRegistry(enabled_names=["second"])
    first = DummyCollector("first")
    second = DummyCollector("second")

    registry.register(first)
    registry.register(second)

    assert registry.enabled_collectors() == [second]


def test_registry_rejects_duplicate_collectors() -> None:
    registry = CollectorRegistry()
    registry.register(DummyCollector("cpu"))

    with pytest.raises(ValueError):
        registry.register(DummyCollector("cpu"))

