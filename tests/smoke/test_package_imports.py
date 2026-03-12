import importlib

import pytest


@pytest.mark.parametrize(
    "module_name",
    [
        "api",
        "api.app",
        "api.baseline_query_service",
        "api.main",
        "api.metric_statistics_service",
        "cluster_metrics_platform",
        "cluster_metrics_platform.bootstrap",
        "cluster_metrics_platform.api",
        "cluster_metrics_platform.collectors",
        "cluster_metrics_platform.config",
        "cluster_metrics_platform.domain",
        "cluster_metrics_platform.orchestrator",
        "cluster_metrics_platform.services",
        "cluster_metrics_platform.settings",
        "cluster_metrics_platform.storage",
    ],
)
def test_module_imports(module_name: str) -> None:
    module = importlib.import_module(module_name)

    assert module is not None
