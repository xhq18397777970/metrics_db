"""Cluster configuration loading helpers."""

from __future__ import annotations

import json
from pathlib import Path

from cluster_metrics_platform.domain.models import ClusterConfig


def load_cluster_groups(path: str | Path) -> dict[str, list[ClusterConfig]]:
    """Load grouped cluster definitions from the cluster configuration file."""
    config_path = Path(path)
    raw_data = json.loads(config_path.read_text(encoding="utf-8"))

    grouped_clusters: dict[str, list[ClusterConfig]] = {}
    for group_name, group_data in raw_data.items():
        clusters = group_data.get("clusters")
        if not isinstance(clusters, list):
            raise ValueError(f"group {group_name} is missing a valid clusters list")
        application_name = group_data.get("application_name", group_name)
        if not isinstance(application_name, str):
            raise ValueError(f"group {group_name} has an invalid application_name")

        grouped_clusters[group_name] = [
            ClusterConfig(
                group_name=group_name,
                cluster_name=cluster_name,
                application_name=application_name,
            )
            for cluster_name in clusters
            if isinstance(cluster_name, str)
        ]

    return grouped_clusters


def load_clusters(path: str | Path) -> list[ClusterConfig]:
    """Load a flattened list of clusters while preserving group order."""
    grouped_clusters = load_cluster_groups(path)
    return [cluster for clusters in grouped_clusters.values() for cluster in clusters]
