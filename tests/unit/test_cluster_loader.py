from __future__ import annotations

import json
from pathlib import Path

from cluster_metrics_platform.config.cluster_loader import load_cluster_groups, load_clusters


def test_load_cluster_groups_preserves_group_counts() -> None:
    config_path = Path(__file__).resolve().parents[2] / "cluster.json"

    groups = load_cluster_groups(config_path)

    assert len(groups) >= 2
    assert "lan-ha-jd" in groups
    assert "pub-jfe-jd" in groups
    assert all(isinstance(groups[group_name], list) for group_name in groups)


def test_load_clusters_returns_flattened_cluster_list() -> None:
    config_path = Path(__file__).resolve().parents[2] / "cluster.json"

    groups = load_cluster_groups(config_path)
    clusters = load_clusters(config_path)

    assert len(clusters) == sum(len(group_clusters) for group_clusters in groups.values())
    assert all(cluster.group_name in groups for cluster in clusters)
    assert all(cluster.cluster_name for cluster in clusters)
    assert all(cluster.application_name for cluster in clusters)


def test_load_clusters_supports_explicit_application_name(tmp_path) -> None:
    config_path = tmp_path / "clusters.json"
    config_path.write_text(
        json.dumps(
            {
                "service-a": {
                    "application_name": "应用A",
                    "clusters": ["cluster-a", "cluster-b"],
                }
            }
        ),
        encoding="utf-8",
    )

    clusters = load_clusters(config_path)

    assert [cluster.cluster_name for cluster in clusters] == ["cluster-a", "cluster-b"]
    assert {cluster.application_name for cluster in clusters} == {"应用A"}
