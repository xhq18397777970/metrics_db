from __future__ import annotations

from pathlib import Path

from cluster_metrics_platform.config.cluster_loader import load_cluster_groups, load_clusters


def test_load_cluster_groups_preserves_group_counts() -> None:
    config_path = Path(__file__).resolve().parents[2] / "cluster.json"

    groups = load_cluster_groups(config_path)

    assert list(groups) == ["lan-ha-jd", "pub-jfe-jd"]
    assert len(groups["lan-ha-jd"]) == 46
    assert len(groups["pub-jfe-jd"]) == 24


def test_load_clusters_returns_flattened_cluster_list() -> None:
    config_path = Path(__file__).resolve().parents[2] / "cluster.json"

    clusters = load_clusters(config_path)

    assert len(clusters) == 70
    assert clusters[0].cluster_name == "hk1-lan-ha1"
    assert clusters[0].group_name == "lan-ha-jd"
    assert clusters[-1].cluster_name == "sq-pub-tjfe1"
    assert clusters[-1].group_name == "pub-jfe-jd"
