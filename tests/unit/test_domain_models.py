from __future__ import annotations

from datetime import datetime, timezone

from cluster_metrics_platform.domain.models import MetricPoint, build_labels_fingerprint


def test_build_labels_fingerprint_is_stable_across_key_order() -> None:
    labels_a = {"direction": "in", "class": "2xx"}
    labels_b = {"class": "2xx", "direction": "in"}

    assert build_labels_fingerprint(labels_a) == build_labels_fingerprint(labels_b)


def test_metric_point_computes_labels_fingerprint_and_default_collected_at() -> None:
    window_start = datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc)
    window_end = datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc)

    point = MetricPoint(
        cluster_name="lf-lan-ha1",
        bucket_time=window_start,
        window_start=window_start,
        window_end=window_end,
        metric_name="http_code_count",
        metric_value=42.0,
        labels={"class": "2xx"},
        source_tool="code",
    )

    assert point.labels_fingerprint == build_labels_fingerprint({"class": "2xx"})
    assert point.collected_at == window_end

