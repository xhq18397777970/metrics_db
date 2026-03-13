from __future__ import annotations

import tools.qps as qps_tool
from cluster_metrics_platform.collectors.qps_collector import QpsCollector


def test_qps_collector_success(monkeypatch, sample_window) -> None:
    monkeypatch.setattr(qps_tool, "get_cluster_qps", lambda *_args: {"qps": 321.5})

    result = QpsCollector().collect("lf-lan-ha1", sample_window)

    assert result.status == "success"
    assert result.error is None
    assert len(result.points) == 1
    assert result.points[0].metric_name == "qps_avg"
    assert result.points[0].metric_value == 321.5


def test_qps_collector_empty_data(monkeypatch, sample_window) -> None:
    monkeypatch.setattr(
        qps_tool,
        "get_cluster_qps",
        lambda *_args: {"error": "当前查询条件下暂无数据"},
    )

    result = QpsCollector().collect("lf-lan-ha1", sample_window)

    assert result.status == "success"
    assert result.error is None
    assert len(result.points) == 1
    assert result.points[0].metric_name == "qps_avg"
    assert result.points[0].metric_value == 0.0


def test_qps_collector_malformed_response(monkeypatch, sample_window) -> None:
    monkeypatch.setattr(qps_tool, "get_cluster_qps", lambda *_args: {"qps": "bad"})

    result = QpsCollector().collect("lf-lan-ha1", sample_window)

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "malformed_response"


def test_qps_collector_handled_error(monkeypatch, sample_window) -> None:
    monkeypatch.setattr(qps_tool, "get_cluster_qps", lambda *_args: {"error": "接口请求失败"})

    result = QpsCollector().collect("lf-lan-ha1", sample_window)

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "tool_error"
