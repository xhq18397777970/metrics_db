from __future__ import annotations

import tools.tp as tp_tool
from cluster_metrics_platform.collectors.tp_collector import TpCollector


def test_tp_collector_success(monkeypatch, sample_window) -> None:
    monkeypatch.setattr(tp_tool, "get_cluster_tp_api", lambda *_args: {"tp": 18.2})

    result = TpCollector().collect("lf-lan-ha1", sample_window)

    assert result.status == "success"
    assert result.error is None
    assert len(result.points) == 1
    assert result.points[0].metric_name == "tp_avg"
    assert result.points[0].metric_value == 18.2


def test_tp_collector_empty_data(monkeypatch, sample_window) -> None:
    monkeypatch.setattr(
        tp_tool,
        "get_cluster_tp_api",
        lambda *_args: {"error": "当前查询条件下暂无数据"},
    )

    result = TpCollector().collect("lf-lan-ha1", sample_window)

    assert result.status == "success"
    assert result.error is None
    assert len(result.points) == 1
    assert result.points[0].metric_name == "tp_avg"
    assert result.points[0].metric_value == 0.0


def test_tp_collector_malformed_response(monkeypatch, sample_window) -> None:
    monkeypatch.setattr(tp_tool, "get_cluster_tp_api", lambda *_args: {"tp": "bad"})

    result = TpCollector().collect("lf-lan-ha1", sample_window)

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "malformed_response"


def test_tp_collector_handled_error(monkeypatch, sample_window) -> None:
    monkeypatch.setattr(tp_tool, "get_cluster_tp_api", lambda *_args: {"error": "接口请求失败"})

    result = TpCollector().collect("lf-lan-ha1", sample_window)

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "tool_error"
