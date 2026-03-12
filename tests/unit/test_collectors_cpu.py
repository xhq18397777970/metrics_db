from __future__ import annotations

import pytest

import tools.cpu as cpu_tool
from cluster_metrics_platform.collectors.cpu_collector import CpuCollector


@pytest.fixture
def cpu_success_payload() -> dict[str, float]:
    return {
        "cluster_cpu_avg": 12.5,
        "net_in_bps_max": 100.0,
        "net_out_bps_max": 120.0,
    }


def test_cpu_collector_success(monkeypatch, sample_window, cpu_success_payload) -> None:
    monkeypatch.setattr(cpu_tool, "get_cluster_cpu_metrics", lambda *_args: cpu_success_payload)

    result = CpuCollector().collect("lf-lan-ha1", sample_window)

    assert result.status == "success"
    assert result.error is None
    assert [point.metric_name for point in result.points] == ["cpu_avg", "net_bps", "net_bps"]
    assert [point.labels for point in result.points] == [
        {},
        {"direction": "in"},
        {"direction": "out"},
    ]


def test_cpu_collector_empty_data(monkeypatch, sample_window) -> None:
    monkeypatch.setattr(
        cpu_tool,
        "get_cluster_cpu_metrics",
        lambda *_args: {"error": "当前查询条件下暂无数据"},
    )

    result = CpuCollector().collect("lf-lan-ha1", sample_window)

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.message == "当前查询条件下暂无数据"


def test_cpu_collector_malformed_response(monkeypatch, sample_window) -> None:
    monkeypatch.setattr(
        cpu_tool,
        "get_cluster_cpu_metrics",
        lambda *_args: {"cluster_cpu_avg": "bad", "net_in_bps_max": None},
    )

    result = CpuCollector().collect("lf-lan-ha1", sample_window)

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "malformed_response"


def test_cpu_collector_handled_error(monkeypatch, sample_window) -> None:
    monkeypatch.setattr(
        cpu_tool,
        "get_cluster_cpu_metrics",
        lambda *_args: {"error": "接口请求失败"},
    )

    result = CpuCollector().collect("lf-lan-ha1", sample_window)

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "tool_error"
