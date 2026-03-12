from __future__ import annotations

import tools.code as code_tool
from cluster_metrics_platform.collectors.http_code_collector import HttpCodeCollector


def test_http_code_collector_success(monkeypatch, sample_window) -> None:
    monkeypatch.setattr(
        code_tool,
        "get_cluster_status_code_api",
        lambda *_args: {"2xx": 10, "4xx": 3, "5xx": 1},
    )

    result = HttpCodeCollector().collect("lf-lan-ha1", sample_window)

    assert result.status == "success"
    assert result.error is None
    assert len(result.points) == 3
    assert {point.labels["class"] for point in result.points} == {"2xx", "4xx", "5xx"}


def test_http_code_collector_empty_data(monkeypatch, sample_window) -> None:
    monkeypatch.setattr(
        code_tool,
        "get_cluster_status_code_api",
        lambda *_args: {"error": "当前查询条件下暂无数据"},
    )

    result = HttpCodeCollector().collect("lf-lan-ha1", sample_window)

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.message == "当前查询条件下暂无数据"


def test_http_code_collector_malformed_response(monkeypatch, sample_window) -> None:
    monkeypatch.setattr(
        code_tool,
        "get_cluster_status_code_api",
        lambda *_args: {"2xx": "bad"},
    )

    result = HttpCodeCollector().collect("lf-lan-ha1", sample_window)

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "malformed_response"


def test_http_code_collector_handled_error(monkeypatch, sample_window) -> None:
    monkeypatch.setattr(
        code_tool,
        "get_cluster_status_code_api",
        lambda *_args: {"error": "接口请求失败"},
    )

    result = HttpCodeCollector().collect("lf-lan-ha1", sample_window)

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "tool_error"
