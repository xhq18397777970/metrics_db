from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime
from typing import Any

import requests

TARGET_METRICS = ("cluster_cpu_avg", "net_in_bps_max", "net_out_bps_max")


def calculate_average(values: list[Any]) -> float | None:
    numeric_values = [value for value in values if isinstance(value, (int, float))]
    if not numeric_values:
        return None
    return round(sum(numeric_values) / len(numeric_values), 2)


def extract_metric_averages(result: dict[str, Any]) -> dict[str, float | None]:
    averages = {metric: None for metric in TARGET_METRICS}
    for metric_group in result.get("data", []):
        for series in metric_group.get("series_data", []):
            series_name = series.get("name")
            if series_name in averages:
                averages[series_name] = calculate_average(series.get("value", []))
    return averages


def npa_summary_data(postdata: dict[str, Any], apiurl: str, method: str = "POST") -> dict[str, Any]:
    user = "xiehanqi.jackson"
    ctime = str(int(time.time()))
    new_key = f"{user}|{ctime}"
    api_header_val = f"{hashlib.md5(new_key.encode()).hexdigest()}|{ctime}"
    url = f"http://npa-test.jd.com{apiurl}"
    user_agent = "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)"
    headers = {
        "auth-api": api_header_val,
        "auth-user": user,
        "Content-Type": "application/json",
        "User-Agent": user_agent,
    }
    try:
        if method == "POST":
            response = requests.post(url, json=postdata, headers=headers, timeout=30)
        else:
            response = requests.get(url, params=postdata, headers=headers, timeout=30)
        response.raise_for_status()
        logging.info("code:%s, response:%s", response.status_code, response.text)
        result = response.json()
        if isinstance(result, dict) and isinstance(result.get("data"), list) and not result["data"]:
            result["message"] = "当前查询条件下暂无数据"
        return result
    except requests.RequestException as exc:
        logging.error("API request error: %s", exc)
        return {}


def get_cluster_cpu_metrics(
    groupname: str,
    window_start: datetime | str,
    window_end: datetime | str,
) -> dict[str, float | None] | dict[str, str]:
    postdata = {
        "groupname": groupname,
        "begin_time": format_window_time(window_start),
        "end_time": format_window_time(window_end),
    }
    apiurl = "/prod-api/api/v2/analysis/prometheus/core?format=json"
    result = npa_summary_data(postdata, apiurl)
    if not isinstance(result, dict) or not result:
        return {"error": "接口请求失败"}
    if isinstance(result.get("data"), list) and not result["data"]:
        return {"error": "当前查询条件下暂无数据"}

    averages = extract_metric_averages(result)
    if all(value is None for value in averages.values()):
        return {"error": "接口返回成功，但未获取到有效 cpu 数据"}
    return averages


def npa_analysis_prometheus_core(
    groupname: str,
    begin_time: datetime | str,
    end_time: datetime | str,
) -> dict[str, float | None] | dict[str, str]:
    return get_cluster_cpu_metrics(groupname, begin_time, end_time)


def format_window_time(value: datetime | str) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone()
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, str):
        return value
    raise TypeError("window time must be a datetime or formatted string")


if __name__ == "__main__":
    response = npa_analysis_prometheus_core(
        "lf-lan-ha1",
        "2026-03-11 09:43:14",
        "2026-03-11 10:13:14",
    )
    print(response)
