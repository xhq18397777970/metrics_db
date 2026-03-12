from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime
from typing import Any

import requests

TARGET_STATUS_GROUPS = ("2xx", "4xx", "5xx")


def summarize_status_code_counts(result: dict[str, Any]) -> dict[str, int] | dict[str, str]:
    if not isinstance(result, dict) or not result:
        return {"error": "接口返回格式异常"}

    data = result.get("data")
    if not isinstance(data, list):
        return {"error": "接口返回中缺少 data"}
    if not data:
        return {"error": "当前查询条件下暂无数据"}

    totals = {group: 0 for group in TARGET_STATUS_GROUPS}

    for item in data:
        title = item.get("title", "")
        group = title.rsplit("__", 1)[-1]
        if group not in totals:
            continue

        for series in item.get("series_data", []):
            values = series.get("value", [])
            totals[group] += sum(
                value for value in values if isinstance(value, (int, float))
            )

    return totals


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


def get_cluster_status_code_api(
    groupname: str,
    begin_time: datetime | str,
    end_time: datetime | str,
) -> dict[str, int] | dict[str, str]:
    postdata = {
        "groupname": groupname,
        "begin_time": format_window_time(begin_time),
        "end_time": format_window_time(end_time),
    }
    apiurl = "/prod-api/api/v2/analysis/deeplog/querycode?format=json"
    result = npa_summary_data(postdata, apiurl)
    return summarize_status_code_counts(result)


def format_window_time(value: datetime | str) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone()
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, str):
        return value
    raise TypeError("window time must be a datetime or formatted string")


if __name__ == "__main__":
    response = get_cluster_status_code_api(
        "ozhl-lan-ha1",
        "2026-03-12 09:00:14",
        "2026-03-12 09:30:14",
    )
    print(response)
