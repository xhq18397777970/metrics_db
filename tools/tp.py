from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime
from typing import Any

import requests


def build_error_result(message: str) -> dict[str, str]:
    return {"error": message}


def calculate_tp(result: Any) -> dict[str, float] | dict[str, str]:
    if not isinstance(result, dict):
        return build_error_result("接口返回格式异常")

    if result.get("code") not in (200, "200", 0, "0", None):
        return build_error_result(str(result.get("message") or "接口返回业务错误"))

    data = result.get("data")
    if data == {}:
        return build_error_result("当前条件查询返回数据为空")
    if not isinstance(data, dict):
        return build_error_result("接口返回中缺少 data")

    series_data = data.get("series_data")
    if not isinstance(series_data, list) or not series_data:
        return build_error_result("接口返回中缺少 series_data")

    series_map: dict[str, list[Any]] = {}
    for series in series_data:
        if not isinstance(series, dict):
            continue
        name = series.get("name")
        values = series.get("value")
        if isinstance(name, str) and isinstance(values, list):
            series_map[name] = values

    total_values = series_map.get("total_delay") or series_map.get("total_delay_tp")
    srv_values = series_map.get("srv_delay") or series_map.get("srv_delay_tp")
    if not isinstance(total_values, list) or not isinstance(srv_values, list):
        return build_error_result("接口返回中缺少 total_delay 或 srv_delay")

    diff_values = [
        total - srv
        for total, srv in zip(total_values, srv_values)
        if isinstance(total, (int, float)) and isinstance(srv, (int, float))
    ]
    if not diff_values:
        return build_error_result("未获取到有效 tp 数据")

    return {"tp": round(sum(diff_values) / len(diff_values), 2)}


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


def get_cluster_tp_api(
    groupname: str,
    begin_time: datetime | str,
    end_time: datetime | str,
) -> dict[str, float] | dict[str, str]:
    postdata = {
        "groupname": groupname,
        "begin_time": format_window_time(begin_time),
        "end_time": format_window_time(end_time),
    }
    apiurl = "/prod-api/api/v2/analysis/deeplog/querytpn?format=json"
    result = npa_summary_data(postdata, apiurl)
    return calculate_tp(result)


def format_window_time(value: datetime | str) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, str):
        return value
    raise TypeError("window time must be a datetime or formatted string")


if __name__ == "__main__":
    response = get_cluster_tp_api(
        "lf-lan-ha1",
        "2026-03-11 10:25:00",
        "2026-03-11 10:30:00",
    )
    print(response)
