from __future__ import annotations

import hashlib
import time
from datetime import datetime
from typing import Any

import requests

CONFIG = {
    "appCode": "JC_PIDLB",
    "token": "9b78f9ab773774f5b2c4b627ff007152",
    "api_url": "http://deeplog-lb-api.jd.com/",
}


def calculate_average(values: list[float]) -> float | None:
    numeric_values = [value for value in values if isinstance(value, (int, float))]
    if not numeric_values:
        return None
    return round(sum(numeric_values) / len(numeric_values), 2)


def extract_qps_values(payload: Any) -> list[float]:
    values: list[float] = []

    def collect(node: Any) -> None:
        if isinstance(node, (int, float)):
            values.append(node)
            return

        if isinstance(node, list):
            if node and all(isinstance(item, (int, float)) for item in node):
                values.extend(node)
                return
            for item in node:
                collect(item)
            return

        if not isinstance(node, dict):
            return

        for key in ("response", "data", "result", "results", "series_data", "value", "values"):
            if key in node:
                collect(node[key])

    collect(payload)
    return values


def build_error_result(
    message: str,
    status_code: int | None = None,
    error_code: Any = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {"error": message}
    if status_code is not None:
        result["status_code"] = status_code
    if error_code is not None:
        result["code"] = error_code
    return result


def get_np_auth_headers(app_code: str, token: str) -> dict[str, str]:
    now = datetime.now()
    time_str = now.strftime("%H%M%Y%m%d")
    timestamp = str(int(time.time() * 1000))
    sign_str = f"#{token}NP{time_str}"
    sign = hashlib.md5(sign_str.encode("utf-8")).hexdigest()

    return {
        "Content-Type": "application/json;charset=utf-8",
        "appCode": app_code,
        "sign": sign,
        "time": timestamp,
    }


def get_cluster_qps(
    cluster_name: str,
    window_start: datetime | str | int,
    window_end: datetime | str | int,
) -> dict[str, Any]:
    headers = get_np_auth_headers(CONFIG["appCode"], CONFIG["token"])
    url = f"{CONFIG['api_url']}v1/search"
    start_time = to_timestamp_ms(window_start)
    end_time = to_timestamp_ms(window_end)

    params = {
        "size": 10,
        "bizName": "lbha",
        "resource": "count",
        "timeRange": {
            "start": start_time,
            "end": end_time,
        },
        "interval": "10s",
        "match": [{
            "eq": {
                "lb-node-name": [cluster_name],
            },
        }],
        "algorithm": {
            "algorithmName": "sum",
        },
    }
    try:
        response = requests.post(url, headers=headers, json=params, timeout=30)
        try:
            raw_data = response.json()
        except ValueError:
            raw_data = None

        if response.status_code != 200:
            message = "接口请求失败"
            if isinstance(raw_data, dict) and raw_data.get("message"):
                message = str(raw_data["message"])
            elif response.text:
                message = response.text.strip()
            return build_error_result(message, status_code=response.status_code)

        if not isinstance(raw_data, dict):
            return build_error_result("接口返回不是合法 JSON", status_code=response.status_code)

        if raw_data.get("code") not in (0, "0", None):
            return build_error_result(
                raw_data.get("message") or "接口返回业务错误",
                status_code=response.status_code,
                error_code=raw_data.get("code"),
            )

        qps = calculate_average(extract_qps_values(raw_data))
        if qps is None:
            return build_error_result(
                "接口返回成功，但未获取到有效 qps 数据",
                status_code=response.status_code,
            )

        return {"qps": qps}

    except requests.exceptions.RequestException as exc:
        status_code = None
        response = getattr(exc, "response", None)
        if response is not None:
            status_code = response.status_code
        return build_error_result(f"请求失败: {exc}", status_code=status_code)


def to_timestamp_ms(value: datetime | str | int) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    if isinstance(value, str):
        return datetime_str_to_timestamp(value)
    raise TypeError("window time must be datetime, formatted string, or millisecond timestamp")


def datetime_str_to_timestamp(dt_str: str) -> int:
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    return int(dt.timestamp() * 1000)


if __name__ == "__main__":
    result = get_cluster_qps(
        "lf-lan-ha1",
        "2025-03-11 10:00:00",
        "2025-03-11 10:30:00",
    )
    print(result)
