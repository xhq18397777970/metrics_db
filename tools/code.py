import time
import requests
import logging
import hashlib


TARGET_STATUS_GROUPS = ("2xx", "4xx", "5xx")


def summarize_status_code_counts(result):
    if not isinstance(result, dict):
        return {group: 0 for group in TARGET_STATUS_GROUPS}

    data = result.get("data")
    if isinstance(data, list) and not data:
        return {"error": "当前查询条件暂无数据"}

    totals = {group: 0 for group in TARGET_STATUS_GROUPS}

    for item in data or []:
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


#鉴权
def npa_summary_data(postdata, apiurl,method="POST"):
    user = "xiehanqi.jackson"
    ctime = str(int(time.time()))
    new_key = f"{user}|{ctime}"
    # 修正这里：使用 hashlib.md5() 来计算哈希值
    api_header_val = f"{hashlib.md5(new_key.encode()).hexdigest()}|{ctime}"
    url = f'http://npa-test.jd.com{apiurl}'
    user_agent = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'
    headers = {'auth-api': api_header_val, 'auth-user': user, 'Content-Type': "application/json", 'User-Agent': user_agent}
    try:
        if method=="POST":
            response = requests.post(url, json=postdata, headers=headers)
        if method=="GET":
            response = requests.get(url, params=postdata, headers=headers)
        response.raise_for_status()
        logging.info(f"code:{response.status_code}, response:{response.text}")
        result = response.json()
        if isinstance(result, dict) and isinstance(result.get("data"), list) and not result["data"]:
            result["message"] = "当前查询条件下暂无数据"
        return result
    except requests.RequestException as e:
        logging.error(f"API request error: {e}")
        return {}
    
    # 获取http_code数据,需要参数，起止时间、集群名称
def get_cluster_status_code_api(groupname,begin_time,end_time):
    postdata = {
        "groupname": groupname,
        "begin_time": begin_time,
        "end_time": end_time
    }
    apiurl = "/prod-api/api/v2/analysis/deeplog/querycode?format=json"
    result = npa_summary_data(postdata, apiurl)
    return summarize_status_code_counts(result)


if __name__ == "__main__":
    r = get_cluster_status_code_api(
        "lf-lan-ha1",
        "2026-03-11 09:00:14",
        "2026-03-11 09:30:14",
    )
    print(r)
