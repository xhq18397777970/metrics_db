import time
import requests
import logging
import hashlib


TARGET_METRICS = ("cluster_cpu_avg", "net_in_bps_max", "net_out_bps_max")


def calculate_average(values):
    numeric_values = [value for value in values if isinstance(value, (int, float))]
    if not numeric_values:
        return None
    return round(sum(numeric_values) / len(numeric_values), 2)


def extract_metric_averages(result):
    averages = {metric: None for metric in TARGET_METRICS}
    for metric_group in result.get("data", []):
        for series in metric_group.get("series_data", []):
            series_name = series.get("name")
            if series_name in averages:
                averages[series_name] = calculate_average(series.get("value", []))
    return averages


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
    
    
#获取时间段，集群CPU指标
def npa_analysis_prometheus_core():
    postdata = {"groupname":"lf-lan-ha1","begin_time":"2026-03-11 09:43:14","end_time":"2026-03-11 10:13:14"}
    apiurl= "/prod-api/api/v2/analysis/prometheus/core?format=json"
    result = npa_summary_data(postdata,apiurl)
    if isinstance(result, dict) and isinstance(result.get("data"), list) and not result["data"]:
        return {
            "error":"当前查询条件下暂无数据"
            }
    return extract_metric_averages(result)


if __name__ == "__main__":
    r = npa_analysis_prometheus_core()
    print(r)
