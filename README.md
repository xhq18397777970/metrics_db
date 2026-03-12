# Cluster Metrics Platform

一个面向运维人员的集群指标平台。系统按 5 分钟粒度采集所有集群的 `CPU / QPS / HTTP Code / TP / BPS` 指标，写入 TimescaleDB，并提供补数和基线查询接口。

## 功能

- 5 分钟标准时间窗采集
- Collector 插件化扩展
- TimescaleDB 原始表、连续聚合和 retention policy
- 手工单窗口采集
- 历史补数
- 基线查询 API

## 当前目录

```text
src/cluster_metrics_platform/
  bootstrap.py
  main.py
  settings.py
  collectors/
  services/
  storage/
sql/
  001_init.sql
  002_rollups.sql
tests/
```

## 本地环境

默认使用 `conda` 环境 `agent`。

### 1. 激活环境并安装项目

```bash
conda activate agent
python -m pip install -e ".[dev]"
```

### 2. 安装 PostgreSQL 17 和 TimescaleDB

```bash
brew tap timescale/tap
brew install postgresql@17 timescale/tap/timescaledb timescale/tap/timescaledb-tools
timescaledb-tune --quiet --yes --pg-config /opt/homebrew/opt/postgresql@17/bin/pg_config
timescaledb_move.sh
brew services start postgresql@17
```

### 3. 创建数据库并启用扩展

```bash
/opt/homebrew/opt/postgresql@17/bin/psql -d postgres -c "CREATE DATABASE cluster_metrics_test"
/opt/homebrew/opt/postgresql@17/bin/psql -d cluster_metrics_test -c "CREATE EXTENSION IF NOT EXISTS timescaledb"
```

如果数据库已经存在，跳过第一条即可。

## 环境变量

```bash
export CLUSTER_METRICS_DATABASE_URL=postgresql:///cluster_metrics_test
export CLUSTER_METRICS_CLUSTER_CONFIG=/Users/xiehanqi.jackson/Documents/workspace/metrics_database/cluster.json
export CLUSTER_METRICS_INIT_STORAGE=true
```

可选变量：

- `CLUSTER_METRICS_ENABLED_COLLECTORS=cpu,qps,http_code,tp`
- `CLUSTER_METRICS_MAX_CONCURRENCY=10`
- `CLUSTER_METRICS_RETRY_LIMIT=0`
- `CLUSTER_METRICS_TIMEOUT_SECONDS=30`

`CLUSTER_METRICS_INIT_STORAGE=true` 会在启动时初始化 `metric_points`、rollup 和 policy；首次启动建议打开。

## 启动 API

```bash
conda run -n agent python -m cluster_metrics_platform.main serve-api --host 127.0.0.1 --port 8000
```

启动后会暴露：

- `POST /api/v1/baselines/query`

### 查询示例

```bash
curl -X POST http://127.0.0.1:8000/api/v1/baselines/query \
  -H 'Content-Type: application/json' \
  -d '{
    "cluster_name": "lf-lan-ha1",
    "metric_name": "cpu_avg",
    "start_time": "2026-03-12T10:00:00+08:00",
    "end_time": "2026-03-12T13:00:00+08:00",
    "mode": "historical_range",
    "lookback_days": 7,
    "aggregations": ["avg", "p50", "p95"]
  }'
```

## 手工采集

单窗口采集：

```bash
conda run -n agent python -m cluster_metrics_platform.main collect-window \
  --window-end "2026-03-12T10:05:00+08:00" \
  --cluster lf-lan-ha1
```

历史补数：

```bash
conda run -n agent python -m cluster_metrics_platform.main backfill \
  --start "2026-03-12T10:00:00+08:00" \
  --end "2026-03-12T12:00:00+08:00" \
  --cluster lf-lan-ha1
```

持续采集：

```bash
conda run -n agent python -m cluster_metrics_platform.main run-scheduler
```

如果只想验证最近的一个或几个时间窗，可以加 `--iterations`：

```bash
conda run -n agent python -m cluster_metrics_platform.main run-scheduler --iterations 1
```

## 运行测试

```bash
TIMESCALE_TEST_DSN=postgresql:///cluster_metrics_test \
conda run -n agent pytest tests/unit tests/integration tests/e2e tests/smoke -q

conda run -n agent ruff check src tests tools
```

## 当前实现边界

- 第一版只提供 CLI 和 HTTP API，不含前端页面和告警
- 生产部署方式未固化
- `tools` API 真实调用依赖外部系统鉴权和可达性
