# 集群指标库任务执行文档

## 1. 文档目的

本文档基于 [executable_design.md](/Users/xiehanqi.jackson/Documents/workspace/metrics_database/docs/executable_design.md) 继续细化，目标是把方案拆成可以逐项执行的任务，并为每个任务明确：

- 测试环境
- 具体操作步骤
- 验证命令或验证方式
- 通过标准
- 失败后的处理方式

执行规则只有一条：

**当前任务未通过验证，不允许进入下一个任务。**

## 1.1 执行环境前提

本文档默认所有命令都在 conda 环境 `agent` 中执行。  
本机已确认存在该环境，Python 版本为 `3.10.18`。

交互式终端执行前先运行：

```bash
conda activate agent
```

如果是脚本或非交互方式执行，优先使用：

```bash
conda run -n agent <command>
```

## 2. 测试环境定义

所有任务必须在指定测试环境中完成验证。测试环境统一定义如下。

### ENV-1：本地单元测试环境

用途：

- 校验纯逻辑代码
- 校验工具标准化输出
- 校验时间窗和集群加载逻辑
- 校验调度器并发行为

组成：

- conda 环境 `agent`
- `pytest`
- `pytest-asyncio`
- `unittest.mock` 或 `pytest-mock`
- 固定测试数据夹具

特点：

- 不依赖真实 API
- 不依赖真实数据库
- 结果可重复

### ENV-2：本地集成测试环境

用途：

- 校验应用和数据库的交互
- 校验批量写入、幂等写入、连续聚合
- 校验 fake collector 到数据库的端到端链路

组成：

- conda 环境 `agent`
- Docker
- TimescaleDB 容器
- 本地 fake API server 或 mock collector

特点：

- 不使用真实线上接口
- 可反复销毁重建

### ENV-3：联调测试环境

用途：

- 校验真实工具接口可用性
- 校验真实数据是否能被标准化并写入测试库

组成：

- 真实 API 访问能力
- 独立测试数据库或独立 schema
- 仅使用少量集群做联调

建议范围：

- 先 1 个集群
- 再 5 个集群

### ENV-4：灰度运行环境

用途：

- 校验定时调度
- 校验真实 5 分钟时间窗连续采集
- 校验补数逻辑

组成：

- 真实 API
- 真实数据库中的独立 schema 或影子表
- 完整调度链路

建议灰度范围：

- 第一天只采 5 个集群
- 稳定后扩到全部 70 个集群

## 3. 执行总规则

### 3.1 分支规则

每个阶段使用单独分支开发，例如：

```bash
git checkout -b feature/stage-0-contract
git checkout -b feature/stage-1-collector
```

### 3.2 提交流程

每个任务都遵守以下顺序：

1. 先补测试样例或测试夹具
2. 再实现最小代码
3. 在对应测试环境中执行验证
4. 测试通过后再提交代码
5. 提交说明中写清楚任务编号

### 3.3 门禁规则

进入下一任务前必须同时满足：

- 对应测试环境全部通过
- 本任务产出文件齐全
- 日志或结果截图可证明成功
- 无已知阻塞问题未处理

## 4. 阶段与任务拆解

---

## 阶段 A：工程测试基线

目标：先把测试和运行底座搭起来，否则后面的“每步可验证”无法落地。

### TASK-A1：初始化开发与测试依赖

测试环境：

- ENV-1

执行步骤：

1. 新增 Python 项目依赖配置文件
2. 加入开发依赖：`pytest`、`pytest-asyncio`、`pytest-mock`
3. 加入数据库依赖：`psycopg` 或 `asyncpg`
4. 加入 HTTP 客户端依赖：`httpx`
5. 增加格式化和静态检查工具：`ruff`

建议产出：

- `pyproject.toml`
- `requirements-dev.txt` 或等效配置

验证方式：

1. 激活 `agent` 环境
2. 安装依赖
3. 运行测试框架空跑

建议验证命令：

```bash
conda activate agent
pip install -U pip
pip install -e .[dev]
pytest -q
```

通过标准：

- `agent` 环境可正常使用
- 开发依赖可成功安装
- `pytest` 可正常执行

失败处理：

- 依赖安装失败时，不允许开始任何业务代码开发

### TASK-A2：建立测试目录和基础夹具

测试环境：

- ENV-1

执行步骤：

1. 新增 `tests/` 目录
2. 增加 `tests/conftest.py`
3. 增加最小 smoke test
4. 增加 `tests/fixtures/` 目录，准备后续工具响应样例

建议产出：

- `tests/smoke/test_smoke.py`
- `tests/conftest.py`
- `tests/fixtures/README.md`

建议验证命令：

```bash
pytest tests/smoke -q
```

通过标准：

- smoke test 通过
- 目录结构稳定

失败处理：

- smoke test 失败时，不允许进入阶段 B

### TASK-A3：建立本地数据库测试环境

测试环境：

- ENV-2

执行步骤：

1. 新增 `docker-compose.test.yml`
2. 启动 TimescaleDB 容器
3. 增加数据库连通性测试

建议产出：

- `docker-compose.test.yml`
- `tests/integration/test_db_connection.py`

建议验证命令：

```bash
docker compose -f docker-compose.test.yml up -d
pytest tests/integration/test_db_connection.py -q
docker compose -f docker-compose.test.yml down
```

通过标准：

- TimescaleDB 容器可正常启动
- 测试代码能成功连接数据库

失败处理：

- 数据库环境未打通，不允许进入数据库相关实现

---

## 阶段 B：冻结工具契约

目标：统一现有 `tools` 的输入、输出、错误语义。

### TASK-B1：为每个工具定义标准输入模型

测试环境：

- ENV-1

执行步骤：

1. 定义统一时间窗对象：`window_start`、`window_end`、`bucket_time`
2. 统一工具入参为：
   - `cluster_name`
   - `window_start`
   - `window_end`
3. 明确时间格式规范

建议规范：

- 内部统一使用 `datetime`
- 调用工具前在适配层完成格式转换
- 不允许业务层直接处理“字符串时间”和“毫秒时间戳”混用

验证方式：

- 对时间格式转换函数编写单元测试

建议验证命令：

```bash
pytest tests/unit/test_time_window.py -q
```

通过标准：

- 时间窗转换结果稳定
- 能同时适配 `cpu/qps/code/tp` 四类工具

失败处理：

- 任一工具仍需业务代码手工拼时间，禁止进入下一任务

### TASK-B2：为每个工具定义标准输出模型

测试环境：

- ENV-1

执行步骤：

1. 定义统一输出结构 `MetricPoint`
2. 定义统一错误结构 `CollectorError`
3. 规定 `metric_name` 命名：
   - `cpu_avg`
   - `net_bps`
   - `qps_avg`
   - `http_code_count`
   - `tp_avg`

字段建议：

- `cluster_name`
- `bucket_time`
- `metric_name`
- `metric_value`
- `labels`
- `source_tool`

验证方式：

1. 为 `cpu.py` 准备 fixture
2. 为 `qps.py` 准备 fixture
3. 为 `code.py` 准备 fixture
4. 为 `tp.py` 准备 fixture
5. 分别验证能转换为统一结构

建议验证命令：

```bash
pytest tests/unit/test_metric_contracts.py -q
```

通过标准：

- 四个工具都能产出统一结构
- 多值指标可正确展开为多条 `MetricPoint`
- 错误能统一落为 `CollectorError`

失败处理：

- 任一工具输出结构需要特判，禁止进入下一任务

### TASK-B3：移除硬编码示例参数

测试环境：

- ENV-1
- ENV-3

执行步骤：

1. 改造 `tools/cpu.py`，使其支持传入 `groupname`、`begin_time`、`end_time`
2. 检查其他工具是否仍存在示例参数依赖
3. 保留 `__main__` 示例，但业务调用入口不得再依赖示例值

验证方式：

- 本地 mock 测试通过
- 在联调环境选 1 个真实集群验证

建议验证命令：

```bash
pytest tests/unit/test_tools_entrypoints.py -q
```

联调验证：

- 选 1 个真实集群
- 选 1 个固定时间窗
- 手工执行工具函数
- 确认输出结构正确

通过标准：

- 4 个工具全部支持参数化调用
- 无硬编码集群名
- 无硬编码时间窗

失败处理：

- 未参数化完成前，不允许进入阶段 C

---

## 阶段 C：Collector 标准层

目标：把四个工具封装成标准采集器，形成插件化扩展能力。

### TASK-C1：建立领域模型

测试环境：

- ENV-1

执行步骤：

1. 新增 `src/domain/models.py`
2. 定义：
   - `MetricPoint`
   - `CollectorResult`
   - `CollectorError`
   - `TimeWindow`

验证方式：

- 编写模型构造测试

建议验证命令：

```bash
pytest tests/unit/test_domain_models.py -q
```

通过标准：

- 模型可直接用于 collector、orchestrator、storage 三层

### TASK-C2：建立 Collector 基类与注册机制

测试环境：

- ENV-1

执行步骤：

1. 新增 `src/collectors/base.py`
2. 定义统一 Collector 接口
3. 增加注册表 `collector_registry`

验证方式：

- 用 fake collector 做注册和读取测试

建议验证命令：

```bash
pytest tests/unit/test_collector_registry.py -q
```

通过标准：

- 新增 collector 不需要改调度核心逻辑

### TASK-C3：实现四个基础 Collector

测试环境：

- ENV-1
- ENV-3

执行步骤：

1. 新增 `CpuCollector`
2. 新增 `QpsCollector`
3. 新增 `HttpCodeCollector`
4. 新增 `TpCollector`

要求：

- 每个 collector 只负责一个工具的适配
- 工具异常时返回失败结果，不抛出未捕获异常
- 每个 collector 输出 `list[MetricPoint]`

验证方式：

- ENV-1 下使用 fixture 和 mock 测试
- ENV-3 下使用 1 个真实集群联调

建议验证命令：

```bash
pytest tests/unit/test_collectors -q
```

通过标准：

- 每个 collector 能独立运行
- 同一 collector 的成功和失败路径都已覆盖

失败处理：

- 任何 collector 未稳定前，不允许进入阶段 D

---

## 阶段 D：时间窗与集群配置

目标：把“采什么集群、采哪个 5 分钟桶”固定成标准逻辑。

### TASK-D1：实现 5 分钟时间窗对齐逻辑

测试环境：

- ENV-1

执行步骤：

1. 规定统一采集规则：
   - 默认采“刚刚结束的一个 5 分钟桶”
2. 实现时间窗对齐函数
3. 实现 `bucket_time` 生成规则

必须固定的规则：

- `10:00:00` 触发时采集 `09:55:00-10:00:00`
- 不能在不同模块中出现不同定义

验证方式：

- 使用固定时间点做参数化测试

建议验证命令：

```bash
pytest tests/unit/test_bucket_alignment.py -q
```

通过标准：

- 所有边界时间点测试通过

### TASK-D2：实现集群清单加载器

测试环境：

- ENV-1

执行步骤：

1. 新增 `src/config/cluster_loader.py`
2. 读取 `cluster.json`
3. 输出扁平化集群列表
4. 保留分组信息

验证方式：

- 校验当前集群总数为 `70`
- 校验每个 group 的数量

建议验证命令：

```bash
pytest tests/unit/test_cluster_loader.py -q
```

通过标准：

- 当前配置能稳定解析
- 扁平列表和分组映射都可用

失败处理：

- 集群清单解析不稳定，不允许进入阶段 E

---

## 阶段 E：并发调度器

目标：实现“所有集群 x 所有 collector”的受控执行。

### TASK-E1：实现单时间窗调度器

测试环境：

- ENV-1

执行步骤：

1. 新增 `src/orchestrator/scheduler.py`
2. 输入一个 `TimeWindow`
3. 输出该时间窗的所有采集任务

验证方式：

- 70 个集群、4 个 collector 时，应生成 `280` 个基础采集任务

建议验证命令：

```bash
pytest tests/unit/test_scheduler.py -q
```

通过标准：

- 任务总数正确
- 不遗漏、不重复

### TASK-E2：实现并发 Dispatcher

测试环境：

- ENV-1

执行步骤：

1. 新增 `src/orchestrator/dispatcher.py`
2. 使用 `asyncio` 控制并发
3. 增加全局并发限制
4. 增加单任务超时
5. 增加可配置重试

验证方式：

- 使用 fake collector 构造如下场景：
   - 慢任务
   - 超时任务
   - 失败任务
   - 部分成功任务

建议验证命令：

```bash
pytest tests/unit/test_dispatcher.py -q
```

通过标准：

- 某任务失败不阻塞其他任务
- 并发限制确实生效
- 超时和重试行为符合配置

### TASK-E3：实现任务执行结果汇总

测试环境：

- ENV-1

执行步骤：

1. 输出成功条数
2. 输出失败条数
3. 输出失败详情
4. 输出 collector 维度统计

验证方式：

- 用混合成功失败场景校验汇总结果

建议验证命令：

```bash
pytest tests/unit/test_dispatcher_summary.py -q
```

通过标准：

- 执行结果可用于后续入库和补数

失败处理：

- 无法统计失败详情时，不允许进入阶段 F

---

## 阶段 F：TimescaleDB 入库层

目标：把标准化结果写入数据库，并保证幂等。

### TASK-F1：设计并落地数据库 Schema

测试环境：

- ENV-2

执行步骤：

1. 新增 `sql/001_init.sql`
2. 创建 `metric_points`
3. 创建 `collection_runs`
4. 创建唯一约束和必要索引
5. 初始化 hypertable

验证方式：

- 启动 TimescaleDB
- 执行 schema 脚本
- 查询表结构

建议验证命令：

```bash
docker compose -f docker-compose.test.yml up -d
psql "$TEST_DB_URL" -f sql/001_init.sql
pytest tests/integration/test_schema.py -q
```

通过标准：

- schema 执行成功
- hypertable 创建成功
- 约束和索引存在

### TASK-F2：实现指标批量写入

测试环境：

- ENV-2

执行步骤：

1. 新增 `src/storage/timescale_repo.py`
2. 实现批量插入接口
3. 实现 upsert
4. 实现运行状态写入

验证方式：

- 插入一批 `MetricPoint`
- 重复插入同一批数据
- 检查无重复

建议验证命令：

```bash
pytest tests/integration/test_metric_repository.py -q
```

通过标准：

- 幂等写入成立
- 批量写入成功
- 运行状态可追踪

### TASK-F3：实现数据库聚合层

测试环境：

- ENV-2

执行步骤：

1. 新增连续聚合或汇总视图 SQL
2. 至少生成：
   - `5m`
   - `1h`
   - `1d`

验证方式：

- 插入样本数据
- 验证聚合结果是否符合预期

建议验证命令：

```bash
pytest tests/integration/test_continuous_aggregates.py -q
```

通过标准：

- 聚合结果正确
- 聚合层可用于后续基线查询

失败处理：

- 聚合层未验证成功前，不允许进入阶段 G

---

## 阶段 G：单时间窗端到端采集

目标：先验证一次完整链路，再进入定时化。

### TASK-G1：实现手工触发入口

测试环境：

- ENV-2

执行步骤：

1. 新增命令行入口
2. 支持输入：
   - 指定单个集群
   - 指定时间窗
   - 指定启用哪些 collector

建议验证命令：

```bash
python -m src.main collect --cluster lf-lan-ha1 --start "2026-03-11 10:00:00" --end "2026-03-11 10:05:00"
```

通过标准：

- 能从命令行触发一次完整采集任务

### TASK-G2：使用 mock collector 完成端到端测试

测试环境：

- ENV-2

执行步骤：

1. 用 fake collector 跑完整链路
2. 写入测试数据库
3. 查询数据库确认数据存在

建议验证命令：

```bash
pytest tests/e2e/test_collect_one_window_mock.py -q
```

通过标准：

- 调度、collector、入库链路全部打通

### TASK-G3：使用真实 API 完成小流量联调

测试环境：

- ENV-3

执行步骤：

1. 先 1 个集群
2. 再 5 个集群
3. 每个集群执行 1 个固定时间窗
4. 写入测试数据库或影子 schema

验证方式：

- 检查成功率
- 检查写入条数
- 检查失败日志

通过标准：

- 至少 1 轮真实采集成功写入
- 写入数据与工具输出一致

失败处理：

- 真实联调不稳定时，不允许进入阶段 H

---

## 阶段 H：定时任务与补数

目标：把一次性采集变成持续稳定的采集服务。

### TASK-H1：实现每 5 分钟定时调度

测试环境：

- ENV-4

执行步骤：

1. 实现 cron 或常驻调度器
2. 每次运行生成“刚结束的 5 分钟桶”
3. 执行所有集群采集

验证方式：

- 连续运行 1 小时
- 检查应产生 12 个时间桶

通过标准：

- 不漏桶
- 不重复执行

### TASK-H2：实现补数机制

测试环境：

- ENV-4

执行步骤：

1. 支持输入起止时间做补数
2. 自动拆分成多个 5 分钟时间窗
3. 对已存在数据使用幂等写入

验证方式：

- 对过去 1 小时补数
- 再次重复执行同一补数
- 确认无重复数据

建议验证命令：

```bash
python -m src.main backfill --start "2026-03-11 09:00:00" --end "2026-03-11 10:00:00"
```

通过标准：

- 补数可重复执行
- 数据不重复

### TASK-H3：实现失败重试和失败时间窗追踪

测试环境：

- ENV-4

执行步骤：

1. 对失败任务持久化
2. 支持重试指定失败时间窗
3. 支持查询失败原因

验证方式：

- 人为制造部分失败
- 重试失败任务
- 确认重试后数据可落库

通过标准：

- 失败记录可查询
- 重试路径可执行

失败处理：

- 无失败追踪能力，不允许进入阶段 I

---

## 阶段 I：基线查询能力

目标：支持用户查询历史基线。

### TASK-I1：固定基线定义

测试环境：

- ENV-1

执行步骤：

1. 明确定义基线算法
2. 至少确定以下参数：
   - 统计周期
   - 聚合函数
   - 对比方式

建议第一版固定：

- 历史 7 天同时间窗
- `avg + p50 + p95`

验证方式：

- 基线规则使用固定样本数据单元测试

建议验证命令：

```bash
pytest tests/unit/test_baseline_rules.py -q
```

通过标准：

- 规则定义无歧义

### TASK-I2：实现基线查询 SQL 和服务层

测试环境：

- ENV-2

执行步骤：

1. 新增 `src/services/baseline_service.py`
2. 针对 CPU 先实现：
   - 时间段基线
   - 上周同时间段基线
3. 优先使用聚合层查询

验证方式：

- 构造测试数据
- 手工 SQL 对比服务输出

建议验证命令：

```bash
pytest tests/integration/test_baseline_service.py -q
```

通过标准：

- 服务结果与手工 SQL 一致

### TASK-I3：暴露查询接口

测试环境：

- ENV-2
- ENV-3

执行步骤：

1. 新增查询 API 或 CLI
2. 支持请求：
   - 某集群 `10:00-13:00` 的 CPU 基线
   - 上周同时间段 CPU 基线

验证方式：

- 本地接口测试
- 联调环境校验真实数据结果

建议验证命令：

```bash
pytest tests/e2e/test_baseline_api.py -q
```

通过标准：

- 用户查询可在规定时间内返回结果

---

## 5. 每阶段强制检查清单

每完成一个任务，必须执行以下检查：

1. 对应测试已运行
2. 测试输出已保存
3. 日志已检查
4. 未处理失败项为 0
5. 文档已同步更新

若任一项不满足，不允许合并代码，也不允许开始下一任务。

## 6. 第一轮建议执行顺序

第一轮只做以下任务，不扩展范围：

1. `TASK-A1`
2. `TASK-A2`
3. `TASK-A3`
4. `TASK-B1`
5. `TASK-B2`
6. `TASK-B3`
7. `TASK-C1`
8. `TASK-C2`
9. `TASK-C3`

原因：

- 这 9 个任务能先把“测试底座 + 工具契约 + collector 插件层”打稳
- 只有这些基础稳定，后面的调度、数据库和基线才不会反复返工

## 7. 本轮执行终点

本轮开发到以下状态即可视为完成第一里程碑：

- 四个现有工具已经标准化
- collector 层已经完成
- 所有相关单元测试通过
- 至少完成一次 1 集群的真实联调

达到这个里程碑后，再启动数据库和调度相关任务。
