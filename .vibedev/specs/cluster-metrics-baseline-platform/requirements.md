# Requirements Document

## Introduction

本功能用于构建一个面向运维人员的集群指标平台。系统需要基于现有并持续扩展的 `tools` API，按 5 分钟粒度持续采集所有集群的指标数据，并将结果存入 TimescaleDB。第一版范围仅包含采集、入库、补数、失败重试和基线查询接口，不包含前端页面和告警能力。测试环境限定为 macOS，并通过 Homebrew 安装 TimescaleDB 与相关依赖。

## Requirements

### 1. Cluster Metric Collection

**User Story:** As an operations engineer, I want the system to collect metrics for every managed cluster every 5 minutes, so that I can build a complete historical metrics repository.

#### Acceptance Criteria

1. When a 5-minute collection window closes, the system shall generate one collection job for each configured cluster.
2. When a collection job runs for a cluster, the system shall invoke all enabled metric tools for that cluster and the same time window.
3. While processing a collection window, the system shall use a single canonical time-window definition for all tools and downstream storage.
4. If a metric tool succeeds, the system shall persist the returned metric values for the target cluster and collection window.
5. If a metric tool returns no data, the system shall record the collection result as a handled failure without terminating the whole collection job.
6. When the system performs regular incremental collection, the system shall use the most recently closed 5-minute window as the API input range.
7. While running regular incremental collection, the system shall align each collection window to canonical 5-minute boundaries instead of deriving the next window from task completion time.

### 2. Tool Extensibility

**User Story:** As a platform maintainer, I want new tools APIs to be added without rewriting the scheduler or storage model, so that the platform can evolve as more metrics become available.

#### Acceptance Criteria

1. Where a new metrics API is introduced, the system shall support integrating it through a collector extension point instead of modifying core scheduling logic.
2. When a collector is registered, the system shall execute it using the same cluster and time-window contract as existing collectors.
3. If a collector returns one metric or multiple metrics, the system shall normalize both cases into the same internal metric-point model.
4. Where a collector requires tool-specific request formatting, the system shall isolate that formatting inside the collector adapter.

### 3. Metric Normalization and Storage

**User Story:** As an operations engineer, I want all metrics to be stored in a consistent time-series schema, so that I can query and compare metrics efficiently across clusters and time ranges.

#### Acceptance Criteria

1. When metric data is written, the system shall store each metric point with cluster name, bucket time, source time window, metric name, metric value, and any applicable labels.
2. Where a metric contains dimensions such as HTTP code class or network direction, the system shall store those dimensions as labels without requiring a schema change.
3. When the same cluster, metric, labels, and bucket time are written more than once, the system shall perform idempotent upsert behavior.
4. While storing data, the system shall use TimescaleDB as the primary database.
5. Where local testing is performed, the system shall support TimescaleDB running on macOS installed through Homebrew.

### 4. Collection Reliability

**User Story:** As an operations engineer, I want collection failures to be traceable and recoverable, so that temporary API issues do not create permanent gaps in the metrics repository.

#### Acceptance Criteria

1. If a collector request fails, the system shall record the failure status, error message, cluster, collector name, and time window.
2. If one collector fails for a cluster, the system shall continue running the remaining collectors for that cluster whenever possible.
3. If one cluster job fails, the system shall continue running jobs for other clusters.
4. When a retry policy is configured, the system shall retry failed collector executions within the configured limits.
5. Where a collection window remains incomplete after retries, the system shall preserve enough execution metadata to support later backfill.

### 5. Backfill Capability

**User Story:** As an operations engineer, I want to backfill historical time windows, so that I can repair missing data caused by outages or newly added collectors.

#### Acceptance Criteria

1. When an operator specifies a historical start time and end time, the system shall split the range into canonical 5-minute windows.
2. When a backfill job runs, the system shall execute the same collection pipeline used for regular scheduled collection.
3. If a historical window already has stored metric points, the system shall avoid duplicate records through idempotent writes.
4. Where a newly added collector is enabled, the system shall allow historical backfill for that collector without requiring re-ingestion of unrelated metrics.
5. When the system backfills a time range, the system shall finish collecting the current 5-minute window for all target clusters before advancing the cursor to the next 5-minute window.
6. While backfilling a continuous time range, the system shall advance the collection cursor by exactly 5 minutes per step until the requested end time is reached.

### 6. Baseline Query API

**User Story:** As an operations engineer, I want to query a cluster's metric baseline for a target time period, so that I can quickly compare current behavior with historical norms.

#### Acceptance Criteria

1. When an operator requests a CPU baseline for a specific time range, the system shall return a computed baseline result using historical stored metrics.
2. When an operator requests a "last week same time range" baseline, the system shall return a baseline derived from the corresponding historical comparison period.
3. Where baseline rules are defined, the system shall compute results using explicit parameters such as lookback range and aggregation method.
4. If no qualifying historical data exists, the system shall return a clear no-data response instead of a misleading baseline.
5. While responding to baseline queries, the system shall use database-side aggregation suitable for fast retrieval rather than scanning raw data in application memory.

### 7. Operational Interfaces

**User Story:** As an operations engineer, I want explicit interfaces for collection and querying, so that I can trigger ingestion, backfill, and baseline retrieval in a controlled way.

#### Acceptance Criteria

1. When the system starts in scheduled mode, the system shall automatically collect the most recently closed 5-minute window.
2. When an operator invokes a manual collection or backfill command, the system shall accept cluster scope and time-range parameters.
3. When an operator invokes a baseline query interface, the system shall accept cluster name, metric name, and query time range.
4. Where version one scope is enforced, the system shall expose ingestion and baseline query interfaces without requiring a frontend UI.
5. When an operator invokes a manual historical collection command, the system shall support sequential forward collection in 5-minute steps across the requested time range.

### 8. Testability and Local Validation

**User Story:** As a platform maintainer, I want every implementation step to be testable on macOS, so that each new capability is verified before the next stage starts.

#### Acceptance Criteria

1. When a new collector or storage feature is added, the system shall include automated tests for success and failure paths before the next implementation stage proceeds.
2. Where logic can be validated without real APIs, the system shall provide unit tests using mocks or fixtures.
3. Where database behavior must be validated, the system shall provide integration tests against a local TimescaleDB instance installed via Homebrew on macOS.
4. If a stage's required tests do not pass, the system shall block progression to the next stage of implementation.
