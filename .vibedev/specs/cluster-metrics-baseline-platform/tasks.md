# Implementation Plan

1. [ ] Bootstrap the Python project structure and automated test harness for the metrics platform.
   - Create the package skeleton under `src/` plus `tests/` so later tasks have stable module boundaries.
   - Add dependency and test configuration files needed for code execution in the existing `conda` environment, including `pytest`, async test support, and lint configuration.
   - Add smoke tests that prove the package imports and the test runner works before any feature code is added.
   - Requirements: `Req 8.1`, `Req 8.2`, `Req 8.4`

2. [ ] Implement the core domain models and time-window utilities with unit tests.
   - Add typed models for `TimeWindow`, `MetricPoint`, `CollectorError`, `CollectorResult`, `CollectionRun`, `BaselineQuery`, and `BaselineResponse`.
   - Implement canonical 5-minute bucket alignment logic and tests for boundary timestamps.
   - Implement forward 5-minute window iteration for historical backfill and tests that prove the cursor advances by exact bucket boundaries.
   - Implement stable label fingerprint generation and tests that prove equivalent label dictionaries produce the same fingerprint regardless of key order.
   - Add cluster configuration loader code for `cluster.json` and tests that validate the current grouped and flattened cluster outputs.
   - Files/components: `src/domain/models.py`, `src/domain/time_window.py`, `src/config/cluster_loader.py`, `tests/unit/test_time_window.py`, `tests/unit/test_cluster_loader.py`
   - Requirements: `Req 1.1`, `Req 1.2`, `Req 1.3`, `Req 1.6`, `Req 1.7`, `Req 5.5`, `Req 5.6`, `Req 7.1`, `Req 8.1`, `Req 8.2`

3. [ ] Refactor the existing tool integrations into parameterized collector adapters and cover them with fixture-based tests.
   - Remove hard-coded example inputs from the current `tools` entry paths and expose functions that accept `cluster_name`, `window_start`, and `window_end`.
   - Implement a collector interface and one adapter each for CPU, QPS, HTTP code, and TP that normalize tool output into `MetricPoint` objects.
   - Add fixture-driven tests for success, empty-data, malformed-response, and handled-error paths for each collector.
   - Files/components: `tools/*.py`, `src/collectors/base.py`, `src/collectors/cpu_collector.py`, `src/collectors/qps_collector.py`, `src/collectors/http_code_collector.py`, `src/collectors/tp_collector.py`, `tests/unit/test_collectors_*`
   - Requirements: `Req 1.2`, `Req 1.4`, `Req 1.5`, `Req 2.1`, `Req 2.2`, `Req 2.3`, `Req 2.4`, `Req 3.1`, `Req 3.2`, `Req 4.1`, `Req 8.1`, `Req 8.2`

4. [ ] Implement the collector registry and concurrent dispatcher with retry-aware execution tests.
   - Add a registry for enabled collectors so the dispatcher can discover collectors without hard-coded branching.
   - Implement a dispatcher that expands `clusters x collectors` for one `TimeWindow`, enforces bounded concurrency, captures partial failures, and returns a summary model.
   - Add unit tests using fake collectors to verify timeout handling, retry limits, partial success, and continued execution when one collector or cluster fails.
   - Files/components: `src/collectors/registry.py`, `src/orchestrator/dispatcher.py`, `src/orchestrator/models.py`, `tests/unit/test_collector_registry.py`, `tests/unit/test_dispatcher.py`
   - Requirements: `Req 1.1`, `Req 1.2`, `Req 1.5`, `Req 2.1`, `Req 4.1`, `Req 4.2`, `Req 4.3`, `Req 4.4`, `Req 4.5`, `Req 8.1`, `Req 8.2`

5. [ ] Add the TimescaleDB schema, repository layer, and integration tests for idempotent writes.
   - Create SQL migrations for `metric_points` and `collection_runs`, including the hypertable conversion and indexes needed by the design.
   - Implement repository code that batch-upserts metric points and persists collection run records.
   - Add integration tests against a local Homebrew-installed TimescaleDB test database to verify schema creation, idempotent upsert behavior, stable label fingerprint behavior, and run-record persistence.
   - Files/components: `sql/001_init.sql`, `src/storage/timescale_repo.py`, `src/storage/db.py`, `tests/integration/test_schema.py`, `tests/integration/test_metric_repository.py`
   - Requirements: `Req 3.1`, `Req 3.2`, `Req 3.3`, `Req 3.4`, `Req 3.5`, `Req 4.1`, `Req 4.5`, `Req 5.3`, `Req 8.1`, `Req 8.3`

6. [ ] Implement the scheduled collection and backfill application services with automated tests.
   - Add application services that compute the closed 5-minute window, load clusters, call the dispatcher, and write both metric points and run records.
   - Add a backfill service that splits a historical range into canonical 5-minute windows, advances the cursor by exactly 5 minutes after each completed window, and reuses the same collection pipeline.
   - Add automated tests that verify scheduled collection always uses the most recently closed bucket instead of completion-time drift.
   - Add automated tests that verify backfill processes one 5-minute window across all target clusters before advancing to the next window, without duplicating stored points.
   - Files/components: `src/services/collection_service.py`, `src/services/backfill_service.py`, `src/orchestrator/scheduler.py`, `tests/unit/test_collection_service.py`, `tests/unit/test_backfill_service.py`, `tests/integration/test_backfill_idempotency.py`
   - Requirements: `Req 1.1`, `Req 1.2`, `Req 1.6`, `Req 1.7`, `Req 4.5`, `Req 5.1`, `Req 5.2`, `Req 5.3`, `Req 5.4`, `Req 5.5`, `Req 5.6`, `Req 7.1`, `Req 7.2`, `Req 7.5`, `Req 8.1`, `Req 8.3`

7. [ ] Implement database rollups and the baseline query service with integration tests that compare SQL results to expected outputs.
   - Add the hourly and daily continuous aggregate SQL objects described in the design and code to initialize or refresh them.
   - Add SQL or repository bootstrap code for continuous aggregate refresh policies and separate retention policies for raw and rollup data.
   - Implement `BaselineService` for `historical_range` and `last_week_same_range` modes, using database-side aggregation instead of application-side scans.
   - Add integration tests with seeded metric data that validate `avg`, `p50`, and `p95` outputs, explicit `no_data` responses, and the existence of configured refresh/retention policies.
   - Files/components: `sql/002_rollups.sql`, `src/services/baseline_service.py`, `src/storage/baseline_queries.py`, `tests/integration/test_continuous_aggregates.py`, `tests/integration/test_baseline_service.py`
   - Requirements: `Req 6.1`, `Req 6.2`, `Req 6.3`, `Req 6.4`, `Req 6.5`, `Req 8.1`, `Req 8.3`

8. [ ] Expose CLI entry points for collection and backfill plus an HTTP API for baseline queries, then cover them with end-to-end tests.
   - Add CLI commands for `collect-window` and `backfill` that wire into the application services created in earlier tasks.
   - Ensure the `backfill` CLI supports sequential forward collection over a requested time range using canonical 5-minute windows.
   - Add an HTTP API endpoint for baseline queries that validates request parameters and returns the normalized `BaselineResponse`.
   - Add end-to-end tests that exercise the CLI and API against test doubles or seeded database fixtures without requiring manual execution.
   - Files/components: `src/main.py`, `src/api/app.py`, `src/api/routes/baselines.py`, `tests/e2e/test_collect_cli.py`, `tests/e2e/test_backfill_cli.py`, `tests/e2e/test_baseline_api.py`
   - Requirements: `Req 6.1`, `Req 6.2`, `Req 6.4`, `Req 7.1`, `Req 7.2`, `Req 7.3`, `Req 7.4`, `Req 7.5`, `Req 8.1`, `Req 8.2`, `Req 8.3`

9. [ ] Wire the default collector set and add a full automated pipeline test that proves the v1 feature is integrated end to end.
   - Register the four built-in collectors as the default enabled set and connect configuration, dispatcher, repository, rollups, and API modules through a single application bootstrap path.
   - Add an automated end-to-end test that seeds cluster configuration, runs a single collection window through the full pipeline, and verifies the resulting baseline query over stored data.
   - Keep the final wiring code minimal and remove any temporary scaffolding that is not exercised by the automated test suite.
   - Files/components: `src/bootstrap.py`, `src/settings.py`, `tests/e2e/test_full_pipeline.py`
   - Requirements: `Req 1.1`, `Req 1.2`, `Req 2.1`, `Req 3.4`, `Req 4.1`, `Req 5.2`, `Req 6.1`, `Req 7.1`, `Req 7.3`, `Req 8.1`, `Req 8.4`
