"""CLI entry points for manual collection and backfill commands."""

from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Any
from wsgiref.simple_server import make_server

from cluster_metrics_platform.api.routes.baselines import parse_datetime
from cluster_metrics_platform.bootstrap import create_application
from cluster_metrics_platform.domain.time_window import get_closed_window
from cluster_metrics_platform.orchestrator.scheduler import ScheduledCollector, default_now_provider


def build_parser() -> argparse.ArgumentParser:
    """Build the project CLI parser."""

    parser = argparse.ArgumentParser(prog="cluster-metrics-platform")
    subparsers = parser.add_subparsers(dest="command", required=True)

    collect_parser = subparsers.add_parser("collect-window")
    collect_parser.add_argument("--window-end", required=True)
    collect_parser.add_argument("--cluster", action="append", default=[])

    backfill_parser = subparsers.add_parser("backfill")
    backfill_parser.add_argument("--start", required=True)
    backfill_parser.add_argument("--end", required=True)
    backfill_parser.add_argument("--cluster", action="append", default=[])

    schedule_parser = subparsers.add_parser("run-scheduler")
    schedule_parser.add_argument("--cluster", action="append", default=[])
    schedule_parser.add_argument("--iterations", type=int, default=None)
    schedule_parser.add_argument("--step-minutes", type=int, default=5)

    serve_parser = subparsers.add_parser("serve-api")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8000)

    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    collection_service=None,
    backfill_service=None,
    collection_status_service=None,
    api_app=None,
    application_factory=create_application,
    server_factory=make_server,
    scheduler_now_provider=default_now_provider,
    scheduler_sleep=asyncio.sleep,
) -> int:
    """Run the requested CLI command."""

    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    application_context = None

    try:
        if args.command == "collect-window":
            if collection_service is None:
                application_context = application_factory()
                collection_service = application_context.collection_service
            payload = asyncio.run(
                _run_collect_window(
                    collection_service,
                    window_end=parse_datetime(args.window_end),
                    cluster_names=args.cluster,
                )
            )
        elif args.command == "backfill":
            if backfill_service is None:
                application_context = application_factory()
                backfill_service = application_context.backfill_service
            payload = asyncio.run(
                _run_backfill(
                    backfill_service,
                    start_time=parse_datetime(args.start),
                    end_time=parse_datetime(args.end),
                    cluster_names=args.cluster,
                )
            )
        elif args.command == "serve-api":
            if api_app is None:
                application_context = application_factory()
                api_app = application_context.api_app
            return _serve_api(
                api_app,
                host=args.host,
                port=args.port,
                server_factory=server_factory,
            )
        elif args.command == "run-scheduler":
            if collection_service is None:
                application_context = application_factory()
                collection_service = application_context.collection_service
                collection_status_service = application_context.collection_status_service
            return asyncio.run(
                _run_scheduler(
                    collection_service,
                    cluster_names=args.cluster,
                    step_minutes=args.step_minutes,
                    iterations=args.iterations,
                    collection_status_service=collection_status_service,
                    now_provider=scheduler_now_provider,
                    sleep_fn=scheduler_sleep,
                )
            )
        else:  # pragma: no cover - argparse keeps this unreachable
            raise RuntimeError(f"unsupported command: {args.command}")

        print(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=_json_default))
        return 0
    finally:
        if application_context is not None and args.command != "serve-api":
            application_context.close()


async def _run_collect_window(
    collection_service,
    *,
    window_end: datetime,
    cluster_names,
) -> dict[str, Any]:
    window = get_closed_window(window_end)
    execution = await collection_service.collect_window(window, cluster_names or None)
    return {
        "command": "collect-window",
        "window": _serialize_window(execution.window),
        "loaded_cluster_count": execution.loaded_cluster_count,
        "selected_cluster_count": execution.selected_cluster_count,
        "points_written": execution.points_written,
        "runs_written": execution.runs_written,
    }


async def _run_backfill(
    backfill_service,
    *,
    start_time: datetime,
    end_time: datetime,
    cluster_names,
) -> dict[str, Any]:
    execution = await backfill_service.backfill(start_time, end_time, cluster_names or None)
    return {
        "command": "backfill",
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "total_windows": execution.total_windows,
        "total_points_written": execution.total_points_written,
        "total_runs_written": execution.total_runs_written,
    }


def _serve_api(api_app, *, host: str, port: int, server_factory=make_server) -> int:
    server = server_factory(host, port, api_app)
    print(f"Serving dashboard and API on http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:  # pragma: no cover - manual stop path
        pass
    finally:
        server.server_close()
    return 0


async def _run_scheduler(
    collection_service,
    *,
    cluster_names,
    step_minutes: int,
    iterations: int | None,
    collection_status_service=None,
    now_provider=default_now_provider,
    sleep_fn=asyncio.sleep,
) -> int:
    if step_minutes <= 0:
        raise ValueError("step_minutes must be positive")
    if iterations is not None and iterations <= 0:
        raise ValueError("iterations must be positive when provided")

    scheduler = ScheduledCollector(
        collect_window=collection_service.collect_window,
        step_minutes=step_minutes,
        now_provider=now_provider,
    )
    cluster_scope = cluster_names or None
    executed_iterations = 0
    last_bucket_time = None

    if collection_status_service is not None:
        collection_status_service.mark_scheduler_idle(step_minutes=step_minutes)

    try:
        while iterations is None or executed_iterations < iterations:
            execution = await scheduler.collect_once(cluster_scope)
            if getattr(execution, "window", None) is not None:
                window = execution.window
                if last_bucket_time != window.bucket_time:
                    payload = {
                        "command": "run-scheduler",
                        "window": _serialize_window(window),
                        "selected_cluster_count": execution.selected_cluster_count,
                        "points_written": execution.points_written,
                        "runs_written": execution.runs_written,
                    }
                    print(
                        json.dumps(
                            payload,
                            ensure_ascii=False,
                            sort_keys=True,
                            default=_json_default,
                        )
                    )
                    last_bucket_time = window.bucket_time
                    executed_iterations += 1
                    if collection_status_service is not None:
                        collection_status_service.mark_scheduler_idle(
                            step_minutes=step_minutes,
                            last_finished_at=now_provider(),
                        )
            if iterations is not None and executed_iterations >= iterations:
                break

            await sleep_fn(_seconds_until_next_window(now_provider(), step_minutes))
            if collection_status_service is not None:
                collection_status_service.mark_scheduler_idle(step_minutes=step_minutes)
    finally:
        if collection_status_service is not None:
            collection_status_service.mark_scheduler_stopped(step_minutes=step_minutes)

    return 0


def _seconds_until_next_window(now: datetime, step_minutes: int) -> float:
    bucket_end = get_closed_window(now + timedelta(minutes=step_minutes), step_minutes).end_time
    return max((bucket_end - now).total_seconds(), 1.0)


def _serialize_window(window) -> dict[str, Any]:
    return {
        "bucket_time": window.bucket_time.isoformat(),
        "start_time": window.start_time.isoformat(),
        "end_time": window.end_time.isoformat(),
        "window_seconds": window.window_seconds,
    }


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc).isoformat()
        return value.isoformat()
    raise TypeError(f"Object of type {type(value)!r} is not JSON serializable")


if __name__ == "__main__":  # pragma: no cover - CLI module entry point
    raise SystemExit(main())
