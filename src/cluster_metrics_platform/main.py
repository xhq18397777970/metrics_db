"""CLI entry points for manual collection and backfill commands."""

from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

from cluster_metrics_platform.api.routes.baselines import parse_datetime
from cluster_metrics_platform.domain.time_window import get_closed_window


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

    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    collection_service=None,
    backfill_service=None,
) -> int:
    """Run the requested CLI command."""

    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command == "collect-window":
        if collection_service is None:
            raise RuntimeError("collection_service is required for collect-window")
        payload = asyncio.run(
            _run_collect_window(
                collection_service,
                window_end=parse_datetime(args.window_end),
                cluster_names=args.cluster,
            )
        )
    elif args.command == "backfill":
        if backfill_service is None:
            raise RuntimeError("backfill_service is required for backfill")
        payload = asyncio.run(
            _run_backfill(
                backfill_service,
                start_time=parse_datetime(args.start),
                end_time=parse_datetime(args.end),
                cluster_names=args.cluster,
            )
        )
    else:  # pragma: no cover - argparse keeps this unreachable
        raise RuntimeError(f"unsupported command: {args.command}")

    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=_json_default))
    return 0


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
