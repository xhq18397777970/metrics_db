"""CLI entry point for the standalone baseline query API service."""

from __future__ import annotations

import argparse
import os
from collections.abc import Sequence
from wsgiref.simple_server import make_server

from api.app import create_app
from api.baseline_query_service import BaselineQueryService
from api.metric_statistics_service import MetricStatisticsService
from cluster_metrics_platform.storage.db import DatabaseConfig, connect_db

EXTERNAL_API_DATABASE_ENV = "BASELINE_QUERY_DATABASE_URL"
FALLBACK_DATABASE_ENV = "CLUSTER_METRICS_DATABASE_URL"


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser for the standalone service."""

    parser = argparse.ArgumentParser(prog="baseline-query-api")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve_parser = subparsers.add_parser("serve")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8003)

    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    connection=None,
    query_service=None,
    statistics_service=None,
    api_app=None,
    server_factory=make_server,
) -> int:
    """Run the standalone baseline query API server."""

    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    owns_connection = False

    try:
        if args.command != "serve":  # pragma: no cover - argparse keeps unreachable
            raise RuntimeError(f"unsupported command: {args.command}")

        if api_app is None:
            if query_service is None:
                if connection is None:
                    connection = connect_db(_database_config(), autocommit=True)
                    owns_connection = True
                query_service = BaselineQueryService(connection)
            if statistics_service is None:
                statistics_service = MetricStatisticsService(connection)
            api_app = create_app(query_service, statistics_service)

        server = server_factory(args.host, args.port, api_app)
        print(f"Serving standalone baseline query API on http://{args.host}:{args.port}")
        try:
            server.serve_forever()
        except KeyboardInterrupt:  # pragma: no cover - manual stop path
            pass
        finally:
            server.server_close()
        return 0
    finally:
        if owns_connection and connection is not None:
            connection.close()


def _database_config() -> DatabaseConfig:
    default_dsn = os.getenv(FALLBACK_DATABASE_ENV)
    return DatabaseConfig.from_env(
        EXTERNAL_API_DATABASE_ENV,
        default_dsn=default_dsn,
        application_name="cluster-metrics-standalone-baseline-api",
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
