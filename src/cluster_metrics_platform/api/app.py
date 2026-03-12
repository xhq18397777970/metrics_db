"""Minimal WSGI application for baseline query APIs."""

from __future__ import annotations

import json
from collections.abc import Callable
from http import HTTPStatus
from typing import Any
from urllib.parse import parse_qs

from cluster_metrics_platform.api.dashboard import render_dashboard
from cluster_metrics_platform.api.routes.baselines import (
    build_baseline_query,
    serialize_baseline_response,
)

StartResponse = Callable[[str, list[tuple[str, str]]], object]


def create_app(baseline_service, metrics_table_service=None):
    """Create a tiny WSGI app exposing dashboard and API endpoints."""

    def app(environ: dict[str, Any], start_response: StartResponse):
        method = environ.get("REQUEST_METHOD", "GET")
        path = environ.get("PATH_INFO", "")

        if path == "/":
            if method != "GET":
                return _json_response(
                    start_response,
                    HTTPStatus.METHOD_NOT_ALLOWED,
                    {"error": "method not allowed"},
                )
            return _html_response(start_response, HTTPStatus.OK, render_dashboard())

        if path == "/api/v1/metrics/recent":
            if method != "GET":
                return _json_response(
                    start_response,
                    HTTPStatus.METHOD_NOT_ALLOWED,
                    {"error": "method not allowed"},
                )
            if metrics_table_service is None:
                return _json_response(
                    start_response,
                    HTTPStatus.NOT_FOUND,
                    {"error": "not found"},
                )
            try:
                page, page_size = _parse_recent_metrics_query(environ)
                payload = metrics_table_service.list_recent_points(
                    page=page,
                    page_size=page_size,
                )
                return _json_response(start_response, HTTPStatus.OK, payload)
            except ValueError as exc:
                return _json_response(
                    start_response,
                    HTTPStatus.BAD_REQUEST,
                    {"error": str(exc)},
                )

        if path != "/api/v1/baselines/query":
            return _json_response(
                start_response,
                HTTPStatus.NOT_FOUND,
                {"error": "not found"},
            )
        if method != "POST":
            return _json_response(
                start_response,
                HTTPStatus.METHOD_NOT_ALLOWED,
                {"error": "method not allowed"},
            )

        try:
            payload = _load_json_body(environ)
            query = build_baseline_query(payload)
            response = baseline_service.query_baseline(query)
            return _json_response(
                start_response,
                HTTPStatus.OK,
                serialize_baseline_response(response),
            )
        except ValueError as exc:
            return _json_response(
                start_response,
                HTTPStatus.BAD_REQUEST,
                {"error": str(exc)},
            )

    return app


def _load_json_body(environ: dict[str, Any]) -> dict[str, Any]:
    content_length = int(environ.get("CONTENT_LENGTH", "0") or "0")
    raw_body = environ["wsgi.input"].read(content_length)
    if not raw_body:
        raise ValueError("request body must not be empty")

    try:
        payload = json.loads(raw_body.decode("utf-8"))
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive path
        raise ValueError("request body must be valid JSON") from exc

    if not isinstance(payload, dict):
        raise ValueError("request body must be a JSON object")
    return payload


def _json_response(
    start_response: StartResponse,
    status: HTTPStatus,
    payload: dict[str, Any],
):
    encoded = json.dumps(payload, ensure_ascii=False, default=_json_default).encode("utf-8")
    start_response(
        f"{status.value} {status.phrase}",
        [
            ("Content-Type", "application/json"),
            ("Content-Length", str(len(encoded))),
        ],
    )
    return [encoded]


def _html_response(
    start_response: StartResponse,
    status: HTTPStatus,
    payload: str,
):
    encoded = payload.encode("utf-8")
    start_response(
        f"{status.value} {status.phrase}",
        [
            ("Content-Type", "text/html; charset=utf-8"),
            ("Content-Length", str(len(encoded))),
        ],
    )
    return [encoded]


def _parse_recent_metrics_query(environ: dict[str, Any]) -> tuple[int, int]:
    query_string = environ.get("QUERY_STRING", "")
    params = parse_qs(query_string)
    raw_page = params.get("page", ["1"])[0]
    raw_page_size = params.get("page_size", params.get("limit", ["100"]))[0]

    try:
        page = int(raw_page)
    except ValueError as exc:
        raise ValueError("page must be an integer") from exc

    try:
        page_size = int(raw_page_size)
    except ValueError as exc:
        raise ValueError("page_size must be an integer") from exc

    return page, page_size


def _json_default(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value)!r} is not JSON serializable")
