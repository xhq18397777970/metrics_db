"""Minimal WSGI application for baseline query APIs."""

from __future__ import annotations

import json
from collections.abc import Callable
from http import HTTPStatus
from typing import Any

from cluster_metrics_platform.api.routes.baselines import (
    build_baseline_query,
    serialize_baseline_response,
)

StartResponse = Callable[[str, list[tuple[str, str]]], object]


def create_app(baseline_service):
    """Create a tiny WSGI app exposing the baseline query endpoint."""

    def app(environ: dict[str, Any], start_response: StartResponse):
        method = environ.get("REQUEST_METHOD", "GET")
        path = environ.get("PATH_INFO", "")

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


def _json_default(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value)!r} is not JSON serializable")
