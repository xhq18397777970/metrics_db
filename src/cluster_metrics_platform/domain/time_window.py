"""Utilities for canonical 5-minute collection windows."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta

from cluster_metrics_platform.domain.models import TimeWindow


def align_to_bucket(value: datetime, step_minutes: int = 5) -> datetime:
    """Align a timestamp down to the start of its containing bucket."""
    _validate_step_minutes(step_minutes)
    midnight = value.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed_seconds = int((value - midnight).total_seconds())
    bucket_seconds = step_minutes * 60
    aligned_seconds = (elapsed_seconds // bucket_seconds) * bucket_seconds
    return midnight + timedelta(seconds=aligned_seconds)


def get_closed_window(now: datetime, step_minutes: int = 5) -> TimeWindow:
    """Return the most recently closed time window."""
    bucket_end = align_to_bucket(now, step_minutes=step_minutes)
    bucket_start = bucket_end - timedelta(minutes=step_minutes)
    return TimeWindow(
        bucket_time=bucket_start,
        start_time=bucket_start,
        end_time=bucket_end,
        window_seconds=step_minutes * 60,
    )


def iter_windows(start: datetime, end: datetime, step_minutes: int = 5) -> Iterator[TimeWindow]:
    """Yield canonical windows that cover the requested time range."""
    _validate_step_minutes(step_minutes)
    if start >= end:
        return

    step = timedelta(minutes=step_minutes)
    cursor = align_to_bucket(start, step_minutes=step_minutes)
    stop = _ceil_to_bucket(end, step_minutes=step_minutes)

    while cursor < stop:
        yield TimeWindow(
            bucket_time=cursor,
            start_time=cursor,
            end_time=cursor + step,
            window_seconds=step_minutes * 60,
        )
        cursor += step


def _ceil_to_bucket(value: datetime, step_minutes: int) -> datetime:
    aligned = align_to_bucket(value, step_minutes=step_minutes)
    if value == aligned:
        return aligned
    return aligned + timedelta(minutes=step_minutes)


def _validate_step_minutes(step_minutes: int) -> None:
    if step_minutes <= 0:
        raise ValueError("step_minutes must be a positive integer")

