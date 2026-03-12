from __future__ import annotations

from datetime import datetime, timezone

import pytest

from cluster_metrics_platform.domain.time_window import (
    align_to_bucket,
    get_closed_window,
    iter_windows,
)


@pytest.mark.parametrize(
    ("current_time", "expected_start", "expected_end"),
    [
        (
            datetime(2026, 3, 12, 10, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 12, 9, 55, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 12, 10, 0, 0, tzinfo=timezone.utc),
        ),
        (
            datetime(2026, 3, 12, 10, 0, 1, tzinfo=timezone.utc),
            datetime(2026, 3, 12, 9, 55, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 12, 10, 0, 0, tzinfo=timezone.utc),
        ),
        (
            datetime(2026, 3, 12, 10, 4, 59, tzinfo=timezone.utc),
            datetime(2026, 3, 12, 9, 55, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 12, 10, 0, 0, tzinfo=timezone.utc),
        ),
        (
            datetime(2026, 3, 12, 10, 5, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 12, 10, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 12, 10, 5, 0, tzinfo=timezone.utc),
        ),
    ],
)
def test_get_closed_window_returns_most_recent_closed_bucket(
    current_time: datetime,
    expected_start: datetime,
    expected_end: datetime,
) -> None:
    window = get_closed_window(current_time)

    assert window.bucket_time == expected_start
    assert window.start_time == expected_start
    assert window.end_time == expected_end
    assert window.window_seconds == 300


def test_align_to_bucket_removes_seconds_and_microseconds() -> None:
    current_time = datetime(2026, 3, 12, 10, 17, 42, 123456, tzinfo=timezone.utc)

    assert align_to_bucket(current_time) == datetime(
        2026,
        3,
        12,
        10,
        15,
        0,
        tzinfo=timezone.utc,
    )


def test_iter_windows_advances_in_five_minute_steps() -> None:
    start = datetime(2026, 3, 12, 10, 2, 0, tzinfo=timezone.utc)
    end = datetime(2026, 3, 12, 10, 13, 0, tzinfo=timezone.utc)

    windows = list(iter_windows(start, end))

    assert [window.start_time for window in windows] == [
        datetime(2026, 3, 12, 10, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 3, 12, 10, 5, 0, tzinfo=timezone.utc),
        datetime(2026, 3, 12, 10, 10, 0, tzinfo=timezone.utc),
    ]
    assert all(window.window_seconds == 300 for window in windows)


def test_iter_windows_returns_empty_for_non_positive_range() -> None:
    start = datetime(2026, 3, 12, 10, 0, 0, tzinfo=timezone.utc)

    assert list(iter_windows(start, start)) == []


def test_align_to_bucket_rejects_non_positive_steps() -> None:
    with pytest.raises(ValueError):
        align_to_bucket(datetime(2026, 3, 12, 10, 0, 0), step_minutes=0)
