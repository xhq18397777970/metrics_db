from __future__ import annotations

from datetime import datetime, timezone

from tools.code import format_window_time as format_code_window_time
from tools.cpu import format_window_time as format_cpu_window_time
from tools.tp import format_window_time as format_tp_window_time


def test_tools_convert_aware_datetimes_to_local_wall_clock() -> None:
    value = datetime(2026, 3, 12, 5, 30, tzinfo=timezone.utc)

    assert format_cpu_window_time(value) == "2026-03-12 13:30:00"
    assert format_code_window_time(value) == "2026-03-12 13:30:00"
    assert format_tp_window_time(value) == "2026-03-12 13:30:00"
