from __future__ import annotations

from datetime import datetime, timezone

import pytest

from cluster_metrics_platform.domain.models import TimeWindow
from cluster_metrics_platform.services.backfill_service import BackfillService
from cluster_metrics_platform.services.collection_service import CollectionExecution


class FakeCollectionService:
    def __init__(self) -> None:
        self.calls: list[tuple[TimeWindow, set[str] | None]] = []

    async def collect_window(
        self,
        window: TimeWindow,
        cluster_names,
    ) -> CollectionExecution:
        scoped_names = set(cluster_names) if cluster_names else None
        self.calls.append((window, scoped_names))
        return CollectionExecution(
            window=window,
            summary=None,  # type: ignore[arg-type]
            loaded_cluster_count=2,
            selected_cluster_count=2,
            points_written=2,
            runs_written=2,
        )


@pytest.mark.asyncio
async def test_backfill_advances_one_window_at_a_time() -> None:
    service = FakeCollectionService()
    backfill = BackfillService(service)

    result = await backfill.backfill(
        start_time=datetime(2026, 3, 12, 10, 2, tzinfo=timezone.utc),
        end_time=datetime(2026, 3, 12, 10, 16, tzinfo=timezone.utc),
        cluster_names={"cluster-a", "cluster-b"},
    )

    assert result.total_windows == 4
    assert result.total_points_written == 8
    assert result.total_runs_written == 8

    observed_windows = [
        (window.start_time, window.end_time)
        for window, _ in service.calls
    ]
    assert observed_windows == [
        (
            datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc),
        ),
        (
            datetime(2026, 3, 12, 10, 5, tzinfo=timezone.utc),
            datetime(2026, 3, 12, 10, 10, tzinfo=timezone.utc),
        ),
        (
            datetime(2026, 3, 12, 10, 10, tzinfo=timezone.utc),
            datetime(2026, 3, 12, 10, 15, tzinfo=timezone.utc),
        ),
        (
            datetime(2026, 3, 12, 10, 15, tzinfo=timezone.utc),
            datetime(2026, 3, 12, 10, 20, tzinfo=timezone.utc),
        ),
    ]
    assert [scope for _, scope in service.calls] == [
        {"cluster-a", "cluster-b"},
        {"cluster-a", "cluster-b"},
        {"cluster-a", "cluster-b"},
        {"cluster-a", "cluster-b"},
    ]


@pytest.mark.asyncio
async def test_backfill_returns_empty_result_for_invalid_range() -> None:
    service = FakeCollectionService()
    backfill = BackfillService(service)

    result = await backfill.backfill(
        start_time=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
        end_time=datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
    )

    assert result.total_windows == 0
    assert result.total_points_written == 0
    assert service.calls == []
