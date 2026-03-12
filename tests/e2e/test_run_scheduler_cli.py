from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone

from cluster_metrics_platform.domain.models import TimeWindow
from cluster_metrics_platform.main import main
from cluster_metrics_platform.orchestrator.models import DispatchSummary
from cluster_metrics_platform.services.collection_service import CollectionExecution


@dataclass
class FakeCollectionService:
    calls: list[tuple[TimeWindow, list[str] | None]]

    async def collect_window(self, window: TimeWindow, cluster_names):
        names = list(cluster_names) if cluster_names else None
        self.calls.append((window, names))
        return CollectionExecution(
            window=window,
            summary=DispatchSummary(window=window),
            loaded_cluster_count=3,
            selected_cluster_count=1 if names else 3,
            points_written=6,
            runs_written=4,
        )


def test_run_scheduler_cli_executes_requested_number_of_windows(capsys) -> None:
    service = FakeCollectionService(calls=[])
    now_values = iter(
        [
            datetime(2026, 3, 12, 10, 7, tzinfo=timezone.utc),
            datetime(2026, 3, 12, 10, 8, tzinfo=timezone.utc),
            datetime(2026, 3, 12, 10, 12, tzinfo=timezone.utc),
        ]
    )
    sleep_calls = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    exit_code = main(
        ["run-scheduler", "--cluster", "cluster-a", "--iterations", "2"],
        collection_service=service,
        scheduler_now_provider=lambda: next(now_values),
        scheduler_sleep=fake_sleep,
    )

    output_lines = [json.loads(line) for line in capsys.readouterr().out.strip().splitlines()]

    assert exit_code == 0
    assert [line["window"]["start_time"] for line in output_lines] == [
        "2026-03-12T10:00:00+00:00",
        "2026-03-12T10:05:00+00:00",
    ]
    assert len(service.calls) == 2
    assert service.calls[0][1] == ["cluster-a"]
    assert sleep_calls == [120.0]
