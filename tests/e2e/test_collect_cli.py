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
            points_written=4,
            runs_written=2,
        )


def test_collect_window_cli_outputs_execution_summary(capsys) -> None:
    service = FakeCollectionService(calls=[])

    exit_code = main(
        [
            "collect-window",
            "--window-end",
            "2026-03-12T10:05:00+00:00",
            "--cluster",
            "cluster-a",
        ],
        collection_service=service,
    )

    captured = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert captured["command"] == "collect-window"
    assert captured["window"] == {
        "bucket_time": "2026-03-12T10:00:00+00:00",
        "start_time": "2026-03-12T10:00:00+00:00",
        "end_time": "2026-03-12T10:05:00+00:00",
        "window_seconds": 300,
    }
    assert captured["selected_cluster_count"] == 1
    assert service.calls[0][1] == ["cluster-a"]


def test_collect_window_cli_aligns_naive_input_to_utc(capsys) -> None:
    service = FakeCollectionService(calls=[])

    main(
        [
            "collect-window",
            "--window-end",
            "2026-03-12 10:00:00",
        ],
        collection_service=service,
    )

    captured = json.loads(capsys.readouterr().out)
    called_window = service.calls[0][0]

    assert captured["window"]["start_time"] == "2026-03-12T09:55:00+00:00"
    assert called_window.end_time == datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc)
