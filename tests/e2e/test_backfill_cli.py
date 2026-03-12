from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone

from cluster_metrics_platform.main import main
from cluster_metrics_platform.services.backfill_service import BackfillExecution


@dataclass
class FakeBackfillService:
    calls: list[tuple[datetime, datetime, list[str] | None]]

    async def backfill(self, start_time: datetime, end_time: datetime, cluster_names):
        names = list(cluster_names) if cluster_names else None
        self.calls.append((start_time, end_time, names))
        return BackfillExecution()


def test_backfill_cli_passes_range_and_scope_to_service(capsys) -> None:
    service = FakeBackfillService(calls=[])

    exit_code = main(
        [
            "backfill",
            "--start",
            "2026-03-12T10:00:00+00:00",
            "--end",
            "2026-03-12T10:15:00+00:00",
            "--cluster",
            "cluster-a",
            "--cluster",
            "cluster-b",
        ],
        backfill_service=service,
    )

    captured = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert captured["command"] == "backfill"
    assert captured["start_time"] == "2026-03-12T10:00:00+00:00"
    assert captured["end_time"] == "2026-03-12T10:15:00+00:00"
    assert service.calls == [
        (
            datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 12, 10, 15, tzinfo=timezone.utc),
            ["cluster-a", "cluster-b"],
        )
    ]
