from __future__ import annotations

import pytest

from cluster_metrics_platform.services.metrics_table_service import MetricsTableService


class StubRepository:
    def __init__(self) -> None:
        self.calls = []

    def count_recent_points(self, *, visible_limit: int = 5000):
        self.calls.append(("count", visible_limit))
        return 321

    def list_recent_points(self, *, page: int = 1, page_size: int = 100, visible_limit: int = 5000):
        self.calls.append(("list", page, page_size, visible_limit))
        return [{"cluster_name": "cluster-a", "metric_name": "cpu_avg"}]


def test_metrics_table_service_returns_recent_rows() -> None:
    repository = StubRepository()
    service = MetricsTableService(repository)

    payload = service.list_recent_points(page=2, page_size=50)

    assert payload == {
        "page": 2,
        "page_size": 50,
        "total_rows": 321,
        "total_pages": 7,
        "max_rows": 5000,
        "start_row": 51,
        "end_row": 51,
        "rows": [{"cluster_name": "cluster-a", "metric_name": "cpu_avg"}],
    }
    assert repository.calls == [("count", 5000), ("list", 2, 50, 5000)]


def test_metrics_table_service_defaults_to_first_page_of_100_rows() -> None:
    repository = StubRepository()
    service = MetricsTableService(repository)

    payload = service.list_recent_points()

    assert payload["page"] == 1
    assert payload["page_size"] == 100
    assert repository.calls == [("count", 5000), ("list", 1, 100, 5000)]


@pytest.mark.parametrize("page", [0, -1])
def test_metrics_table_service_validates_page(page: int) -> None:
    service = MetricsTableService(StubRepository())

    with pytest.raises(ValueError, match="page must be greater than 0"):
        service.list_recent_points(page=page)


@pytest.mark.parametrize("page_size", [0, -1, 5001])
def test_metrics_table_service_validates_page_size(page_size: int) -> None:
    service = MetricsTableService(StubRepository())

    with pytest.raises(ValueError, match="page_size must be between 1 and 5000"):
        service.list_recent_points(page_size=page_size)
