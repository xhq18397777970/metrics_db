"""Helpers for selecting canonical scheduled collection windows."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Collection
from dataclasses import dataclass
from datetime import datetime, timezone

from cluster_metrics_platform.domain.models import TimeWindow
from cluster_metrics_platform.domain.time_window import get_closed_window

CollectWindowCallable = Callable[[TimeWindow, Collection[str] | None], Awaitable[object]]
NowProvider = Callable[[], datetime]


def default_now_provider() -> datetime:
    """Return the current UTC time."""

    return datetime.now(timezone.utc)


@dataclass(slots=True)
class ScheduledCollector:
    """Resolve the most recently closed bucket and hand off collection work."""

    collect_window: CollectWindowCallable
    step_minutes: int = 5
    now_provider: NowProvider = default_now_provider

    def resolve_window(self) -> TimeWindow:
        """Return the canonical window for the next scheduled run."""

        return get_closed_window(self.now_provider(), step_minutes=self.step_minutes)

    async def collect_once(self, cluster_names: Collection[str] | None = None) -> object:
        """Collect one scheduled window without drifting from completion time."""

        window = self.resolve_window()
        return await self.collect_window(window, cluster_names)
