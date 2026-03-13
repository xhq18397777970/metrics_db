from __future__ import annotations

import os
from pathlib import Path

from cluster_metrics_platform.services.scheduler_control_service import (
    SchedulerControlService,
)
from cluster_metrics_platform.settings import AppSettings
from cluster_metrics_platform.storage.db import DatabaseConfig


class FakeCollectionStatusService:
    def __init__(self) -> None:
        self.calls: list[tuple[int, str | None]] = []

    def mark_scheduler_stopped(self, *, step_minutes: int, last_error: str | None = None):
        self.calls.append((step_minutes, last_error))


class FakePopen:
    def __init__(self, pid: int) -> None:
        self.pid = pid


def _build_settings(tmp_path: Path) -> AppSettings:
    return AppSettings(
        cluster_config_path=tmp_path / "cluster.json",
        database=DatabaseConfig(dsn="postgresql:///cluster_metrics_test"),
        initialize_storage=True,
    )


def test_start_scheduler_creates_background_process(monkeypatch, tmp_path) -> None:
    pid_file = tmp_path / "scheduler.pid"
    settings = _build_settings(tmp_path)
    control = SchedulerControlService(settings, pid_file_path=pid_file)

    monkeypatch.setattr(control, "_resolve_scheduler_pid", lambda: None)
    captured: dict[str, object] = {}

    def fake_popen(*args, **kwargs):
        captured["args"] = args[0]
        captured["env"] = kwargs["env"]
        return FakePopen(43210)

    monkeypatch.setattr("subprocess.Popen", fake_popen)

    payload = control.start_scheduler()

    assert payload == {
        "status": "started",
        "pid": 43210,
        "message": "自动采集任务已启动",
    }
    assert pid_file.read_text(encoding="utf-8") == "43210"
    assert captured["args"] == [
        os.sys.executable,
        "-m",
        "cluster_metrics_platform.main",
        "run-scheduler",
    ]
    assert captured["env"]["CLUSTER_METRICS_INIT_STORAGE"] == "true"


def test_stop_scheduler_marks_runtime_stopped(monkeypatch, tmp_path) -> None:
    pid_file = tmp_path / "scheduler.pid"
    pid_file.write_text("43210", encoding="utf-8")
    status_service = FakeCollectionStatusService()
    settings = _build_settings(tmp_path)
    control = SchedulerControlService(
        settings,
        collection_status_service=status_service,
        pid_file_path=pid_file,
    )

    monkeypatch.setattr(control, "_resolve_scheduler_pid", lambda: 43210)
    monkeypatch.setattr(control, "_wait_for_exit", lambda *_args, **_kwargs: True)
    monkeypatch.setattr("os.getpgid", lambda _pid: 43210)
    kill_calls: list[tuple[int, int]] = []
    monkeypatch.setattr("os.killpg", lambda pgid, sig: kill_calls.append((pgid, sig)))

    payload = control.stop_scheduler()

    assert payload == {
        "status": "stopped",
        "pid": 43210,
        "message": "自动采集任务已终止",
    }
    assert not pid_file.exists()
    assert kill_calls
    assert status_service.calls == [(5, "stopped via dashboard")]


def test_stop_scheduler_returns_already_stopped_when_no_process(tmp_path) -> None:
    status_service = FakeCollectionStatusService()
    settings = _build_settings(tmp_path)
    control = SchedulerControlService(
        settings,
        collection_status_service=status_service,
        pid_file_path=tmp_path / "scheduler.pid",
    )
    control._resolve_scheduler_pid = lambda: None  # type: ignore[method-assign]

    payload = control.stop_scheduler()

    assert payload == {
        "status": "already_stopped",
        "message": "自动采集任务当前未运行",
    }
    assert status_service.calls == [(5, "stopped via dashboard")]
