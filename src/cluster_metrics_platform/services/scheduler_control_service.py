"""Start and stop the background scheduler process."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from cluster_metrics_platform.settings import (
    DEFAULT_CLUSTER_CONFIG_ENV,
    DEFAULT_DISPATCH_MAX_CONCURRENCY_ENV,
    DEFAULT_DISPATCH_RETRY_LIMIT_ENV,
    DEFAULT_DISPATCH_TIMEOUT_ENV,
    DEFAULT_ENABLED_COLLECTORS_ENV,
    DEFAULT_INIT_STORAGE_ENV,
)
from cluster_metrics_platform.storage.db import DEFAULT_DATABASE_URL_ENV

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PID_FILE_PATH = PROJECT_ROOT / ".runtime" / "scheduler.pid"


class SchedulerControlService:
    """Control the long-running scheduler process used for automatic collection."""

    def __init__(
        self,
        settings,
        *,
        collection_status_service=None,
        pid_file_path: Path | None = None,
    ) -> None:
        self._settings = settings
        self._collection_status_service = collection_status_service
        self._pid_file_path = pid_file_path or DEFAULT_PID_FILE_PATH

    def start_scheduler(self) -> dict[str, object]:
        pid = self._resolve_scheduler_pid()
        if pid is not None:
            return {
                "status": "already_running",
                "pid": pid,
                "message": "自动采集任务已在运行",
            }

        self._pid_file_path.parent.mkdir(parents=True, exist_ok=True)
        process = subprocess.Popen(  # noqa: S603
            [sys.executable, "-m", "cluster_metrics_platform.main", "run-scheduler"],
            cwd=str(PROJECT_ROOT),
            env=self._build_env(),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        self._write_pid(process.pid)

        return {
            "status": "started",
            "pid": process.pid,
            "message": "自动采集任务已启动",
        }

    def stop_scheduler(self) -> dict[str, object]:
        pid = self._resolve_scheduler_pid()
        if pid is None:
            self._remove_pid_file()
            self._mark_scheduler_stopped()
            return {
                "status": "already_stopped",
                "message": "自动采集任务当前未运行",
            }

        try:
            os.killpg(os.getpgid(pid), signal.SIGINT)
        except ProcessLookupError:
            self._remove_pid_file()
            self._mark_scheduler_stopped()
            return {
                "status": "already_stopped",
                "message": "自动采集任务当前未运行",
            }

        stopped = self._wait_for_exit(pid, timeout_seconds=5.0)
        if not stopped:
            try:
                os.killpg(os.getpgid(pid), signal.SIGTERM)
            except ProcessLookupError:
                stopped = True
            else:
                stopped = self._wait_for_exit(pid, timeout_seconds=5.0)

        self._remove_pid_file()
        self._mark_scheduler_stopped()

        return {
            "status": "stopped" if stopped else "stop_requested",
            "pid": pid,
            "message": "自动采集任务已终止" if stopped else "已发送终止信号，正在停止自动采集任务",
        }

    def _build_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env[DEFAULT_DATABASE_URL_ENV] = self._settings.database.dsn
        env[DEFAULT_CLUSTER_CONFIG_ENV] = str(self._settings.cluster_config_path)
        env[DEFAULT_INIT_STORAGE_ENV] = "true" if self._settings.initialize_storage else "false"
        env[DEFAULT_ENABLED_COLLECTORS_ENV] = ",".join(self._settings.enabled_collectors)
        env[DEFAULT_DISPATCH_MAX_CONCURRENCY_ENV] = str(
            self._settings.dispatcher_max_concurrency
        )
        env[DEFAULT_DISPATCH_RETRY_LIMIT_ENV] = str(self._settings.dispatcher_retry_limit)
        env[DEFAULT_DISPATCH_TIMEOUT_ENV] = str(self._settings.dispatcher_timeout_seconds)
        return env

    def _resolve_scheduler_pid(self) -> int | None:
        pid = self._read_pid_file()
        if pid is not None and self._process_exists(pid):
            return pid

        discovered_pid = self._discover_scheduler_pid()
        if discovered_pid is None:
            self._remove_pid_file()
            return None

        self._write_pid(discovered_pid)
        return discovered_pid

    def _discover_scheduler_pid(self) -> int | None:
        result = subprocess.run(  # noqa: S603
            ["pgrep", "-f", "cluster_metrics_platform.main run-scheduler"],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode not in {0, 1}:
            return None

        pids = [
            int(raw_pid)
            for raw_pid in result.stdout.splitlines()
            if raw_pid.strip().isdigit()
        ]
        pids = [pid for pid in pids if pid != os.getpid()]
        if not pids:
            return None
        return max(pids)

    def _read_pid_file(self) -> int | None:
        if not self._pid_file_path.exists():
            return None
        raw_value = self._pid_file_path.read_text(encoding="utf-8").strip()
        if not raw_value.isdigit():
            return None
        return int(raw_value)

    def _write_pid(self, pid: int) -> None:
        self._pid_file_path.write_text(str(pid), encoding="utf-8")

    def _remove_pid_file(self) -> None:
        if self._pid_file_path.exists():
            self._pid_file_path.unlink()

    @staticmethod
    def _process_exists(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True

    def _wait_for_exit(self, pid: int, *, timeout_seconds: float) -> bool:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if not self._process_exists(pid):
                return True
            time.sleep(0.1)
        return not self._process_exists(pid)

    def _mark_scheduler_stopped(self) -> None:
        if self._collection_status_service is None:
            return
        self._collection_status_service.mark_scheduler_stopped(
            step_minutes=5,
            last_error="stopped via dashboard",
        )
