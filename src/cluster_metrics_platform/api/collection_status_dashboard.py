"""Static HTML for the collection status dashboard."""

from __future__ import annotations


def render_collection_status_dashboard() -> str:
    """Return the single-page collection status dashboard HTML."""

    return """<!doctype html>
<html lang="zh-CN">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>任务后台</title>
    <style>
      :root {
        color-scheme: light;
        --bg: #f4efe7;
        --panel: rgba(255, 252, 247, 0.94);
        --line: #d3c6b2;
        --text: #1f1a16;
        --muted: #65594a;
        --accent: #0e7c66;
        --accent-strong: #085646;
        --warn: #b96a1a;
        --danger: #a23529;
      }

      * {
        box-sizing: border-box;
      }

      body {
        margin: 0;
        min-height: 100vh;
        font-family: "Iowan Old Style", "Palatino Linotype", serif;
        background:
          radial-gradient(circle at top left, rgba(14, 124, 102, 0.14), transparent 36%),
          linear-gradient(180deg, #f9f4ec 0%, var(--bg) 100%);
        color: var(--text);
      }

      .shell {
        width: min(1200px, calc(100vw - 32px));
        margin: 32px auto;
        padding: 24px;
        border: 1px solid rgba(101, 89, 74, 0.12);
        border-radius: 24px;
        background: var(--panel);
        box-shadow: 0 24px 60px rgba(43, 31, 18, 0.08);
      }

      .header {
        display: flex;
        justify-content: space-between;
        gap: 16px;
        align-items: end;
        margin-bottom: 20px;
      }

      h1 {
        margin: 0;
        font-size: clamp(28px, 5vw, 44px);
        line-height: 1;
      }

      .subtitle {
        margin: 10px 0 0;
        color: var(--muted);
        font-size: 15px;
      }

      .controls {
        display: flex;
        gap: 10px;
        align-items: center;
        flex-wrap: wrap;
      }

      button,
      select,
      a.link-button {
        border-radius: 999px;
        border: 1px solid var(--line);
        font: inherit;
      }

      button,
      a.link-button {
        padding: 10px 18px;
        background: var(--accent);
        color: #fff;
        cursor: pointer;
        text-decoration: none;
      }

      button:hover,
      a.link-button:hover {
        background: var(--accent-strong);
      }

      select {
        padding: 10px 14px;
        background: #fffdf8;
        color: var(--text);
      }

      .status {
        margin: 0 0 16px;
        color: var(--muted);
        font-size: 14px;
      }

      .summary-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 12px;
        margin: 0 0 20px;
      }

      .card {
        padding: 14px 16px;
        border: 1px solid rgba(101, 89, 74, 0.14);
        border-radius: 18px;
        background: rgba(255, 255, 255, 0.72);
      }

      .card-label {
        margin: 0 0 8px;
        font-size: 12px;
        color: var(--muted);
        text-transform: uppercase;
        letter-spacing: 0.04em;
      }

      .card-value {
        margin: 0;
        font-size: 24px;
        line-height: 1.1;
      }

      .badge {
        display: inline-block;
        padding: 6px 10px;
        border-radius: 999px;
        font-size: 13px;
      }

      .badge-running {
        background: rgba(14, 124, 102, 0.14);
        color: var(--accent-strong);
      }

      .badge-idle {
        background: rgba(31, 26, 22, 0.08);
        color: var(--muted);
      }

      .badge-stopped {
        background: rgba(162, 53, 41, 0.14);
        color: var(--danger);
      }

      .badge-completed {
        background: rgba(14, 124, 102, 0.14);
        color: var(--accent-strong);
      }

      .badge-completed_with_errors {
        background: rgba(185, 106, 26, 0.16);
        color: var(--warn);
      }

      .badge-failed {
        background: rgba(162, 53, 41, 0.14);
        color: var(--danger);
      }

      .badge-running-window {
        background: rgba(14, 124, 102, 0.14);
        color: var(--accent-strong);
      }

      .table-wrap {
        overflow: auto;
        border-radius: 18px;
        border: 1px solid rgba(101, 89, 74, 0.14);
        background: rgba(255, 255, 255, 0.72);
      }

      table {
        width: 100%;
        min-width: 1080px;
        border-collapse: collapse;
        font-family: "SFMono-Regular", "Menlo", monospace;
        font-size: 13px;
      }

      thead {
        background: rgba(31, 26, 22, 0.05);
      }

      th,
      td {
        padding: 12px 14px;
        border-bottom: 1px solid rgba(101, 89, 74, 0.12);
        text-align: left;
        vertical-align: top;
      }

      tbody tr:nth-child(even) {
        background: rgba(244, 239, 231, 0.45);
      }

      .empty {
        text-align: center;
        color: var(--muted);
      }

      @media (max-width: 720px) {
        .header {
          flex-direction: column;
          align-items: stretch;
        }

        .controls {
          justify-content: space-between;
        }
      }
    </style>
  </head>
  <body>
    <main class="shell">
      <section class="header">
        <div>
          <h1>任务后台</h1>
          <p class="subtitle">查看当前采集进度、后台是否停止，以及最近时间桶的完成情况</p>
        </div>
        <div class="controls">
          <label for="limit-select">时间桶</label>
          <select id="limit-select">
            <option value="10" selected>10</option>
            <option value="20">20</option>
            <option value="30">30</option>
          </select>
          <button id="start-button" type="button">启动</button>
          <button id="stop-button" type="button">终止</button>
          <button id="refresh-button" type="button">刷新</button>
          <a class="link-button" href="/">指标前台</a>
        </div>
      </section>

      <p class="status" id="status">正在加载采集状态...</p>

      <section class="summary-grid">
        <article class="card">
          <p class="card-label">调度器状态</p>
          <p class="card-value" id="scheduler-state">-</p>
        </article>
        <article class="card">
          <p class="card-label">最近心跳</p>
          <p class="card-value" id="heartbeat-at">-</p>
        </article>
        <article class="card">
          <p class="card-label">当前时间桶</p>
          <p class="card-value" id="active-bucket">-</p>
        </article>
        <article class="card">
          <p class="card-label">任务进度</p>
          <p class="card-value" id="task-progress">-</p>
        </article>
        <article class="card">
          <p class="card-label">未完成任务</p>
          <p class="card-value" id="remaining-tasks">-</p>
        </article>
        <article class="card">
          <p class="card-label">成功 / 失败</p>
          <p class="card-value" id="success-failed">-</p>
        </article>
      </section>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>时间桶</th>
              <th>状态</th>
              <th>集群数</th>
              <th>总任务</th>
              <th>已完成</th>
              <th>未完成</th>
              <th>成功</th>
              <th>部分成功</th>
              <th>失败</th>
              <th>写入点数</th>
              <th>更新时间</th>
            </tr>
          </thead>
          <tbody id="status-table-body">
            <tr>
              <td class="empty" colspan="11">加载中...</td>
            </tr>
          </tbody>
        </table>
      </div>
    </main>

    <script>
      const statusNode = document.getElementById("status");
      const limitSelect = document.getElementById("limit-select");
      const startButton = document.getElementById("start-button");
      const stopButton = document.getElementById("stop-button");
      const refreshButton = document.getElementById("refresh-button");
      const tableBody = document.getElementById("status-table-body");
      const schedulerState = document.getElementById("scheduler-state");
      const heartbeatAt = document.getElementById("heartbeat-at");
      const activeBucket = document.getElementById("active-bucket");
      const taskProgress = document.getElementById("task-progress");
      const remainingTasks = document.getElementById("remaining-tasks");
      const successFailed = document.getElementById("success-failed");

      function escapeHtml(value) {
        return String(value)
          .replaceAll("&", "&amp;")
          .replaceAll("<", "&lt;")
          .replaceAll(">", "&gt;")
          .replaceAll('"', "&quot;")
          .replaceAll("'", "&#39;");
      }

      function formatTime(value) {
        if (!value) {
          return "-";
        }
        const parsed = new Date(value);
        if (Number.isNaN(parsed.getTime())) {
          return String(value);
        }
        return parsed.toLocaleString();
      }

      function statusLabel(status) {
        if (status === "running") return "采集中";
        if (status === "idle") return "空闲";
        if (status === "stopped") return "已停止";
        if (status === "completed") return "已完成";
        if (status === "completed_with_errors") return "已完成（有异常）";
        if (status === "failed") return "失败";
        return status || "-";
      }

      function statusClass(status) {
        return `badge badge-${status || "idle"}`;
      }

      function renderSummary(scheduler) {
        const schedulerBadgeClass = statusClass(scheduler.status);
        const schedulerLabel = escapeHtml(statusLabel(scheduler.status));
        schedulerState.innerHTML =
          `<span class="${schedulerBadgeClass}">${schedulerLabel}</span>`;
        heartbeatAt.textContent = formatTime(scheduler.last_heartbeat_at);
        activeBucket.textContent = formatTime(scheduler.active_bucket_time);
        taskProgress.textContent = `${scheduler.completed_tasks}/${scheduler.total_tasks}`;
        remainingTasks.textContent = String(scheduler.remaining_tasks);
        successFailed.textContent = `${scheduler.success_count} / ${scheduler.failed_count}`;
        const canStop = scheduler.status === "running" || scheduler.status === "idle";
        startButton.disabled = canStop && !scheduler.is_stale;
        stopButton.disabled = !canStop;
      }

      function renderWindows(rows) {
        if (!rows.length) {
          tableBody.innerHTML = '<tr><td class="empty" colspan="11">暂无采集记录</td></tr>';
          return;
        }

        tableBody.innerHTML = rows.map((row) => `
          <tr>
            <td>${escapeHtml(formatTime(row.bucket_time))}</td>
            <td>
              <span class="${statusClass(row.status)}">${escapeHtml(statusLabel(row.status))}</span>
            </td>
            <td>${escapeHtml(row.selected_cluster_count)}</td>
            <td>${escapeHtml(row.total_tasks)}</td>
            <td>${escapeHtml(row.completed_tasks)}</td>
            <td>${escapeHtml(row.remaining_tasks)}</td>
            <td>${escapeHtml(row.success_count)}</td>
            <td>${escapeHtml(row.partial_success_count)}</td>
            <td>${escapeHtml(row.failed_count)}</td>
            <td>${escapeHtml(row.points_written)}</td>
            <td>${escapeHtml(formatTime(row.updated_at))}</td>
          </tr>
        `).join("");
      }

      async function loadStatus() {
        const limit = limitSelect.value;
        statusNode.textContent = "正在刷新采集状态...";
        refreshButton.disabled = true;

        try {
          const endpoint = `/api/v1/collection/status?limit=${encodeURIComponent(limit)}`;
          const response = await fetch(endpoint);
          if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
          }
          const payload = await response.json();
          renderSummary(payload.scheduler || {});
          renderWindows(payload.windows || []);
          const refreshedAt = new Date().toLocaleTimeString();
          statusNode.textContent =
            `已刷新 ${payload.windows.length} 个时间桶，更新时间 ${refreshedAt}`;
        } catch (error) {
          tableBody.innerHTML = '<tr><td class="empty" colspan="11">加载失败</td></tr>';
          statusNode.textContent = `刷新失败: ${error.message}`;
        } finally {
          refreshButton.disabled = false;
        }
      }

      async function controlScheduler(action) {
        const actionLabel = action === "start" ? "启动" : "终止";
        statusNode.textContent = `正在${actionLabel}自动采集任务...`;
        startButton.disabled = true;
        stopButton.disabled = true;
        refreshButton.disabled = true;

        try {
          const response = await fetch(`/api/v1/scheduler/${action}`, { method: "POST" });
          const payload = await response.json();
          if (!response.ok) {
            throw new Error(payload.error || `HTTP ${response.status}`);
          }
          await loadStatus();
          statusNode.textContent = payload.message || `自动采集任务已${actionLabel}`;
        } catch (error) {
          statusNode.textContent = `${actionLabel}失败: ${error.message}`;
          await loadStatus();
        } finally {
          refreshButton.disabled = false;
        }
      }

      startButton.addEventListener("click", () => controlScheduler("start"));
      stopButton.addEventListener("click", () => controlScheduler("stop"));
      refreshButton.addEventListener("click", loadStatus);
      limitSelect.addEventListener("change", loadStatus);
      loadStatus();
      setInterval(loadStatus, 5000);
    </script>
  </body>
</html>
"""
