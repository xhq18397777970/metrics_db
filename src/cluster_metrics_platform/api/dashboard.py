"""Static HTML for the lightweight metrics dashboard."""

from __future__ import annotations


def render_dashboard() -> str:
    """Return the single-page dashboard HTML."""

    return """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>集群指标看板</title>
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
      select {
        border-radius: 999px;
        border: 1px solid var(--line);
        font: inherit;
      }

      button {
        padding: 10px 18px;
        background: var(--accent);
        color: #fff;
        cursor: pointer;
      }

      button:hover {
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

      .pager {
        display: flex;
        gap: 10px;
        align-items: center;
        margin: 0 0 16px;
        flex-wrap: wrap;
      }

      .page-indicator {
        color: var(--muted);
        font-size: 14px;
      }

      .table-wrap {
        overflow: auto;
        border-radius: 18px;
        border: 1px solid rgba(101, 89, 74, 0.14);
        background: rgba(255, 255, 255, 0.72);
      }

      table {
        width: 100%;
        min-width: 860px;
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
          <h1>集群指标库</h1>
          <p class="subtitle">展示 metric_points 中最新的 5000 条数据</p>
        </div>
        <div class="controls">
          <label for="page-size-select">Rows / page</label>
          <select id="page-size-select">
            <option value="100" selected>100</option>
            <option value="250">250</option>
            <option value="500">500</option>
            <option value="1000">1000</option>
          </select>
          <button id="refresh-button" type="button">Refresh</button>
        </div>
      </section>

      <p class="status" id="status">Loading data...</p>
      <div class="pager">
        <button id="prev-button" type="button">Previous</button>
        <button id="next-button" type="button">Next</button>
        <span class="page-indicator" id="page-indicator">Page 1 / 0</span>
      </div>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Bucket Time</th>
              <th>Cluster</th>
              <th>Metric</th>
              <th>Value</th>
              <th>Labels</th>
              <th>Source</th>
              <th>Collected At</th>
            </tr>
          </thead>
          <tbody id="metrics-table-body">
            <tr>
              <td class="empty" colspan="7">Loading...</td>
            </tr>
          </tbody>
        </table>
      </div>
    </main>

    <script>
      const statusNode = document.getElementById("status");
      const tableBody = document.getElementById("metrics-table-body");
      const refreshButton = document.getElementById("refresh-button");
      const pageSizeSelect = document.getElementById("page-size-select");
      const prevButton = document.getElementById("prev-button");
      const nextButton = document.getElementById("next-button");
      const pageIndicator = document.getElementById("page-indicator");
      let currentPage = 1;

      function formatCell(value) {
        if (value === null || value === undefined) {
          return "";
        }
        if (typeof value === "object") {
          return JSON.stringify(value);
        }
        return String(value);
      }

      function escapeHtml(value) {
        return value
          .replaceAll("&", "&amp;")
          .replaceAll("<", "&lt;")
          .replaceAll(">", "&gt;")
          .replaceAll('"', "&quot;")
          .replaceAll("'", "&#39;");
      }

      function formatTime(value) {
        if (!value) {
          return "";
        }
        const parsed = new Date(value);
        if (Number.isNaN(parsed.getTime())) {
          return String(value);
        }
        return parsed.toLocaleString();
      }

      function renderRows(rows) {
        if (!rows.length) {
          tableBody.innerHTML = '<tr><td class="empty" colspan="7">No data</td></tr>';
          return;
        }

        tableBody.innerHTML = rows.map((row) => `
          <tr>
            <td>${escapeHtml(formatCell(formatTime(row.bucket_time)))}</td>
            <td>${escapeHtml(formatCell(row.cluster_name))}</td>
            <td>${escapeHtml(formatCell(row.metric_name))}</td>
            <td>${escapeHtml(formatCell(row.metric_value))}</td>
            <td>${escapeHtml(formatCell(row.labels))}</td>
            <td>${escapeHtml(formatCell(row.source_tool))}</td>
            <td>${escapeHtml(formatCell(formatTime(row.collected_at)))}</td>
          </tr>
        `).join("");
      }

      function updatePager(payload) {
        const totalPages = payload.total_pages || 0;
        pageIndicator.textContent = `Page ${payload.page} / ${totalPages}`;
        prevButton.disabled = payload.page <= 1;
        nextButton.disabled = totalPages === 0 || payload.page >= totalPages;
      }

      async function loadRows() {
        const pageSize = pageSizeSelect.value;
        statusNode.textContent = "Refreshing...";
        refreshButton.disabled = true;
        prevButton.disabled = true;
        nextButton.disabled = true;

        try {
          const params = new URLSearchParams({
            page: String(currentPage),
            page_size: String(pageSize),
          });
          const response = await fetch(`/api/v1/metrics/recent?${params.toString()}`);
          if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
          }
          const payload = await response.json();
          if (payload.total_pages > 0 && currentPage > payload.total_pages) {
            currentPage = payload.total_pages;
            await loadRows();
            return;
          }
          renderRows(payload.rows || []);
          updatePager(payload);
          const refreshedAt = new Date().toLocaleTimeString();
          const rangeText = `${payload.start_row}-${payload.end_row}`;
          statusNode.textContent =
            `Showing ${rangeText} of latest ${payload.total_rows} rows at ${refreshedAt}`;
        } catch (error) {
          tableBody.innerHTML = '<tr><td class="empty" colspan="7">Failed to load data</td></tr>';
          pageIndicator.textContent = "Page 1 / 0";
          statusNode.textContent = `Refresh failed: ${error.message}`;
        } finally {
          refreshButton.disabled = false;
        }
      }

      refreshButton.addEventListener("click", loadRows);
      pageSizeSelect.addEventListener("change", () => {
        currentPage = 1;
        loadRows();
      });
      prevButton.addEventListener("click", () => {
        if (currentPage > 1) {
          currentPage -= 1;
          loadRows();
        }
      });
      nextButton.addEventListener("click", () => {
        currentPage += 1;
        loadRows();
      });
      loadRows();
    </script>
  </body>
</html>
"""
