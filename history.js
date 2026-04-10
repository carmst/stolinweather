function getElement(selector) {
  return document.querySelector(selector);
}

const groupsEl = getElement("#history-groups");
const summaryEl = getElement("#history-summary");
const updatedEl = getElement("#history-updated-time");
const dayCountEl = getElement("#history-day-count");

function setHtml(element, html) {
  if (element) {
    element.innerHTML = html;
  }
}

function formatTemp(value) {
  return typeof value === "number" ? `${value.toFixed(1)}F` : "--";
}

function formatDelta(forecast, actual) {
  if (typeof forecast !== "number" || typeof actual !== "number") {
    return "--";
  }
  const delta = forecast - actual;
  const sign = delta > 0 ? "+" : "";
  return `${sign}${delta.toFixed(1)}F`;
}

function formatActualCell(row) {
  const source = row.actualSource;
  const sourceBadge =
    source === "official"
      ? '<span class="history-source-badge is-official">official</span>'
      : source === "preliminary"
        ? '<span class="history-source-badge is-preliminary">preliminary</span>'
        : '<span class="history-source-badge">unknown</span>';

  return `
    <div class="history-actual-cell">
      <span>${formatTemp(row.actualHighF)}</span>
      ${source ? sourceBadge : ""}
    </div>
  `;
}

function renderGroups(payload) {
  const markets = payload.markets || [];
  if (!markets.length) {
    setHtml(
      groupsEl,
      `<div class="empty-history">No recent history rows were available for the selected window.</div>`
    );
    return;
  }

  setHtml(
    groupsEl,
    markets
      .map(
        (market) => `
          <section class="history-market-card">
            <div class="history-market-header">
              <h2>${market.location}</h2>
              <span class="history-market-meta">${market.rows.length} days</span>
            </div>
            <div class="table-wrap">
              <table class="history-table">
                <thead>
                  <tr>
                    <th>Date</th>
                    <th>NOAA</th>
                    <th>Open-Meteo</th>
                    <th>Visual Crossing</th>
                    <th>Model</th>
                    <th>Actual</th>
                    <th>Model Error</th>
                  </tr>
                </thead>
                <tbody>
                  ${market.rows
                    .map(
                      (row) => `
                        <tr>
                          <td class="mono">${row.date}</td>
                          <td>${formatTemp(row.noaaHighF)}</td>
                          <td>${formatTemp(row.openMeteoHighF)}</td>
                          <td>${formatTemp(row.visualCrossingHighF)}</td>
                          <td class="positive">${formatTemp(row.modelHighF)}</td>
                          <td class="mono">${formatActualCell(row)}</td>
                          <td class="${typeof row.modelHighF === "number" && typeof row.actualHighF === "number" ? (row.modelHighF - row.actualHighF > 0 ? "negative" : "positive") : ""}">${formatDelta(row.modelHighF, row.actualHighF)}</td>
                        </tr>
                      `
                    )
                    .join("")}
                </tbody>
              </table>
            </div>
          </section>
        `
      )
      .join("")
  );
}

async function loadHistory() {
  const dayCount = Number.parseInt(dayCountEl?.value || "5", 10);
  const response = await fetch(`/api/history?days=${dayCount}`);
  const payload = await response.json();

  const updatedAt = new Date(payload.updatedAt);
  if (updatedEl && !Number.isNaN(updatedAt.getTime())) {
    updatedEl.textContent = `${String(updatedAt.getUTCHours()).padStart(2, "0")}:${String(
      updatedAt.getUTCMinutes()
    ).padStart(2, "0")}`;
  }

  if (summaryEl) {
    summaryEl.textContent = `${payload.rows?.length || 0} market-day rows from ${payload.dataBackend}.`;
  }

  renderGroups(payload);
}

dayCountEl?.addEventListener("change", loadHistory);

loadHistory().catch((error) => {
  setHtml(groupsEl, `<div class="empty-history">Failed to load history: ${error.message}</div>`);
});
