function formatHistoryTemp(value) {
  return typeof value === "number" ? `${value.toFixed(1)}F` : "--";
}

function formatHistoryDelta(forecast, actual) {
  if (typeof forecast !== "number" || typeof actual !== "number") return "--";
  const delta = forecast - actual;
  return `${delta > 0 ? "+" : ""}${delta.toFixed(1)}F`;
}

function actualBadge(source) {
  if (source === "official") {
    return '<span class="ml-3 px-3 py-1 rounded-full bg-emerald-500/15 text-emerald-300 text-xs font-bold uppercase tracking-[0.2em]">Official</span>';
  }
  if (source === "preliminary") {
    return '<span class="ml-3 px-3 py-1 rounded-full bg-amber-500/15 text-amber-300 text-xs font-bold uppercase tracking-[0.2em]">Preliminary</span>';
  }
  return "";
}

async function loadNewHistory() {
  const root = document.querySelector("#history-root");
  if (!root) return;
  const select = document.querySelector("#history-day-select");
  const days = select ? Number(select.value) : 5;
  const response = await fetch(`/api/history?days=${days}`);
  const payload = await response.json();

  root.innerHTML = `
    <section class="mb-12">
      <div class="flex flex-col md:flex-row md:items-end md:justify-between gap-6">
        <div>
          <p class="text-xs uppercase tracking-[0.28em] text-primary mb-4">Morning checkpoint review</p>
          <h1 class="text-5xl md:text-7xl font-black tracking-tighter mb-5">How the forecast performed at the start of the day.</h1>
          <p class="text-xl text-slate-400 max-w-3xl leading-relaxed">Each row shows the 8 AM local forecast snapshot by provider, our blended model, and the eventual actual high using official data when available and preliminary intraday max otherwise.</p>
        </div>
        <div class="flex items-center gap-3">
          <span class="text-xs uppercase tracking-[0.24em] text-slate-400">Days</span>
          <select id="history-day-select" class="bg-surface-container-high rounded-xl border border-white/10 px-4 py-3">
            <option value="3" ${days === 3 ? "selected" : ""}>3 days</option>
            <option value="5" ${days === 5 ? "selected" : ""}>5 days</option>
            <option value="7" ${days === 7 ? "selected" : ""}>7 days</option>
          </select>
        </div>
      </div>
      <div class="mt-6 text-slate-400">${payload.rows?.length || 0} market-day rows from ${payload.dataBackend}.</div>
    </section>
    <section class="space-y-8">
      ${(payload.markets || []).map((market) => `
        <article class="glass-panel rounded-[2rem] border border-white/5 overflow-hidden">
          <div class="px-8 py-6 flex items-center justify-between border-b border-white/5">
            <h2 class="text-3xl font-black">${market.location}</h2>
            <span class="text-slate-400 text-lg">${market.rows.length} days</span>
          </div>
          <div class="overflow-x-auto">
            <table class="w-full min-w-[980px]">
              <thead class="bg-surface-container-low/50">
                <tr>
                  <th class="px-6 py-4 text-left text-xs uppercase tracking-[0.24em] text-slate-400">Date</th>
                  <th class="px-6 py-4 text-left text-xs uppercase tracking-[0.24em] text-slate-400">NOAA</th>
                  <th class="px-6 py-4 text-left text-xs uppercase tracking-[0.24em] text-slate-400">Open-Meteo</th>
                  <th class="px-6 py-4 text-left text-xs uppercase tracking-[0.24em] text-slate-400">Visual Crossing</th>
                  <th class="px-6 py-4 text-left text-xs uppercase tracking-[0.24em] text-slate-400">Model</th>
                  <th class="px-6 py-4 text-left text-xs uppercase tracking-[0.24em] text-slate-400">Actual</th>
                  <th class="px-6 py-4 text-left text-xs uppercase tracking-[0.24em] text-slate-400">Model Error</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-white/5">
                ${market.rows.map((row) => `
                  <tr>
                    <td class="px-6 py-5 font-mono text-xl">${row.date}</td>
                    <td class="px-6 py-5 text-xl">${formatHistoryTemp(row.noaaHighF)}</td>
                    <td class="px-6 py-5 text-xl">${formatHistoryTemp(row.openMeteoHighF)}</td>
                    <td class="px-6 py-5 text-xl">${formatHistoryTemp(row.visualCrossingHighF)}</td>
                    <td class="px-6 py-5 text-xl font-bold text-primary">${formatHistoryTemp(row.modelHighF)}</td>
                    <td class="px-6 py-5 text-xl">${formatHistoryTemp(row.actualHighF)} ${actualBadge(row.actualSource)}</td>
                    <td class="px-6 py-5 text-xl font-bold ${(typeof row.modelHighF === "number" && typeof row.actualHighF === "number" && row.modelHighF - row.actualHighF > 0) ? "text-secondary" : "text-primary"}">${formatHistoryDelta(row.modelHighF, row.actualHighF)}</td>
                  </tr>
                `).join("")}
              </tbody>
            </table>
          </div>
        </article>
      `).join("")}
    </section>
  `;

  const nextSelect = document.querySelector("#history-day-select");
  if (nextSelect) {
    nextSelect.addEventListener("change", loadNewHistory, { once: true });
  }
}

loadNewHistory().catch((error) => {
  const root = document.querySelector("#history-root");
  if (root) root.innerHTML = `<div class="px-8 py-32 text-center text-secondary">Failed to load history: ${error.message}</div>`;
});
