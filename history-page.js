function formatHistoryTemp(value) {
  return typeof value === "number" ? `${value.toFixed(1)}F` : "--";
}

function formatHistoryDelta(forecast, actual) {
  if (typeof forecast !== "number" || typeof actual !== "number") return "--";
  const delta = forecast - actual;
  return `${delta > 0 ? "+" : ""}${delta.toFixed(1)}F`;
}

function sourceBadge(source) {
  if (source === "official") {
    return `<span class="font-label text-[10px] text-slate-500 px-2 py-1 border border-outline-variant rounded">OFFICIAL</span>`;
  }
  if (source === "preliminary") {
    return `<span class="font-label text-[10px] text-amber-300 px-2 py-1 border border-amber-500/30 rounded">PRELIMINARY</span>`;
  }
  return "";
}

function buildAccuracyBars(rows) {
  const values = rows
    .map((row) => (typeof row.modelHighF === "number" && typeof row.actualHighF === "number" ? Math.abs(row.modelHighF - row.actualHighF) : null))
    .filter((value) => typeof value === "number");
  if (!values.length) {
    return new Array(6).fill(35);
  }
  const max = Math.max(...values, 1);
  return values.slice(0, 6).map((value) => Math.max(20, Math.round((1 - value / max) * 100))).concat(new Array(Math.max(0, 6 - values.length)).fill(30));
}

function buildSettlementCard(row) {
  const outcome =
    typeof row.modelHighF === "number" && typeof row.actualHighF === "number"
      ? row.modelHighF >= row.actualHighF
        ? "Model ran hot"
        : "Model ran cool"
      : "Awaiting settled comparison";
  const outcomeClass =
    typeof row.modelHighF === "number" && typeof row.actualHighF === "number"
      ? row.modelHighF >= row.actualHighF
        ? "text-secondary"
        : "text-green-400"
      : "text-slate-400";

  return `
    <div class="surface-container-high rounded-2xl p-6 transition-all hover:scale-[1.02] cursor-pointer group">
      <div class="flex justify-between items-start mb-6">
        <div class="bg-primary/10 p-2 rounded-lg text-primary">
          <span class="material-symbols-outlined">thermostat</span>
        </div>
        ${sourceBadge(row.actualSource)}
      </div>
      <h4 class="font-headline font-bold text-lg leading-tight mb-2">${row.location}</h4>
      <p class="text-slate-400 text-sm mb-4">${row.date}</p>
      <div class="space-y-3">
        <div class="flex justify-between text-xs font-label">
          <span class="text-slate-500">Morning Model</span>
          <span class="text-primary font-bold">${formatHistoryTemp(row.modelHighF)}</span>
        </div>
        <div class="flex justify-between text-xs font-label">
          <span class="text-slate-500">Actual High</span>
          <span class="text-on-surface">${formatHistoryTemp(row.actualHighF)}</span>
        </div>
        <div class="pt-4 border-t border-white/5 flex items-center gap-2">
          <span class="material-symbols-outlined ${outcomeClass} text-sm">check_circle</span>
          <span class="text-xs font-bold ${outcomeClass}">OUTCOME: ${outcome} (${formatHistoryDelta(row.modelHighF, row.actualHighF)})</span>
        </div>
      </div>
    </div>
  `;
}

async function loadHistoryPage() {
  const root = document.querySelector("#history-root");
  if (!root) return;

  const existingSelect = document.querySelector("#history-days");
  const dayCount = existingSelect ? Number(existingSelect.value) : 5;
  const response = await fetch(`/api/history?days=${dayCount}`);
  const payload = await response.json();
  const rows = payload.rows || [];
  const settledRows = rows.filter((row) => typeof row.actualHighF === "number");
  const exactMatches = settledRows.filter((row) => typeof row.modelHighF === "number" && Math.abs(row.modelHighF - row.actualHighF) <= 2).length;
  const meanAbsError =
    settledRows.length > 0
      ? settledRows.reduce((sum, row) => sum + Math.abs((row.modelHighF || 0) - row.actualHighF), 0) / settledRows.length
      : null;
  const avgAlpha =
    settledRows.length > 0
      ? settledRows.reduce((sum, row) => sum + Math.abs((row.noaaHighF || row.modelHighF || 0) - (row.modelHighF || 0)), 0) / settledRows.length
      : null;
  const accuracyBars = buildAccuracyBars(settledRows);
  const cardRows = settledRows.slice(0, 4);

  root.innerHTML = `
    <section class="relative mb-16 rounded-3xl overflow-hidden min-h-[400px] flex flex-col justify-end p-12">
      <div class="absolute inset-0 z-0">
        <div class="w-full h-full bg-[radial-gradient(circle_at_20%_20%,rgba(109,235,253,0.15),transparent_20%),radial-gradient(circle_at_80%_30%,rgba(255,115,72,0.12),transparent_25%),linear-gradient(160deg,#0b1317,#070f12)] opacity-90"></div>
        <div class="absolute inset-0 bg-gradient-to-t from-background via-background/60 to-transparent"></div>
      </div>
      <div class="relative z-10 grid grid-cols-1 md:grid-cols-3 gap-8">
        <div class="glass-panel p-8 rounded-2xl border border-white/5">
          <div class="flex items-center gap-3 mb-2">
            <span class="material-symbols-outlined text-primary">analytics</span>
            <span class="font-label text-xs uppercase tracking-widest text-slate-400">Settled Market-Days</span>
          </div>
          <div class="text-5xl font-black font-headline text-on-surface text-glow">${settledRows.length}</div>
          <div class="mt-2 text-primary text-sm font-label">${payload.dataBackend}</div>
        </div>
        <div class="glass-panel p-8 rounded-2xl border border-white/5">
          <div class="flex items-center gap-3 mb-2">
            <span class="material-symbols-outlined text-secondary">verified</span>
            <span class="font-label text-xs uppercase tracking-widest text-slate-400">Within 2°F</span>
          </div>
          <div class="text-5xl font-black font-headline text-on-surface text-glow">${settledRows.length ? `${Math.round((exactMatches / settledRows.length) * 100)}%` : "--"}</div>
          <div class="mt-2 text-secondary text-sm font-label">Morning checkpoint hit rate</div>
        </div>
        <div class="glass-panel p-8 rounded-2xl border border-white/5">
          <div class="flex items-center gap-3 mb-2">
            <span class="material-symbols-outlined text-primary">trending_up</span>
            <span class="font-label text-xs uppercase tracking-widest text-slate-400">Average NOAA Gap</span>
          </div>
          <div class="text-5xl font-black font-headline text-on-surface text-glow">${avgAlpha == null ? "--" : `${avgAlpha.toFixed(1)}F`}</div>
          <div class="mt-2 text-primary text-sm font-label">Model vs baseline spread</div>
        </div>
      </div>
    </section>
    <section class="grid grid-cols-1 lg:grid-cols-12 gap-8 mb-16">
      <div class="lg:col-span-3 space-y-6">
        <div class="glass-panel p-6 rounded-2xl">
          <h3 class="font-headline font-bold text-xl mb-6">Refine Data</h3>
          <div class="space-y-4">
            <div>
              <label class="block font-label text-xs text-slate-500 uppercase tracking-tighter mb-2">Checkpoint</label>
              <div class="space-y-2">
                <label class="flex items-center gap-3 cursor-pointer group">
                  <input checked class="rounded bg-surface border-outline-variant text-primary focus:ring-primary" type="checkbox" />
                  <span class="text-sm group-hover:text-primary transition-colors">8 AM local forecast</span>
                </label>
                <label class="flex items-center gap-3 cursor-pointer group">
                  <input checked class="rounded bg-surface border-outline-variant text-primary focus:ring-primary" type="checkbox" />
                  <span class="text-sm group-hover:text-primary transition-colors">Official actuals when available</span>
                </label>
                <label class="flex items-center gap-3 cursor-pointer group">
                  <input checked class="rounded bg-surface border-outline-variant text-primary focus:ring-primary" type="checkbox" />
                  <span class="text-sm group-hover:text-primary transition-colors">Preliminary intraday fallback</span>
                </label>
              </div>
            </div>
            <div>
              <label class="block font-label text-xs text-slate-500 uppercase tracking-tighter mb-2">Date Range</label>
              <select id="history-days" class="w-full bg-surface border-none rounded-xl text-sm p-3 focus:ring-2 focus:ring-primary/40 appearance-none">
                <option value="3" ${dayCount === 3 ? "selected" : ""}>Last 3 Days</option>
                <option value="5" ${dayCount === 5 ? "selected" : ""}>Last 5 Days</option>
                <option value="7" ${dayCount === 7 ? "selected" : ""}>Last 7 Days</option>
              </select>
            </div>
            <button class="w-full py-4 bg-gradient-to-r from-primary to-primary-container text-on-primary font-bold rounded-xl active:scale-95 transition-all mt-4">
              Apply Filters
            </button>
          </div>
        </div>
      </div>
      <div class="lg:col-span-9 glass-panel p-8 rounded-2xl relative overflow-hidden">
        <div class="flex justify-between items-start mb-8">
          <div>
            <h3 class="font-headline font-bold text-2xl">Model Accuracy Over Time</h3>
            <p class="text-slate-400 text-sm">Morning checkpoint model error against settled highs</p>
          </div>
          <div class="flex gap-4">
            <div class="flex items-center gap-2">
              <span class="w-3 h-3 rounded-full bg-primary"></span>
              <span class="text-xs font-label">Checkpoint Model</span>
            </div>
            <div class="flex items-center gap-2">
              <span class="w-3 h-3 rounded-full bg-slate-600"></span>
              <span class="text-xs font-label">Actual High</span>
            </div>
          </div>
        </div>
        <div class="h-64 flex items-end gap-1 relative pt-10">
          <div class="absolute inset-0 border-b border-white/5 flex flex-col justify-between py-2">
            <div class="border-t border-white/5 w-full"></div>
            <div class="border-t border-white/5 w-full"></div>
            <div class="border-t border-white/5 w-full"></div>
          </div>
          <div class="absolute inset-0 w-full h-full pointer-events-none flex items-end gap-1 pb-3">
            ${accuracyBars
              .map(
                (value) => `
                  <div class="flex-1 h-full flex items-end">
                    <div class="w-full bg-primary/20 rounded-sm" style="height:${value}%"></div>
                  </div>
                `
              )
              .join("")}
          </div>
        </div>
        <div class="flex justify-between mt-4 font-label text-[10px] text-slate-500 uppercase tracking-widest">
          <span>${payload.dataBackend}</span>
          <span>${meanAbsError == null ? "MAE --" : `MAE ${meanAbsError.toFixed(1)}F`}</span>
          <span>${rows.length} rows</span>
        </div>
      </div>
    </section>
    <section class="mb-16">
      <div class="flex justify-between items-end mb-8">
        <div>
          <h2 class="font-headline font-extrabold text-3xl tracking-tight">Recent Settlements</h2>
          <p class="text-slate-400">Morning model checkpoints compared to official and preliminary actual highs.</p>
        </div>
      </div>
      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        ${cardRows.map((row) => buildSettlementCard(row)).join("")}
      </div>
    </section>
    <section class="glass-panel rounded-3xl p-12 text-center relative overflow-hidden">
      <div class="relative z-10">
        <h2 class="text-3xl md:text-5xl font-black font-headline mb-4">Keep the checkpoint honest.</h2>
        <p class="text-slate-400 max-w-2xl mx-auto mb-8 font-label">We grade the model from a fixed morning forecast, not from an intraday hindsight forecast, so the history page matches how we actually make decisions.</p>
        <div class="flex flex-col md:flex-row gap-4 justify-center">
          <a class="px-8 py-4 bg-primary text-on-primary font-bold rounded-xl active:scale-95 transition-all" href="/marketplace.html">Open Marketplace</a>
          <a class="px-8 py-4 bg-surface-bright/50 backdrop-blur-md text-on-surface font-bold rounded-xl active:scale-95 transition-all" href="/methodology.html">View Methodology</a>
        </div>
      </div>
    </section>
  `;

  const nextSelect = document.querySelector("#history-days");
  if (nextSelect) {
    nextSelect.addEventListener("change", loadHistoryPage, { once: true });
  }
}

loadHistoryPage().catch((error) => {
  const root = document.querySelector("#history-root");
  if (root) {
    root.innerHTML = `<div class="px-8 py-24 text-center text-secondary">Failed to load history: ${error.message}</div>`;
  }
});
