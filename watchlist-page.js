function watchlistEv(row) {
  if (typeof row.expectedValue !== "number") return "--";
  return `${row.expectedValue > 0 ? "+" : ""}$${row.expectedValue.toFixed(2)}`;
}

async function loadWatchlist() {
  const root = document.querySelector("#watchlist-root");
  if (!root) return;
  const response = await fetch("/api/watchlist");
  const payload = await response.json();
  const rows = payload.rows || [];

  root.innerHTML = `
    <header class="mb-12">
      <div class="flex items-end justify-between gap-6">
        <div>
          <div class="flex items-center gap-3 mb-3">
            <span class="text-primary-fixed bg-primary/10 px-3 py-1 rounded-full text-xs uppercase tracking-[0.24em]">Manual Watchlist</span>
            <span class="text-sm text-slate-400">${rows.length} curated contracts</span>
          </div>
          <h1 class="text-5xl font-extrabold tracking-tight">Watchlist</h1>
          <p class="text-lg text-slate-400 mt-4 max-w-2xl">This is our manually curated list for now. Later we can make it account-specific without changing the page contract.</p>
        </div>
        <div class="hidden lg:flex glass-panel rounded-2xl p-6 gap-8">
          <div><div class="text-xs uppercase tracking-[0.2em] text-slate-400">Avg Confidence</div><div class="text-3xl font-black text-primary">${payload.stats?.avgConfidence || 0}%</div></div>
          <div><div class="text-xs uppercase tracking-[0.2em] text-slate-400">Active Signals</div><div class="text-3xl font-black text-secondary">${payload.stats?.activeSignals || 0}</div></div>
        </div>
      </div>
    </header>
    <div class="glass-panel rounded-2xl overflow-hidden shadow-2xl">
      <div class="overflow-x-auto">
        <table class="w-full text-left border-collapse min-w-[1100px]">
          <thead>
            <tr class="bg-surface-container-low/50">
              <th class="px-8 py-5 text-[10px] font-bold uppercase tracking-[0.24em] text-outline">Contract</th>
              <th class="px-6 py-5 text-[10px] font-bold uppercase tracking-[0.24em] text-outline">Location</th>
              <th class="px-6 py-5 text-[10px] font-bold uppercase tracking-[0.24em] text-outline">Model</th>
              <th class="px-6 py-5 text-[10px] font-bold uppercase tracking-[0.24em] text-outline">Kalshi</th>
              <th class="px-6 py-5 text-[10px] font-bold uppercase tracking-[0.24em] text-outline">Confidence</th>
              <th class="px-6 py-5 text-[10px] font-bold uppercase tracking-[0.24em] text-outline">Signal</th>
              <th class="px-6 py-5 text-[10px] font-bold uppercase tracking-[0.24em] text-outline">EV</th>
              <th class="px-8 py-5 text-[10px] font-bold uppercase tracking-[0.24em] text-outline text-right">Action</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-white/5">
            ${rows.length ? rows.map((row) => `
              <tr class="group hover:bg-white/5 transition-colors">
                <td class="px-8 py-6">
                  <div class="font-headline font-bold text-on-surface">${row.contract}</div>
                  <div class="text-xs text-outline mt-2">${row.contractSubtitle || row.eventDate || ""}</div>
                </td>
                <td class="px-6 py-6">${row.location}</td>
                <td class="px-6 py-6 text-primary font-black">${row.modelForecastDisplay}</td>
                <td class="px-6 py-6">${row.kalshiProbDisplay}</td>
                <td class="px-6 py-6"><span class="px-3 py-1 rounded-full text-xs font-bold ${row.confidenceClass}">${row.confidenceLabel}</span></td>
                <td class="px-6 py-6 max-w-[260px] text-sm text-slate-300">${row.signalDriver}</td>
                <td class="px-6 py-6 text-xl font-black ${row.expectedValue > 0 ? "text-primary" : "text-secondary"}">${watchlistEv(row)}</td>
                <td class="px-8 py-6 text-right"><a class="bg-primary/10 hover:bg-primary text-primary hover:text-on-primary-container px-4 py-2 rounded-xl text-xs font-bold transition-all duration-300 inline-block" href="${row.inspectUrl}" target="_blank" rel="noreferrer">Inspect ↗</a></td>
              </tr>
            `).join("") : `
              <tr><td class="px-8 py-12 text-slate-400" colspan="8">No curated contracts are configured yet. Edit /config/watchlist.json to choose the locations or tickers we want to monitor.</td></tr>
            `}
          </tbody>
        </table>
      </div>
    </div>
  `;
}

loadWatchlist().catch((error) => {
  const root = document.querySelector("#watchlist-root");
  if (root) root.innerHTML = `<div class="px-8 py-32 text-center text-secondary">Failed to load watchlist: ${error.message}</div>`;
});
