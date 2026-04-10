function money(value) {
  return typeof value === "number" ? `$${value.toFixed(2)}` : "--";
}

function temp(value) {
  return typeof value === "number" ? `${Math.round(value)}F` : "--";
}

function pct(value) {
  return typeof value === "number" ? `${Math.round(value * 100)}%` : "--";
}

function ev(row) {
  if (typeof row.expectedValue !== "number") return "--";
  const sign = row.expectedValue > 0 ? "+" : "";
  return `${sign}$${row.expectedValue.toFixed(2)}`;
}

async function loadMarketplace() {
  const root = document.querySelector("#marketplace-root");
  if (!root) return;
  const response = await fetch("/api/dashboard");
  const dashboard = await response.json();
  const allContracts = dashboard.contractViews?.allContracts || dashboard.contracts || [];

  root.innerHTML = `
    <section class="relative min-h-[38vh] flex items-end pb-16">
      <div class="absolute inset-0 bg-[radial-gradient(circle_at_15%_20%,rgba(109,235,253,0.18),transparent_25%),radial-gradient(circle_at_80%_30%,rgba(255,115,72,0.12),transparent_22%),linear-gradient(180deg,#081114_0%,#070f12_72%)]"></div>
      <div class="relative z-10 px-8 max-w-7xl mx-auto w-full">
        <p class="text-xs uppercase tracking-[0.28em] text-primary mb-4">Marketplace</p>
        <h1 class="text-5xl md:text-7xl font-black tracking-tighter mb-6">Live weather contracts with model context.</h1>
        <p class="text-xl text-slate-400 max-w-3xl leading-relaxed">We’re showing the latest tradable board, with live model forecast, Kalshi pricing, side-aware EV, and the signal that justifies the trade.</p>
      </div>
    </section>
    <section class="px-8 max-w-7xl mx-auto pb-20">
      <div class="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
        ${(dashboard.metrics || []).map((metric) => `
          <article class="glass-panel rounded-2xl border border-white/5 p-5">
            <div class="text-xs uppercase tracking-[0.24em] text-slate-400 mb-2">${metric.eyebrow}</div>
            <div class="text-3xl font-black ${metric.valueClass || ""}">${metric.value}</div>
            <div class="text-sm text-slate-400 mt-2">${metric.subtle}</div>
          </article>
        `).join("")}
      </div>
      <div class="glass-panel rounded-[2rem] border border-white/5 overflow-hidden">
        <div class="overflow-x-auto">
          <table class="w-full text-left min-w-[1300px]">
            <thead class="bg-surface-container-low/70">
              <tr>
                <th class="px-6 py-4 text-xs uppercase tracking-[0.24em] text-slate-400">Contract</th>
                <th class="px-6 py-4 text-xs uppercase tracking-[0.24em] text-slate-400">Location</th>
                <th class="px-6 py-4 text-xs uppercase tracking-[0.24em] text-slate-400">NOAA High</th>
                <th class="px-6 py-4 text-xs uppercase tracking-[0.24em] text-slate-400">Model Forecast</th>
                <th class="px-6 py-4 text-xs uppercase tracking-[0.24em] text-slate-400">Confidence</th>
                <th class="px-6 py-4 text-xs uppercase tracking-[0.24em] text-slate-400">Kalshi %</th>
                <th class="px-6 py-4 text-xs uppercase tracking-[0.24em] text-slate-400">Cost</th>
                <th class="px-6 py-4 text-xs uppercase tracking-[0.24em] text-slate-400">Model Signal</th>
                <th class="px-6 py-4 text-xs uppercase tracking-[0.24em] text-slate-400">EV</th>
                <th class="px-6 py-4 text-xs uppercase tracking-[0.24em] text-slate-400">Action</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-white/5">
              ${allContracts.map((row) => `
                <tr class="hover:bg-white/5 transition-colors">
                  <td class="px-6 py-5 align-top">
                    <div class="text-3xl font-black ${row.setupClass || ""}">${row.contract}</div>
                    <div class="text-slate-400 mt-2">${row.contractSubtitle || ""}</div>
                    <div class="mt-3"><span class="px-3 py-1 rounded-full text-xs font-bold ${row.setupClass || ""}">${row.setupLabel || ""}</span></div>
                  </td>
                  <td class="px-6 py-5 align-top text-xl font-semibold">${row.location}</td>
                  <td class="px-6 py-5 align-top text-xl">${temp(row.noaaForecastMaxF)}</td>
                  <td class="px-6 py-5 align-top text-xl text-primary font-bold">${row.modelForecastDisplay}</td>
                  <td class="px-6 py-5 align-top"><span class="px-3 py-2 rounded-full text-xs font-bold ${row.confidenceClass}">${row.confidenceLabel}</span></td>
                  <td class="px-6 py-5 align-top text-xl">${row.kalshiProbDisplay}</td>
                  <td class="px-6 py-5 align-top text-xl">${row.contractCostDisplay || "--"}</td>
                  <td class="px-6 py-5 align-top max-w-[260px] leading-tight text-slate-300">${row.signalDriver}</td>
                  <td class="px-6 py-5 align-top text-2xl font-black ${row.expectedValue > 0 ? "text-primary" : "text-secondary"}">${ev(row)}</td>
                  <td class="px-6 py-5 align-top"><a class="text-secondary font-bold hover:text-secondary-fixed-dim transition-colors" href="${row.inspectUrl}" target="_blank" rel="noreferrer">Inspect ↗</a></td>
                </tr>
              `).join("")}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  `;
}

loadMarketplace().catch((error) => {
  const root = document.querySelector("#marketplace-root");
  if (root) root.innerHTML = `<div class="px-8 py-32 text-center text-secondary">Failed to load marketplace: ${error.message}</div>`;
});
