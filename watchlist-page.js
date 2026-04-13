function formatEv(row) {
  if (typeof row.expectedValue !== "number") return "--";
  return `${row.expectedValue > 0 ? "+" : ""}$${row.expectedValue.toFixed(2)}`;
}

function formatPercent(value) {
  return typeof value === "number" ? `${Math.round(value * 100)}%` : "--";
}

function formatOdds(row) {
  if (typeof row.contractCost === "number") {
    return `${Math.round(row.contractCost * 100)}¢`;
  }
  if (typeof row.kalshiProb === "number") {
    return `${Math.round(row.kalshiProb * 100)}¢`;
  }
  return "--";
}

function evTrend(row) {
  const ev = typeof row.expectedValue === "number" ? row.expectedValue : 0;
  if (ev > 0.03) {
    return {
      icon: "trending_up",
      label: `${formatEv(row)} EV`,
      className: "text-primary",
    };
  }
  if (ev < -0.03) {
    return {
      icon: "trending_down",
      label: `${formatEv(row)} EV`,
      className: "text-error",
    };
  }
  return {
    icon: "horizontal_rule",
    label: `${formatEv(row)} EV`,
    className: "text-outline",
  };
}

function marketIcon(row) {
  const text = `${row.contract || ""} ${row.contractSubtitle || ""}`.toLowerCase();
  if (text.includes("low")) return { icon: "ac_unit", className: "text-primary" };
  if (text.includes("rain") || text.includes("precip")) return { icon: "rainy", className: "text-secondary" };
  if (text.includes("wind")) return { icon: "air", className: "text-white/50" };
  return { icon: "thermostat", className: "text-secondary" };
}

function signalCopy(row) {
  if (
    typeof row.hourlyPathViolationHours === "number" &&
    typeof row.hourlyPathHours === "number" &&
    row.hourlyPathViolationHours > 0
  ) {
    return `${row.hourlyPathViolationHours}/${row.hourlyPathHours} forecast hours violate the YES bucket.`;
  }
  if (row.signalDriver) return row.signalDriver;
  if ((row.recommendedSide || "").toUpperCase() === "YES") return "Model currently sees YES edge.";
  if ((row.recommendedSide || "").toUpperCase() === "PASS") return "Model does not currently see YES edge.";
  return "Signal is still forming.";
}

function modelHighCopy(row) {
  if (typeof row.modelHighF === "number") {
    return `Model high ${row.modelHighF.toFixed(1)}F`;
  }
  if (typeof row.adjustedForecastMaxF === "number") {
    return `Model high ${row.adjustedForecastMaxF.toFixed(1)}F`;
  }
  if (row.modelForecastDisplay) return row.modelForecastDisplay;
  return "Model high pending";
}

function buildDetailUrl(row) {
  const identifier = row.ticker || row.eventTicker || row.marketId || "";
  return `/market-detail.html?ticker=${encodeURIComponent(identifier)}`;
}

function buildWatchlistRow(row) {
  const trend = evTrend(row);
  const icon = marketIcon(row);
  const modelPct = typeof row.modelProb === "number" ? row.modelProb : row.winProbability;
  const progressWidth = Math.min(100, Math.max(0, Math.round((modelPct || 0) * 100)));

  return `
    <tr class="group hover:bg-white/5 transition-colors">
      <td class="px-8 py-6">
        <div class="flex items-center gap-4">
          <div class="w-10 h-10 rounded-lg bg-surface-container-highest flex items-center justify-center ${icon.className}">
            <span class="material-symbols-outlined">${icon.icon}</span>
          </div>
          <div>
            <span class="block font-headline font-bold text-on-surface">${row.contract}</span>
            <span class="text-xs text-outline font-label">${row.location} | ${modelHighCopy(row)}</span>
          </div>
        </div>
      </td>
      <td class="px-6 py-6">
        <div class="flex flex-col">
          <span class="text-lg font-headline font-extrabold text-on-surface">${formatOdds(row)}</span>
          <span class="text-[10px] font-label text-outline uppercase tracking-tighter">Implied Prob: ${row.kalshiProbDisplay || formatPercent(row.kalshiProb)}</span>
        </div>
      </td>
      <td class="px-6 py-6">
        <div class="flex flex-col items-center">
          <span class="text-xl font-headline font-black ${row.expectedValue > 0 ? "text-secondary" : "text-primary"}">${formatPercent(modelPct)}</span>
          <span class="text-[10px] font-label text-outline uppercase tracking-tighter">YES probability</span>
          <span class="text-[10px] font-label text-outline uppercase tracking-tighter">${(row.recommendedSide || "").toUpperCase() === "YES" ? "Execute YES" : "Inspect YES"}</span>
          <div class="w-24 h-1 bg-surface-container-highest rounded-full mt-2 overflow-hidden">
            <div class="h-full ${row.expectedValue > 0 ? "bg-secondary" : "bg-primary"}" style="width:${progressWidth}%"></div>
          </div>
        </div>
      </td>
      <td class="px-6 py-6">
        <div class="flex items-center gap-2 ${trend.className}">
          <span class="material-symbols-outlined text-sm">${trend.icon}</span>
          <span class="text-xs font-label font-semibold">${trend.label}</span>
        </div>
        <p class="text-[11px] text-outline mt-2 max-w-[240px]">${signalCopy(row)}</p>
      </td>
      <td class="px-8 py-6 text-right">
        <div class="flex flex-col items-end gap-2">
          <a class="bg-primary/10 hover:bg-primary text-primary hover:text-on-primary-container px-4 py-2 rounded-xl text-xs font-label font-bold transition-all duration-300 transform active:scale-95 inline-block" href="${buildDetailUrl(row)}">
            Forecast Detail
          </a>
          <a class="text-[10px] font-label uppercase tracking-widest text-outline hover:text-secondary transition-colors" href="${row.inspectUrl}" target="_blank" rel="noreferrer">
            Kalshi
          </a>
        </div>
      </td>
    </tr>
  `;
}

function renderWatchlist(root, payload) {
  const rows = payload.rows || [];
  const avgConfidence = payload.stats?.avgConfidence || 0;
  const activeSignals = payload.stats?.activeSignals || rows.length;

  root.innerHTML = `
    <header class="mb-12">
      <div class="flex items-end justify-between">
        <div>
          <div class="flex items-center gap-3 mb-2">
            <span class="text-primary-fixed bg-primary/10 px-3 py-1 rounded-full text-xs font-label tracking-widest uppercase">Live Surveillance</span>
            <span class="w-2 h-2 rounded-full bg-secondary animate-pulse"></span>
          </div>
          <h1 class="text-5xl font-headline font-extrabold tracking-tight text-on-surface">Watchlist</h1>
          <p class="text-on-surface-variant mt-4 max-w-xl font-body text-lg leading-relaxed">
            Tracking the contract whose integer floor bucket contains our one-decimal model high for each of the 20 temperature markets.
          </p>
        </div>
        <div class="hidden lg:block">
          <div class="glass-panel p-6 rounded-xl flex items-center gap-6">
            <div class="text-right">
              <span class="block text-[10px] uppercase tracking-tighter text-outline font-label">Avg Confidence</span>
              <span class="text-2xl font-headline font-bold text-primary">${avgConfidence}%</span>
            </div>
            <div class="h-10 w-[1px] bg-outline-variant"></div>
            <div class="text-right">
              <span class="block text-[10px] uppercase tracking-tighter text-outline font-label">Active Signals</span>
              <span class="text-2xl font-headline font-bold text-secondary">${activeSignals}</span>
            </div>
          </div>
        </div>
      </div>
    </header>
    <div class="mb-8 flex flex-wrap gap-4 items-center justify-between">
      <div class="flex gap-2">
        <button class="bg-surface-container-highest px-6 py-2 rounded-full text-sm font-label font-medium text-primary shadow-lg shadow-primary/5">All Markets</button>
        <button class="hover:bg-surface-container-high px-6 py-2 rounded-full text-sm font-label font-medium text-on-surface-variant transition-colors">Temperature</button>
        <button class="hover:bg-surface-container-high px-6 py-2 rounded-full text-sm font-label font-medium text-on-surface-variant transition-colors">Model High Match</button>
      </div>
      <div class="relative group">
        <span class="material-symbols-outlined absolute left-3 top-1/2 -translate-y-1/2 text-outline text-sm">filter_list</span>
        <select class="bg-transparent border-none focus:ring-0 text-sm font-label text-on-surface-variant pl-10 cursor-pointer">
          <option>Sort by: EV</option>
          <option>Sort by: Confidence</option>
          <option>Sort by: Expiry</option>
        </select>
      </div>
    </div>
    <div class="glass-panel rounded-2xl overflow-hidden shadow-2xl">
      <div class="overflow-x-auto">
        <table class="w-full text-left border-collapse">
          <thead>
            <tr class="bg-surface-container-low/50">
              <th class="px-8 py-5 text-[10px] font-label font-bold uppercase tracking-widest text-outline">Market Name</th>
              <th class="px-6 py-5 text-[10px] font-label font-bold uppercase tracking-widest text-outline">Current Odds</th>
              <th class="px-6 py-5 text-[10px] font-label font-bold uppercase tracking-widest text-outline text-center">YES Probability</th>
              <th class="px-6 py-5 text-[10px] font-label font-bold uppercase tracking-widest text-outline">EV</th>
              <th class="px-8 py-5 text-[10px] font-label font-bold uppercase tracking-widest text-outline text-right">Actions</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-white/5">
            ${rows.length ? rows.map(buildWatchlistRow).join("") : `
              <tr>
                <td class="px-8 py-12 text-slate-400" colspan="5">No model-matched contracts are available yet. Refresh the forecast scoring pipeline and try again.</td>
              </tr>
            `}
          </tbody>
        </table>
      </div>
      <div class="p-8 bg-surface-container-low/30 flex justify-between items-center border-t border-white/5">
        <span class="text-xs font-label text-outline">Showing ${rows.length} model-high matched contracts</span>
        <div class="flex gap-4">
          <button class="text-on-surface-variant hover:text-primary transition-colors">
            <span class="material-symbols-outlined">chevron_left</span>
          </button>
          <button class="text-on-surface-variant hover:text-primary transition-colors">
            <span class="material-symbols-outlined">chevron_right</span>
          </button>
        </div>
      </div>
    </div>
    <div class="mt-20 grid grid-cols-1 md:grid-cols-3 gap-8">
      <div class="glass-panel p-8 rounded-3xl col-span-1 md:col-span-2 relative overflow-hidden group">
        <div class="relative z-10">
          <h3 class="text-2xl font-headline font-bold mb-4">EV Surveillance</h3>
          <p class="text-on-surface-variant font-body mb-6 leading-relaxed">
            We are watching one contract per city: the contract whose strike or range contains the floored one-decimal model high. The EV column shows whether that exact matched contract is positive, negative, or flat.
          </p>
          <a class="flex items-center gap-2 text-secondary font-label font-bold hover:gap-4 transition-all" href="/marketplace.html">
            Open Full Marketplace <span class="material-symbols-outlined">arrow_forward</span>
          </a>
        </div>
        <div class="absolute -right-20 -bottom-20 w-80 h-80 bg-secondary/10 rounded-full blur-[80px] group-hover:bg-secondary/20 transition-all"></div>
      </div>
      <div class="bg-surface-container-high p-8 rounded-3xl relative overflow-hidden flex flex-col justify-between">
        <div>
          <span class="material-symbols-outlined text-primary text-4xl mb-4" style="font-variation-settings: 'FILL' 1;">electric_bolt</span>
          <h4 class="text-xl font-headline font-bold">Signal Alert</h4>
        </div>
        <p class="text-on-surface-variant text-sm font-label mt-4">
          Positive EV rows deserve inspection first. Negative EV rows are still useful because they help us track when a watched market turns.
        </p>
        <div class="mt-6 pt-6 border-t border-white/5">
          <div class="flex justify-between items-center">
            <span class="text-xs font-label text-outline uppercase tracking-widest">Positive EV</span>
            <span class="text-xs font-bold text-secondary">${rows.filter((row) => row.expectedValue > 0).length} LIVE</span>
          </div>
        </div>
      </div>
    </div>
  `;
}

async function loadWatchlist() {
  const root = document.querySelector("#watchlist-root");
  if (!root) return;
  const response = await fetch("/api/watchlist");
  const payload = await response.json();
  renderWatchlist(root, payload);
}

loadWatchlist().catch((error) => {
  const root = document.querySelector("#watchlist-root");
  if (root) {
    root.innerHTML = `<div class="px-8 py-32 text-center text-secondary">Failed to load watchlist: ${error.message}</div>`;
  }
});
