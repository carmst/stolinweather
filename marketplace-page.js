function formatMoney(value) {
  return typeof value === "number" ? `$${value.toFixed(2)}` : "--";
}

function formatPercentFromProb(value) {
  return typeof value === "number" ? `${(value * 100).toFixed(1)}%` : "--";
}

function formatContractTitle(contract) {
  return contract.contractSubtitle ? `${contract.contract} · ${contract.contractSubtitle}` : contract.contract;
}

function formatUpdatedAt(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "Updated just now";
  return `Updated ${date.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  })}`;
}

function renderMiniBars(row, variant) {
  const base = typeof row.kalshiProb === "number" ? row.kalshiProb : 0.5;
  const model = typeof row.modelProb === "number" ? row.modelProb : 0.5;
  const values =
    variant === "positive"
      ? [base * 0.5, base * 0.7, base * 0.65, base * 0.82, model * 0.78, model * 0.92, model]
      : [base, base * 0.9, base * 0.75, model * 0.6, model * 0.45, model * 0.3, model * 0.22];
  const color = variant === "positive" ? "bg-primary" : "bg-secondary";
  const muted = variant === "positive" ? "bg-primary/20" : "bg-secondary/10";
  return `
    <div class="h-16 w-full flex items-end gap-1">
      ${values
        .map((value, index) => {
          const klass = index === values.length - 1 ? color : muted;
          return `<div class="w-full ${klass} rounded-sm" style="height:${Math.max(10, Math.round(value * 100))}%"></div>`;
        })
        .join("")}
    </div>
  `;
}

function buildContractCard(row, emphasis = "positive") {
  const accent = emphasis === "positive" ? "primary" : "secondary";
  const actionLabel = emphasis === "positive" ? "EXECUTE POSITION" : "VIEW ARBITRAGE";
  const actionClass =
    emphasis === "positive"
      ? "bg-gradient-to-r from-primary to-primary-container text-on-primary-container hover:shadow-lg hover:shadow-primary/20"
      : "bg-surface-variant/40 hover:bg-surface-variant text-on-surface border border-white/5";

  return `
    <div class="glass-card rounded-3xl p-8 relative overflow-hidden group transition-all duration-500 hover:shadow-2xl hover:shadow-${accent}/5">
      <div class="absolute top-0 right-0 p-6 opacity-20 group-hover:opacity-100 transition-opacity">
        <span class="material-symbols-outlined text-4xl text-${accent}">${emphasis === "positive" ? "thermostat" : "query_stats"}</span>
      </div>
      <div class="flex flex-col gap-8 h-full">
        <div>
          <span class="text-xs font-label font-bold tracking-[0.2em] text-${accent} uppercase mb-2 block">${row.setupLabel || "Market"}</span>
          <h3 class="text-3xl font-headline font-extrabold tracking-tight">${row.location}: ${row.contract}</h3>
          <p class="text-on-surface-variant text-sm font-label mt-1">${row.contractSubtitle || ""}</p>
        </div>
        <div class="flex items-center justify-between">
          <div class="flex flex-col">
            <span class="text-[0.6rem] font-label text-on-surface-variant uppercase tracking-widest mb-1">Stolin Prediction</span>
            <div class="text-5xl font-headline font-black text-${accent} ${accent === "primary" ? "text-glow" : ""} italic">${formatPercentFromProb(row.modelProb)}</div>
          </div>
          <div class="w-px h-12 bg-outline-variant/30"></div>
          <div class="flex flex-col items-end">
            <span class="text-[0.6rem] font-label text-on-surface-variant uppercase tracking-widest mb-1">Market Odds</span>
            <div class="text-4xl font-headline font-bold text-on-surface">${row.kalshiProbDisplay}</div>
          </div>
        </div>
        <div class="space-y-3">
          <div class="flex justify-between items-center text-[0.65rem] font-label uppercase tracking-widest text-on-surface-variant">
            <span>${row.modelForecastDisplay}</span>
            <span class="text-${accent}">${row.expectedValueDisplay}</span>
          </div>
          ${renderMiniBars(row, emphasis)}
        </div>
        <p class="text-sm leading-relaxed text-on-surface-variant">${row.signalDriver}</p>
        <a class="w-full py-4 rounded-2xl font-headline font-extrabold tracking-tight transition-transform active:scale-[0.98] mt-auto text-center ${actionClass}" href="${row.inspectUrl}" target="_blank" rel="noreferrer">
          ${actionLabel}
        </a>
      </div>
    </div>
  `;
}

function buildFeaturedCard(row) {
  return `
    <div class="glass-card rounded-3xl md:col-span-2 overflow-hidden flex flex-col lg:flex-row relative">
      <div class="lg:w-1/2 p-8 lg:p-12 z-10 bg-slate-950/40 lg:backdrop-blur-none">
        <div class="inline-block px-3 py-1 rounded-full bg-secondary-container/30 border border-secondary/20 text-secondary-fixed text-[10px] font-label font-bold uppercase tracking-[0.2em] mb-6">Featured Setup</div>
        <h2 class="text-4xl font-headline font-extrabold tracking-tighter mb-4 leading-tight">${row.location}: <span class="text-secondary italic">${row.contract}</span></h2>
        <p class="text-on-surface-variant text-base font-body mb-8 leading-relaxed">${row.signalDriver}</p>
        <div class="grid grid-cols-2 gap-6 mb-8">
          <div>
            <div class="text-[0.6rem] font-label uppercase tracking-widest text-on-surface-variant mb-1">Model Forecast</div>
            <div class="text-2xl font-headline font-bold">${row.modelForecastDisplay}</div>
          </div>
          <div>
            <div class="text-[0.6rem] font-label uppercase tracking-widest text-on-surface-variant mb-1">Market Signal</div>
            <div class="text-2xl font-headline font-bold text-secondary">${row.setupLabel}</div>
          </div>
          <div>
            <div class="text-[0.6rem] font-label uppercase tracking-widest text-on-surface-variant mb-1">Kalshi Price</div>
            <div class="text-2xl font-headline font-bold">${row.contractCostDisplay || "--"}</div>
          </div>
          <div>
            <div class="text-[0.6rem] font-label uppercase tracking-widest text-on-surface-variant mb-1">Expected Value</div>
            <div class="text-2xl font-headline font-bold text-primary">${row.expectedValueDisplay}</div>
          </div>
        </div>
        <a class="px-8 py-4 bg-white text-slate-950 font-headline font-extrabold tracking-tight rounded-2xl hover:bg-primary transition-all duration-300 inline-block" href="${row.inspectUrl}" target="_blank" rel="noreferrer">OPEN STRATEGY BUILDER</a>
      </div>
      <div class="lg:w-1/2 relative min-h-[300px] bg-[radial-gradient(circle_at_25%_20%,rgba(109,235,253,0.22),transparent_20%),radial-gradient(circle_at_70%_60%,rgba(255,115,72,0.18),transparent_25%),linear-gradient(160deg,#071114,#12232a)]">
        <div class="absolute inset-0 bg-gradient-to-r from-slate-950 via-transparent to-transparent lg:hidden"></div>
        <div class="absolute inset-0 bg-gradient-to-t from-slate-950 via-transparent to-transparent"></div>
        <div class="absolute inset-0 p-8 flex flex-col justify-between">
          <div class="self-end bg-slate-900/80 backdrop-blur p-3 rounded-xl border border-white/5 flex items-center gap-3">
            <div class="w-2 h-2 rounded-full bg-secondary animate-pulse"></div>
            <span class="text-xs font-label uppercase tracking-widest font-bold">Live Model Tracking</span>
          </div>
          <div class="space-y-4">
            <div class="text-6xl font-black text-primary">${row.location}</div>
            <div class="grid grid-cols-2 gap-3">
              <div class="bg-slate-900/70 rounded-2xl p-4">
                <div class="text-[0.6rem] uppercase tracking-widest text-slate-400 mb-2">NOAA</div>
                <div class="text-2xl font-bold">${typeof row.noaaForecastMaxF === "number" ? `${Math.round(row.noaaForecastMaxF)}F` : "--"}</div>
              </div>
              <div class="bg-slate-900/70 rounded-2xl p-4">
                <div class="text-[0.6rem] uppercase tracking-widest text-slate-400 mb-2">Confidence</div>
                <div class="text-2xl font-bold">${row.confidenceLabel}</div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  `;
}

async function loadMarketplace() {
  const root = document.querySelector("#marketplace-root");
  if (!root) return;

  const response = await fetch("/api/dashboard");
  const dashboard = await response.json();
  const contracts = dashboard.contractViews?.allContracts || dashboard.contracts || [];
  const search = document.querySelector("#marketplace-search");
  const query = (search?.value || "").trim().toLowerCase();
  const filtered = contracts.filter((row) => {
    if (!query) return true;
    return `${row.location} ${row.contract} ${row.contractSubtitle || ""}`.toLowerCase().includes(query);
  });

  const featured = filtered.find((row) => row.setupLabel === "BEST") || filtered[0];
  const topCards = filtered.slice(0, 3);
  const summaryCards = (dashboard.metrics || []).slice(0, 3);

  root.innerHTML = `
    <div class="mb-12 flex flex-col md:flex-row md:items-end justify-between gap-6">
      <div>
        <h1 class="text-5xl font-headline font-extrabold tracking-tighter text-on-surface mb-2">Marketplace</h1>
        <p class="text-on-surface-variant max-w-xl font-label uppercase tracking-widest text-xs">${formatUpdatedAt(dashboard.updatedAt)} · ${dashboard.dataBackend}</p>
      </div>
      <div class="flex flex-wrap gap-3">
        <div class="bg-surface-container-high rounded-xl p-1 flex gap-1">
          <button class="px-4 py-2 text-xs font-label uppercase tracking-widest bg-primary text-on-primary-container rounded-lg font-bold">Live</button>
          <a class="px-4 py-2 text-xs font-label uppercase tracking-widest text-on-surface-variant hover:text-on-surface transition-colors" href="/history.html">History</a>
        </div>
        <div class="bg-surface-container-high rounded-xl p-1 flex gap-1">
          <button class="px-4 py-2 text-xs font-label uppercase tracking-widest text-on-surface rounded-lg bg-surface-variant">North America</button>
        </div>
      </div>
    </div>
    <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
      ${summaryCards
        .map(
          (metric) => `
            <div class="surface-container-high rounded-3xl p-6 border border-white/5">
              <div class="text-[0.65rem] font-label uppercase tracking-widest text-on-surface-variant mb-2">${metric.eyebrow}</div>
              <div class="text-4xl font-headline font-black ${metric.valueClass || ""}">${metric.value}</div>
              <div class="text-sm text-on-surface-variant mt-3">${metric.subtle}</div>
            </div>
          `
        )
        .join("")}
    </div>
    <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-8">
      ${topCards.map((row, index) => buildContractCard(row, index === 1 ? "secondary" : "positive")).join("")}
      ${featured ? buildFeaturedCard(featured) : ""}
      <div class="surface-container-high rounded-3xl p-8 flex flex-col justify-center border border-white/5">
        <div class="mb-6">
          <span class="material-symbols-outlined text-primary text-5xl mb-4">analytics</span>
          <h3 class="text-2xl font-headline font-extrabold tracking-tight">Desk Snapshot</h3>
        </div>
        <div class="space-y-6">
          <div class="flex justify-between items-center p-4 bg-surface-variant/30 rounded-2xl">
            <span class="text-xs font-label uppercase text-on-surface-variant tracking-widest">Tracked Contracts</span>
            <span class="font-headline font-bold text-xl">${contracts.length}</span>
          </div>
          <div class="flex justify-between items-center p-4 bg-surface-variant/30 rounded-2xl">
            <span class="text-xs font-label uppercase text-on-surface-variant tracking-widest">High Confidence</span>
            <span class="font-headline font-bold text-xl text-primary">${contracts.filter((row) => row.confidenceLabel === "STRONG").length}</span>
          </div>
          <div class="flex justify-between items-center p-4 bg-surface-variant/30 rounded-2xl">
            <span class="text-xs font-label uppercase text-on-surface-variant tracking-widest">Positive EV</span>
            <span class="font-headline font-bold text-xl text-secondary">${contracts.filter((row) => typeof row.expectedValue === "number" && row.expectedValue > 0).length}</span>
          </div>
        </div>
      </div>
      ${filtered.slice(3, 9).map((row) => buildContractCard(row, row.expectedValue > 0.08 ? "positive" : "secondary")).join("")}
    </div>
  `;

  const nextSearch = document.querySelector("#marketplace-search");
  if (nextSearch) {
    nextSearch.value = query;
    nextSearch.addEventListener("input", loadMarketplace, { once: true });
  }
}

loadMarketplace().catch((error) => {
  const root = document.querySelector("#marketplace-root");
  if (root) {
    root.innerHTML = `<div class="px-8 py-24 text-center text-secondary">Failed to load marketplace: ${error.message}</div>`;
  }
});
