function tickerProbability(row) {
  if (typeof row.modelProb === "number") return row.modelProb;
  if (typeof row.winProbability === "number") return row.winProbability;
  return 0;
}

function tickerLabel(row) {
  const location = row.location ? row.location.replace(/,\s*[A-Z]{2}$/, "") : "Market";
  return `${location} ${row.contract || ""}`.trim();
}

function tickerClass(row) {
  const text = `${row.contract || ""} ${row.contractSubtitle || ""}`.toLowerCase();
  if (text.includes("low")) {
    return {
      text: "text-primary",
      icon: "ac_unit",
    };
  }
  return {
    text: "text-secondary",
    icon: "device_thermostat",
  };
}

function renderTickerItem(row) {
  const theme = tickerClass(row);
  const probability = tickerProbability(row);
  const icon = probability >= 0.5 ? "trending_up" : "trending_down";
  return `
    <a class="flex items-center gap-3 px-6 py-2 bg-surface-container-high rounded-full border border-white/5 hover:border-primary/30 transition-colors" href="${row.inspectUrl || "/marketplace.html"}" target="${row.inspectUrl ? "_blank" : "_self"}" rel="noreferrer">
      <span class="material-symbols-outlined ${theme.text} text-base">${theme.icon}</span>
      <span class="font-label text-[10px] text-on-surface-variant uppercase tracking-widest">${tickerLabel(row)}</span>
      <span class="font-label text-sm font-bold ${theme.text}">${Math.round(probability * 100)}% MODEL</span>
      <span class="material-symbols-outlined ${theme.text} text-base">${icon}</span>
    </a>
  `;
}

async function loadLandingTicker() {
  const root = document.querySelector("#landing-market-ticker");
  if (!root) return;

  const response = await fetch("/api/dashboard");
  const dashboard = await response.json();
  const contracts = dashboard.contractViews?.allContracts || dashboard.contracts || [];
  const rows = contracts
    .filter((row) => typeof tickerProbability(row) === "number")
    .sort((a, b) => tickerProbability(b) - tickerProbability(a))
    .slice(0, 8);

  if (!rows.length) return;

  root.innerHTML = [...rows, ...rows].map(renderTickerItem).join("");
}

loadLandingTicker().catch(() => {
  // Keep the static fallback ticker if live data is temporarily unavailable.
});
