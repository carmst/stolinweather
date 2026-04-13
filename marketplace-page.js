function formatPercentFromProb(value) {
  return typeof value === "number" ? `${(value * 100).toFixed(1)}%` : "--";
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

let marketplaceDayFilter = "today";

function dateKeyFromValue(value) {
  if (!value) return null;
  return String(value).slice(0, 10);
}

function addDays(dateKey, days) {
  const date = new Date(`${dateKey}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) return null;
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function marketplaceDateKeys(contracts) {
  const dates = [...new Set(contracts.map((row) => dateKeyFromValue(row.eventDate)).filter(Boolean))].sort();
  const today = dates[0] || new Date().toISOString().slice(0, 10);
  return {
    today,
    tomorrow: addDays(today, 1),
  };
}

function dayFilterButton(label, filterName) {
  const active = marketplaceDayFilter === filterName;
  const activeClass = "bg-primary text-on-primary-container rounded-lg font-bold";
  const inactiveClass = "text-on-surface-variant hover:text-on-surface transition-colors";
  return `<button class="px-4 py-2 text-xs font-label uppercase tracking-widest ${active ? activeClass : inactiveClass}" data-market-day="${filterName}">${label}</button>`;
}

function getTemperatureTheme(row) {
  const text = `${row.contract || ""} ${row.contractSubtitle || ""} ${row.eventType || ""}`.toLowerCase();
  const isLowMarket = text.includes("low") || text.includes("temperature_low");
  if (isLowMarket) {
    return {
      accent: "primary",
      accentText: "text-primary",
      accentBg: "bg-primary",
      accentMuted: "bg-primary/15",
      accentSoft: "bg-primary/10",
      border: "border-primary/20",
      glow: "text-glow",
      icon: "ac_unit",
      gradient:
        "bg-[radial-gradient(circle_at_25%_20%,rgba(109,235,253,0.22),transparent_20%),radial-gradient(circle_at_70%_60%,rgba(109,235,253,0.12),transparent_25%),linear-gradient(160deg,#071114,#12232a)]",
      actionText: "text-on-primary-container",
      actionGradient: "from-primary to-primary-container",
    };
  }
  return {
    accent: "secondary",
    accentText: "text-secondary",
    accentBg: "bg-secondary",
    accentMuted: "bg-secondary/15",
    accentSoft: "bg-secondary/10",
    border: "border-secondary/20",
    glow: "",
    icon: "device_thermostat",
    gradient:
      "bg-[radial-gradient(circle_at_25%_20%,rgba(255,115,72,0.20),transparent_20%),radial-gradient(circle_at_70%_60%,rgba(109,235,253,0.14),transparent_25%),linear-gradient(160deg,#071114,#1d1716)]",
    actionText: "text-on-secondary-container",
    actionGradient: "from-secondary to-secondary-container",
  };
}

function getMarketLocalHour(row) {
  const timezone = row.timezone || "America/New_York";
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: timezone,
    hour: "numeric",
    hour12: false,
  }).formatToParts(new Date());
  const hour = parts.find((part) => part.type === "hour");
  return hour ? Number(hour.value) : 12;
}

function renderMiniBars(row, theme) {
  const currentHour = getMarketLocalHour(row);
  const dayHours = [0, 4, 8, 12, 16, 20];
  const dayShape = [18, 26, 44, 100, 72, 34];
  return `
    <div class="space-y-3">
      <div class="flex justify-between text-[0.6rem] font-label uppercase tracking-[0.18em] text-on-surface-variant">
        <span>Forecast window</span>
        <span>${currentHour}:00 local</span>
      </div>
      <div class="relative h-16 w-full flex items-end gap-1">
        ${dayHours
          .map((hour, index) => {
            const isActive = index === dayHours.length - 1 ? currentHour >= hour : currentHour >= hour && currentHour < dayHours[index + 1];
            const klass = isActive ? `${theme.accentBg} shadow-[0_0_12px_rgba(255,255,255,0.08)]` : theme.accentMuted;
            const ring = isActive ? `ring-2 ring-offset-2 ring-offset-surface ring-${theme.accent}/40` : "";
            return `<div class="w-full ${klass} ${ring} rounded-sm transition-all" style="height:${dayShape[index]}%"></div>`;
          })
          .join("")}
      </div>
      <div class="flex justify-between text-[0.55rem] font-label uppercase tracking-[0.18em] text-on-surface-variant/70">
        <span>12a</span>
        <span>8a</span>
        <span>4p</span>
        <span>8p</span>
      </div>
    </div>
  `;
}

function formatActionLabel(row) {
  const side = (row.recommendedSide || "").toUpperCase();
  if (side === "YES") return "EXECUTE YES";
  return "INSPECT YES";
}

function buildDetailUrl(row) {
  const identifier = row.ticker || row.eventTicker || row.marketId || "";
  return `/market-detail.html?ticker=${encodeURIComponent(identifier)}`;
}

function buildSignalSummary(row) {
  const pieces = [];

  if (row.signalDriver) {
    pieces.push(row.signalDriver);
  }

  if (
    typeof row.hourlyPathViolationHours === "number" &&
    typeof row.hourlyPathHours === "number" &&
    row.hourlyPathViolationHours > 0
  ) {
    pieces.push(`${row.hourlyPathViolationHours}/${row.hourlyPathHours} forecast hours violate the YES bucket`);
  }

  if (typeof row.adjustedForecastMaxF === "number" && typeof row.noaaForecastMaxF === "number") {
    const delta = row.adjustedForecastMaxF - row.noaaForecastMaxF;
    if (Math.abs(delta) >= 0.5) {
      pieces.push(`Model is ${Math.abs(delta).toFixed(1)}F ${delta > 0 ? "warmer" : "cooler"} than NOAA`);
    }
  }

  if (typeof row.openMeteoForecastMaxF === "number" && typeof row.noaaForecastMaxF === "number") {
    const spread = row.openMeteoForecastMaxF - row.noaaForecastMaxF;
    if (Math.abs(spread) >= 1.5) {
      pieces.push(`Open-Meteo runs ${Math.abs(spread).toFixed(1)}F ${spread > 0 ? "warmer" : "cooler"} than NOAA`);
    }
  }

  return pieces.slice(0, 2).join(" • ") || "Forecast signal still settling.";
}

function buildContractCard(row) {
  const theme = getTemperatureTheme(row);
  const actionLabel = formatActionLabel(row);
  const wantsYes = (row.recommendedSide || "").toUpperCase() === "YES";
  const actionClass = wantsYes
    ? `bg-gradient-to-r ${theme.actionGradient} ${theme.actionText} hover:shadow-lg hover:shadow-${theme.accent}/20`
    : "bg-surface-variant/40 hover:bg-surface-variant text-on-surface border border-white/5";

  return `
    <div class="glass-card rounded-3xl p-8 relative overflow-hidden group transition-all duration-500 border ${theme.border}">
      <div class="absolute top-0 right-0 p-6 opacity-20 group-hover:opacity-100 transition-opacity">
        <span class="material-symbols-outlined text-4xl ${theme.accentText}">${theme.icon}</span>
      </div>
      <div class="flex flex-col gap-8 h-full">
        <div>
          <span class="text-xs font-label font-bold tracking-[0.2em] ${theme.accentText} uppercase mb-2 block">${row.setupLabel || "Market"}</span>
          <h3 class="text-3xl font-headline font-extrabold tracking-tight">${row.location}: ${row.contract}</h3>
          <p class="text-on-surface-variant text-sm font-label mt-1">${row.contractSubtitle || ""}</p>
        </div>
        <div class="flex items-center justify-between">
          <div class="flex flex-col">
            <span class="text-[0.6rem] font-label text-on-surface-variant uppercase tracking-widest mb-1">Stolin Prediction</span>
            <div class="text-5xl font-headline font-black ${theme.accentText} ${theme.glow} italic">${formatPercentFromProb(row.modelProb)}</div>
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
            <span class="${theme.accentText}">${row.expectedValueDisplay}</span>
          </div>
          ${renderMiniBars(row, theme)}
        </div>
        <p class="text-sm leading-relaxed text-on-surface-variant">${buildSignalSummary(row)}</p>
        ${
          wantsYes
            ? ""
            : `<p class="text-[0.65rem] font-label uppercase tracking-[0.18em] text-on-surface-variant">No YES edge from the current model.</p>`
        }
        <div class="flex items-center justify-between text-[0.65rem] font-label uppercase tracking-[0.18em] text-on-surface-variant">
          <a class="hover:text-primary transition-colors" href="${buildDetailUrl(row)}">Forecast detail</a>
          <a class="hover:text-secondary transition-colors" href="${row.inspectUrl}" target="_blank" rel="noreferrer">Kalshi</a>
        </div>
        <a class="w-full py-4 rounded-2xl font-headline font-extrabold tracking-tight transition-transform active:scale-[0.98] mt-auto text-center ${actionClass}" href="${buildDetailUrl(row)}">
          ${actionLabel}
        </a>
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
  const dateKeys = marketplaceDateKeys(contracts);
  const activeDateKey = marketplaceDayFilter === "tomorrow" ? dateKeys.tomorrow : dateKeys.today;
  const search = document.querySelector("#marketplace-search");
  const query = (search?.value || "").trim().toLowerCase();
  const filtered = contracts.filter((row) => {
    if (activeDateKey && dateKeyFromValue(row.eventDate) !== activeDateKey) return false;
    if (!query) return true;
    return `${row.location} ${row.contract} ${row.contractSubtitle || ""}`.toLowerCase().includes(query);
  });

  const activeDayLabel = marketplaceDayFilter === "tomorrow" ? "Tomorrow" : "Today";

  root.innerHTML = `
    <div class="mb-12 flex flex-col md:flex-row md:items-end justify-between gap-6">
      <div>
        <h1 class="text-5xl font-headline font-extrabold tracking-tighter text-on-surface mb-2">Marketplace</h1>
        <p class="text-on-surface-variant max-w-xl font-label uppercase tracking-widest text-xs">${formatUpdatedAt(dashboard.updatedAt)} · ${dashboard.dataBackend}</p>
      </div>
      <div class="flex flex-wrap gap-3">
        <div class="bg-surface-container-high rounded-xl p-1 flex gap-1">
          ${dayFilterButton("Today", "today")}
          ${dayFilterButton("Tomorrow", "tomorrow")}
        </div>
        <div class="bg-surface-container-high rounded-xl p-1 flex gap-1">
          <a class="px-4 py-2 text-xs font-label uppercase tracking-widest text-on-surface rounded-lg bg-surface-variant hover:text-primary transition-colors" href="/history.html">History</a>
        </div>
      </div>
    </div>
    <div class="mb-6 flex flex-col sm:flex-row sm:items-end justify-between gap-3">
      <div>
        <div class="text-[0.65rem] font-label uppercase tracking-[0.2em] text-on-surface-variant">${activeDayLabel} Contracts</div>
        <div class="text-2xl font-headline font-extrabold">${filtered.length} available</div>
      </div>
      <div class="text-xs font-label uppercase tracking-widest text-on-surface-variant">
        ${activeDateKey || "Date pending"} · ${contracts.length} total loaded
      </div>
    </div>
    <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-8">
      ${
        filtered.length
          ? filtered.map((row) => buildContractCard(row)).join("")
          : `<div class="surface-container-high rounded-3xl p-8 border border-white/5 text-on-surface-variant md:col-span-2 xl:col-span-3">No contracts match the current filters.</div>`
      }
    </div>
  `;

  const nextSearch = document.querySelector("#marketplace-search");
  if (nextSearch) {
    nextSearch.value = query;
    nextSearch.addEventListener("input", loadMarketplace, { once: true });
  }
  document.querySelectorAll("[data-market-day]").forEach((button) => {
    button.addEventListener("click", () => {
      marketplaceDayFilter = button.dataset.marketDay || "today";
      loadMarketplace();
    });
  });
}

loadMarketplace().catch((error) => {
  const root = document.querySelector("#marketplace-root");
  if (root) {
    root.innerHTML = `<div class="px-8 py-24 text-center text-secondary">Failed to load marketplace: ${error.message}</div>`;
  }
});
