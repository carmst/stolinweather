function formatTemp(value) {
  return typeof value === "number" ? `${value.toFixed(1)}F` : "--";
}

function formatPercent(value) {
  return typeof value === "number" ? `${(value * 100).toFixed(1)}%` : "--";
}

function formatCurrency(value) {
  if (typeof value !== "number") return "--";
  return `${value >= 0 ? "+" : "-"}$${Math.abs(value).toFixed(2)}`;
}

function formatTime(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  return date.toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
}

function linePath(points, xFor, yFor) {
  return points
    .map((point, index) => `${index === 0 ? "M" : "L"} ${xFor(point, index).toFixed(2)} ${yFor(point).toFixed(2)}`)
    .join(" ");
}

function buildLineChart({ series, actualHighF, height = 340, xMode = "time" }) {
  const width = 1000;
  const padding = { top: 28, right: 28, bottom: 42, left: 54 };
  const hasProviderPoints = series.some((item) => item.points.length);
  const values = [];
  for (const item of series) {
    for (const point of item.points) {
      if (typeof point.value === "number") values.push(point.value);
    }
  }
  if (typeof actualHighF === "number") values.push(actualHighF);
  if (!hasProviderPoints) {
    return `<div class="h-80 rounded-3xl bg-surface-container-high flex items-center justify-center text-on-surface-variant">No provider hourly path returned for this market yet.</div>`;
  }
  if (!values.length) {
    return `<div class="h-80 rounded-3xl bg-surface-container-high flex items-center justify-center text-on-surface-variant">No chart points available yet.</div>`;
  }

  const minValue = Math.floor(Math.min(...values) - 2);
  const maxValue = Math.ceil(Math.max(...values) + 2);
  const domain = Math.max(1, maxValue - minValue);
  const maxLength = Math.max(...series.map((item) => item.points.length), 1);
  const xValues = xMode === "localHour"
    ? series.flatMap((item) => item.points.map((point) => point.localHour)).filter((value) => typeof value === "number")
    : [];
  const minXValue = xValues.length ? Math.min(...xValues, 0) : 0;
  const maxXValue = xValues.length ? Math.max(...xValues, 23) : 23;
  const xValueDomain = Math.max(1, maxXValue - minXValue);
  const times = xMode === "time"
    ? series
      .flatMap((item) => item.points.map((point) => Date.parse(point.time)))
      .filter((time) => Number.isFinite(time))
    : [];
  const minTime = times.length ? Math.min(...times) : null;
  const maxTime = times.length ? Math.max(...times) : null;
  const timeDomain = minTime == null || maxTime == null ? null : Math.max(1, maxTime - minTime);
  const xFor = (point, index) => {
    if (xMode === "localHour" && typeof point.localHour === "number") {
      return padding.left + ((point.localHour - minXValue) / xValueDomain) * (width - padding.left - padding.right);
    }
    const parsedTime = Date.parse(point.time);
    const position =
      timeDomain != null && Number.isFinite(parsedTime)
        ? (parsedTime - minTime) / timeDomain
        : index / Math.max(1, maxLength - 1);
    return padding.left + position * (width - padding.left - padding.right);
  };
  const yFor = (point) => padding.top + ((maxValue - point.value) / domain) * (height - padding.top - padding.bottom);
  const actualY = typeof actualHighF === "number" ? padding.top + ((maxValue - actualHighF) / domain) * (height - padding.top - padding.bottom) : null;

  return `
    <svg viewBox="0 0 ${width} ${height}" class="w-full h-[360px] overflow-visible">
      <rect x="0" y="0" width="${width}" height="${height}" rx="28" fill="rgba(17,27,31,0.72)"></rect>
      ${[0, 0.25, 0.5, 0.75, 1]
        .map((tick) => {
          const y = padding.top + tick * (height - padding.top - padding.bottom);
          const label = maxValue - tick * domain;
          return `
            <line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" stroke="rgba(255,255,255,0.08)" />
            <text x="18" y="${y + 4}" fill="#6e777a" font-size="18">${label.toFixed(0)}F</text>
          `;
        })
        .join("")}
      ${
        actualY == null
          ? ""
          : `<line x1="${padding.left}" y1="${actualY}" x2="${width - padding.right}" y2="${actualY}" stroke="#f0f8fc" stroke-width="2" stroke-dasharray="8 8" opacity="0.55" />
             <text x="${width - padding.right - 130}" y="${actualY - 8}" fill="#f0f8fc" font-size="18">actual ${formatTemp(actualHighF)}</text>`
      }
      ${
        xMode === "localHour"
          ? [0, 6, 12, 18, 23].map((hour) => {
              const x = padding.left + ((hour - minXValue) / xValueDomain) * (width - padding.left - padding.right);
              const label = hour === 0 ? "12a" : hour < 12 ? `${hour}a` : hour === 12 ? "12p" : `${hour - 12}p`;
              return `<line x1="${x}" y1="${height - padding.bottom}" x2="${x}" y2="${height - padding.bottom + 7}" stroke="rgba(255,255,255,0.18)" />
                <text x="${x}" y="${height - 12}" fill="#6e777a" font-size="18" text-anchor="middle">${label}</text>`;
            }).join("")
          : ""
      }
      ${series
        .filter((item) => item.points.length)
        .map(
          (item) => `
            <path d="${linePath(item.points, xFor, yFor)}" fill="none" stroke="${item.color}" stroke-width="${item.strokeWidth || 4}" stroke-linecap="round" stroke-linejoin="round" opacity="${item.opacity || 1}" ${item.dash ? `stroke-dasharray="${item.dash}"` : ""}></path>
            ${item.points
              .map((point, index) => `<circle cx="${xFor(point, index)}" cy="${yFor(point)}" r="${item.pointRadius || 4}" fill="${item.color}"><title>${item.label}: ${formatTemp(point.value)} at ${xMode === "localHour" && typeof point.localHour === "number" ? `${point.localHour}:00 local` : formatTime(point.time)}</title></circle>`)
              .join("")}
          `
        )
        .join("")}
    </svg>
  `;
}

function providerLabel(provider) {
  if (provider === "noaa-nws") return "NOAA";
  if (provider === "open-meteo") return "Open-Meteo";
  if (provider === "visual-crossing") return "Visual Crossing";
  return provider || "Provider";
}

function buildForecastSeries(points) {
  return [
    { key: "modelHighF", label: "Stolin Model", color: "#6debfd" },
    { key: "noaaHighF", label: "NOAA", color: "#f0f8fc", opacity: 0.72 },
    { key: "openMeteoHighF", label: "Open-Meteo", color: "#ff7348" },
    { key: "visualCrossingHighF", label: "Visual Crossing", color: "#93f1fd", opacity: 0.75 },
  ].map((config) => ({
    ...config,
    points: points
      .filter((point) => typeof point[config.key] === "number")
      .map((point) => ({ time: point.pulledAt, value: point[config.key] })),
  }));
}

function buildHourlySeries(hourlyProviders) {
  const colors = {
    "noaa-nws": "#f0f8fc",
    "open-meteo": "#ff7348",
    "visual-crossing": "#93f1fd",
  };
  return hourlyProviders.map((provider) => ({
    label: providerLabel(provider.provider),
    color: colors[provider.provider] || "#6debfd",
    points: provider.hourly.map((point) => ({
      time: point.time,
      localHour: point.localHour,
      value: point.temperatureF,
    })),
  }));
}

function addModelTargetSeries(series, modelHighF) {
  if (typeof modelHighF !== "number") {
    return series;
  }
  const times = series
    .flatMap((item) => item.points.map((point) => point.time))
    .filter(Boolean)
    .sort((left, right) => Date.parse(left) - Date.parse(right));
  if (!times.length) {
    return series;
  }

  return [
    {
      label: "Stolin Model High",
      color: "#6debfd",
      opacity: 0.95,
      dash: "10 10",
      strokeWidth: 3,
      pointRadius: 0,
      points: [
        { time: times[0], localHour: 0, value: modelHighF },
        { time: times[times.length - 1], localHour: 23, value: modelHighF },
      ],
    },
    ...series,
  ];
}

function statCard(label, value, subtle, colorClass = "text-primary") {
  return `
    <div class="glass-panel rounded-3xl p-6 border border-white/5">
      <div class="text-[0.65rem] font-label uppercase tracking-[0.2em] text-on-surface-variant mb-3">${label}</div>
      <div class="text-4xl font-headline font-black ${colorClass}">${value}</div>
      <div class="text-sm text-on-surface-variant mt-3">${subtle || ""}</div>
    </div>
  `;
}

async function loadMarketDetail() {
  const root = document.querySelector("#market-detail-root");
  if (!root) return;

  const params = new URLSearchParams(window.location.search);
  const response = await fetch(`/api/market-detail?${params.toString()}`);
  const payload = await response.json();
  const market = payload.market;
  if (!market) {
    root.innerHTML = `<div class="glass-panel rounded-3xl p-12 text-center text-on-surface-variant">No market detail is available yet.</div>`;
    return;
  }

  const actualHighF = payload.actual?.actualHighF ?? null;
  const forecastSeries = buildForecastSeries(payload.series || []);
  const hourlySeries = addModelTargetSeries(buildHourlySeries(payload.hourlyProviders || []), market.latestModelHighF);
  const latestPoint = [...(payload.series || [])].reverse().find((point) => typeof point.modelHighF === "number");
  const latestEv = market.latestExpectedValue;
  const pathText =
    typeof market.hourlyPathViolationHours === "number" && typeof market.hourlyPathHours === "number"
      ? `${market.hourlyPathViolationHours}/${market.hourlyPathHours} hours violate bucket`
      : "No path pressure";

  root.innerHTML = `
    <section class="mb-10">
      <div class="flex flex-col lg:flex-row lg:items-end justify-between gap-6">
        <div>
          <a class="text-sm font-label uppercase tracking-[0.2em] text-primary hover:text-cyan-200" href="/marketplace.html">Back to marketplace</a>
          <h1 class="text-5xl md:text-7xl font-headline font-black tracking-tighter mt-4">${market.location || "Market"}</h1>
          <p class="text-2xl text-on-surface-variant mt-3">${market.contract || market.title}</p>
          <p class="text-sm font-label uppercase tracking-[0.2em] text-on-surface-variant mt-4">${market.forecastDate || ""} · ${market.ticker || ""}</p>
        </div>
        <div class="flex flex-wrap gap-3">
          <a class="px-6 py-4 bg-primary text-on-primary-container rounded-2xl font-headline font-extrabold" href="${market.kalshiUrl}" target="_blank" rel="noreferrer">View on Kalshi</a>
          <a class="px-6 py-4 bg-surface-container-high border border-white/10 rounded-2xl font-headline font-extrabold hover:border-primary/40" href="/history.html">History</a>
        </div>
      </div>
    </section>

    <section class="grid grid-cols-1 md:grid-cols-4 gap-4 mb-10">
      ${statCard("Latest Model High", formatTemp(market.latestModelHighF), latestPoint ? `Updated ${formatTime(latestPoint.pulledAt)}` : "No model point")}
      ${statCard("YES Probability", formatPercent(market.latestModelProb), "Current pressure-adjusted model probability", "text-secondary")}
      ${statCard("YES EV", formatCurrency(latestEv), "YES-only expected value", typeof latestEv === "number" && latestEv >= 0 ? "text-primary" : "text-secondary")}
      ${statCard("Hourly Path", pathText, "Provider-hour pressure against this bucket")}
    </section>

    <section class="glass-panel rounded-3xl p-6 md:p-8 border border-white/5 mb-10">
      <div class="flex flex-col md:flex-row md:items-end justify-between gap-4 mb-6">
        <div>
          <h2 class="text-3xl font-headline font-extrabold tracking-tight">Latest Hourly Day Shape</h2>
          <p class="text-on-surface-variant">Hourly temperatures by provider for the contract date, with our model high overlaid as the target line.</p>
        </div>
        <div class="flex flex-wrap gap-4 text-xs font-label uppercase tracking-widest text-on-surface-variant">
          ${hourlySeries.map((item) => `<span><span style="background:${item.color}" class="inline-block w-3 h-3 rounded-full mr-2"></span>${item.label}</span>`).join("")}
        </div>
      </div>
      ${buildLineChart({ series: hourlySeries, actualHighF, xMode: "localHour" })}
    </section>

    <section class="glass-panel rounded-3xl p-6 md:p-8 border border-white/5 mb-10">
      <div class="flex flex-col md:flex-row md:items-end justify-between gap-4 mb-6">
        <div>
          <h2 class="text-3xl font-headline font-extrabold tracking-tight">Forecast Drift</h2>
          <p class="text-on-surface-variant">Collected max-temperature forecast snapshots for this exact Kalshi market. In prod this can be a compact latest-point view until snapshot history is stored in Postgres.</p>
        </div>
        <div class="flex flex-wrap gap-4 text-xs font-label uppercase tracking-widest text-on-surface-variant">
          ${forecastSeries.map((item) => `<span><span style="background:${item.color}" class="inline-block w-3 h-3 rounded-full mr-2"></span>${item.label}</span>`).join("")}
        </div>
      </div>
      ${buildLineChart({ series: forecastSeries, actualHighF })}
    </section>
  `;
}

loadMarketDetail().catch((error) => {
  const root = document.querySelector("#market-detail-root");
  if (root) {
    root.innerHTML = `<div class="glass-panel rounded-3xl p-12 text-center text-secondary">Failed to load market detail: ${error.message}</div>`;
  }
});
