function formatUpdatedLabel(updatedAt) {
  const date = new Date(updatedAt);
  if (Number.isNaN(date.getTime())) {
    return "Updated just now";
  }
  return `Updated ${date.toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" })}`;
}

function formatTemp(value) {
  return typeof value === "number" ? `${value.toFixed(1)}F` : "--";
}

function formatPct(value) {
  return typeof value === "number" ? `${Math.round(value * 100)}%` : "--";
}

async function loadLanding() {
  const root = document.querySelector("#landing-root");
  if (!root) return;
  const response = await fetch("/api/dashboard");
  const dashboard = await response.json();
  const topContracts = (dashboard.contracts || []).slice(0, 3);
  const metrics = dashboard.metrics || [];
  const pipeline = dashboard.pipeline || [];
  const systems = dashboard.systems || [];

  root.innerHTML = `
    <section class="relative min-h-[78vh] flex items-center pt-12">
      <div class="absolute inset-0 z-0">
        <div class="absolute inset-0 bg-[radial-gradient(circle_at_20%_20%,rgba(109,235,253,0.22),transparent_28%),radial-gradient(circle_at_80%_30%,rgba(255,115,72,0.16),transparent_24%),linear-gradient(180deg,#081114_0%,#070f12_72%)]"></div>
      </div>
      <div class="relative z-10 px-8 max-w-7xl mx-auto w-full">
        <div class="max-w-4xl">
          <div class="inline-flex items-center gap-3 bg-primary/10 border border-primary/20 px-4 py-2 rounded-full mb-8">
            <span class="text-xs uppercase tracking-[0.3em] text-primary font-semibold">Live Weather Alpha</span>
            <span class="text-xs text-slate-400">${formatUpdatedLabel(dashboard.updatedAt)}</span>
          </div>
          <h1 class="text-6xl md:text-8xl font-black tracking-tighter leading-[0.92] text-on-surface mb-8">
            We price weather like a <span class="text-primary italic">trading problem</span>.
          </h1>
          <p class="text-xl text-on-surface-variant max-w-2xl leading-relaxed mb-10">
            Morning checkpoint forecasts for evaluation, live intraday forecasts for execution, and verified airport settlement sources across every market we track.
          </p>
          <div class="flex flex-wrap gap-4">
            <a class="bg-gradient-to-r from-primary to-primary-container text-on-primary font-bold px-8 py-4 rounded-xl hover:scale-[1.02] transition-transform" href="/marketplace.html">Open Marketplace</a>
            <a class="glass-panel border border-white/10 text-on-surface font-bold px-8 py-4 rounded-xl hover:bg-white/5 transition-colors" href="/history.html">Review Forecast History</a>
          </div>
        </div>
      </div>
    </section>
    <section class="px-8 max-w-7xl mx-auto -mt-12 relative z-20 mb-20">
      <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
        ${metrics.map((metric) => `
          <article class="glass-panel border border-white/5 rounded-3xl p-6">
            <p class="text-xs uppercase tracking-[0.25em] text-slate-400 mb-3">${metric.eyebrow}</p>
            <div class="text-4xl font-black tracking-tight ${metric.valueClass || ""}">${metric.value}</div>
            <p class="text-sm text-slate-400 mt-3 leading-relaxed">${metric.subtle}</p>
          </article>
        `).join("")}
      </div>
    </section>
    <section class="px-8 max-w-7xl mx-auto mb-20">
      <div class="flex items-end justify-between gap-6 mb-10">
        <div>
          <p class="text-xs uppercase tracking-[0.25em] text-primary mb-3">Top Opportunities</p>
          <h2 class="text-4xl font-black tracking-tight">Morning model edge meets live pricing.</h2>
        </div>
        <a class="text-secondary font-bold hover:text-secondary-fixed-dim transition-colors" href="/marketplace.html">View all markets</a>
      </div>
      <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
        ${topContracts.map((contract) => `
          <article class="bg-surface-container-low border border-white/5 rounded-[2rem] p-8 flex flex-col gap-6">
            <div class="flex items-start justify-between gap-4">
              <div>
                <p class="text-sm text-slate-400">${contract.location}</p>
                <h3 class="text-3xl font-black tracking-tight mt-2">${contract.contract}</h3>
                <p class="text-slate-400 mt-2">${contract.contractSubtitle || ""}</p>
              </div>
              <span class="px-3 py-1 rounded-full text-xs font-bold ${contract.setupClass || ""}">${contract.setupLabel || "LIVE"}</span>
            </div>
            <div class="grid grid-cols-2 gap-4 text-sm">
              <div class="bg-surface-container-high rounded-2xl p-4">
                <div class="text-slate-400 uppercase tracking-[0.2em] text-[11px] mb-2">Model</div>
                <div class="text-2xl font-black text-primary">${contract.modelForecastDisplay}</div>
              </div>
              <div class="bg-surface-container-high rounded-2xl p-4">
                <div class="text-slate-400 uppercase tracking-[0.2em] text-[11px] mb-2">Kalshi</div>
                <div class="text-2xl font-black">${contract.kalshiProbDisplay}</div>
              </div>
              <div class="bg-surface-container-high rounded-2xl p-4">
                <div class="text-slate-400 uppercase tracking-[0.2em] text-[11px] mb-2">NOAA</div>
                <div class="text-2xl font-black">${formatTemp(contract.noaaForecastMaxF)}</div>
              </div>
              <div class="bg-surface-container-high rounded-2xl p-4">
                <div class="text-slate-400 uppercase tracking-[0.2em] text-[11px] mb-2">EV</div>
                <div class="text-2xl font-black ${contract.expectedValue > 0 ? "text-primary" : "text-secondary"}">${contract.expectedValueDisplay}</div>
              </div>
            </div>
            <p class="text-slate-300 leading-relaxed">${contract.signalDriver}</p>
            <div class="flex items-center justify-between pt-2">
              <span class="px-3 py-1 rounded-full text-xs font-bold ${contract.confidenceClass}">${contract.confidenceLabel}</span>
              <a class="text-secondary font-bold hover:text-secondary-fixed-dim transition-colors" href="${contract.inspectUrl}" target="_blank" rel="noreferrer">Inspect ↗</a>
            </div>
          </article>
        `).join("")}
      </div>
    </section>
    <section class="px-8 max-w-7xl mx-auto mb-20">
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <article class="bg-surface-container-low border border-white/5 rounded-[2rem] p-8">
          <p class="text-xs uppercase tracking-[0.25em] text-primary mb-4">Pipeline</p>
          <div class="space-y-4">
            ${pipeline.map((step, index) => `
              <div class="flex gap-4">
                <div class="w-9 h-9 rounded-full bg-primary/10 text-primary flex items-center justify-center font-bold">${index + 1}</div>
                <div>
                  <h3 class="font-bold text-lg">${step.title}</h3>
                  <p class="text-slate-400 leading-relaxed">${step.description}</p>
                </div>
              </div>
            `).join("")}
          </div>
        </article>
        <article class="bg-surface-container-low border border-white/5 rounded-[2rem] p-8">
          <p class="text-xs uppercase tracking-[0.25em] text-secondary mb-4">Operating System</p>
          <div class="space-y-4">
            ${systems.map((system) => `
              <div class="rounded-2xl bg-surface-container-high p-5">
                <div class="text-sm uppercase tracking-[0.2em] text-slate-400 mb-2">${system.label}</div>
                <div class="text-lg font-semibold leading-relaxed">${system.value}</div>
              </div>
            `).join("")}
          </div>
        </article>
      </div>
    </section>
  `;
}

loadLanding().catch((error) => {
  const root = document.querySelector("#landing-root");
  if (root) {
    root.innerHTML = `<div class="px-8 py-32 text-center text-secondary">Failed to load landing page: ${error.message}</div>`;
  }
});
