async function loadMethodology() {
  const root = document.querySelector("#methodology-root");
  if (!root) return;
  const response = await fetch("/api/dashboard");
  const dashboard = await response.json();

  root.innerHTML = `
    <section class="mb-20">
      <p class="text-xs uppercase tracking-[0.28em] text-primary mb-4">Methodology</p>
      <h1 class="text-5xl md:text-7xl font-black tracking-tighter mb-6">How the model is built, checked, and trained.</h1>
      <p class="text-xl text-slate-400 max-w-3xl leading-relaxed">We use verified airport settlement points, repeated provider snapshots, and official daily highs to keep evaluation honest and the trading surface interpretable.</p>
    </section>
    <section class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-16">
      <article class="bg-surface-container-low rounded-[2rem] border border-white/5 p-8">
        <p class="text-xs uppercase tracking-[0.24em] text-primary mb-5">Data Sources</p>
        <div class="space-y-4">
          ${(dashboard.dataSources || []).map((item) => `
            <div class="bg-surface-container-high rounded-2xl p-5">
              <div class="flex items-center justify-between gap-4 mb-2">
                <h3 class="text-xl font-bold">${item.title}</h3>
                <span class="text-xs uppercase tracking-[0.2em] text-secondary">${item.tag}</span>
              </div>
              <p class="text-slate-400 leading-relaxed">${item.description}</p>
            </div>
          `).join("")}
        </div>
      </article>
      <article class="bg-surface-container-low rounded-[2rem] border border-white/5 p-8">
        <p class="text-xs uppercase tracking-[0.24em] text-secondary mb-5">Feature Stack</p>
        <div class="grid grid-cols-1 gap-4">
          ${(dashboard.featureCards || []).map((item) => `
            <div class="bg-surface-container-high rounded-2xl p-5">
              <div class="flex items-center justify-between gap-4 mb-2">
                <h3 class="text-lg font-bold">${item.title}</h3>
                <span class="text-xs uppercase tracking-[0.2em] text-primary">${item.tag}</span>
              </div>
              <p class="text-slate-400 leading-relaxed">${item.description}</p>
            </div>
          `).join("")}
        </div>
      </article>
    </section>
    <section class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-16">
      <article class="bg-surface-container-low rounded-[2rem] border border-white/5 p-8">
        <p class="text-xs uppercase tracking-[0.24em] text-primary mb-5">Pipeline</p>
        <div class="space-y-4">
          ${(dashboard.pipeline || []).map((item, index) => `
            <div class="flex gap-4">
              <div class="w-9 h-9 rounded-full bg-primary/10 text-primary flex items-center justify-center font-bold">${index + 1}</div>
              <div>
                <h3 class="text-lg font-bold">${item.title}</h3>
                <p class="text-slate-400 leading-relaxed">${item.description}</p>
              </div>
            </div>
          `).join("")}
        </div>
      </article>
      <article class="bg-surface-container-low rounded-[2rem] border border-white/5 p-8">
        <p class="text-xs uppercase tracking-[0.24em] text-secondary mb-5">Roadmap</p>
        <div class="space-y-4">
          ${(dashboard.modelRoadmap || []).map((item) => `
            <div class="bg-surface-container-high rounded-2xl p-5">
              <div class="text-xs uppercase tracking-[0.2em] text-primary mb-2">${item.stage}</div>
              <h3 class="text-lg font-bold mb-2">${item.title}</h3>
              <p class="text-slate-400 leading-relaxed">${item.description}</p>
            </div>
          `).join("")}
        </div>
      </article>
    </section>
    <section class="bg-surface-container-low rounded-[2rem] border border-white/5 p-8">
      <p class="text-xs uppercase tracking-[0.24em] text-primary mb-5">Backtest Readiness</p>
      <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
        ${(dashboard.backtestReadiness || []).map((item) => `
          <article class="bg-surface-container-high rounded-2xl p-6">
            <div class="text-xs uppercase tracking-[0.2em] text-slate-400 mb-2">${item.title}</div>
            <div class="text-3xl font-black mb-3">${item.value}</div>
            <p class="text-slate-400 leading-relaxed">${item.detail}</p>
          </article>
        `).join("")}
      </div>
    </section>
  `;
}

loadMethodology().catch((error) => {
  const root = document.querySelector("#methodology-root");
  if (root) root.innerHTML = `<div class="px-8 py-32 text-center text-secondary">Failed to load methodology: ${error.message}</div>`;
});
