const STOLIN_STORM_IMAGE =
  "https://lh3.googleusercontent.com/aida-public/AB6AXuA-8wGxAeQ7C_v-Q9WQMYZLAWSwbne7DMcMi6WsqwZnOYzUk3CqiHqTy793l1oMdwmuJGjdVr3z65i3A1S7zy-_6fNXvowvdSrouqNHIfC2Lnzyt75QgRUWghQi5rAW7p6IqNDDuxg5jmn7AAS_27edIZtWICaAak1iKEQyvrn8MrIAK-1dN_bK2QmiYVX6-uj6Q9Tk9WmuHOKvWFDiGTnfLiFX1bh9BdNltBYPBaZALs0NdUXAiMa4176_Cz9HuFkLdMY69iu-ZNw";

function stolinLoadingCard(icon, label, accentClass = "text-primary") {
  return `
    <div class="glass-panel p-8 rounded-2xl border border-white/5">
      <div class="flex items-center gap-3 mb-5">
        <span class="material-symbols-outlined ${accentClass}">${icon}</span>
        <span class="font-label text-xs uppercase tracking-widest text-slate-400">${label}</span>
      </div>
      <div class="h-12 w-40 rounded-xl bg-white/10 animate-pulse mb-4"></div>
      <div class="h-4 w-56 max-w-full rounded-full bg-white/10 animate-pulse"></div>
    </div>
  `;
}

function renderStolinLoadingState(options = {}) {
  const title = options.title || "Loading live forecast data";
  const subtitle =
    options.subtitle ||
    "Pulling the latest model scores, Kalshi pricing, and verified weather feeds.";
  const context = options.context || "Live weather alpha";

  return `
    <section class="relative mb-16 rounded-3xl overflow-hidden min-h-[400px] flex flex-col justify-end p-8 md:p-12">
      <div class="absolute inset-0 z-0">
        <img class="w-full h-full object-cover opacity-40 grayscale" alt="" src="${STOLIN_STORM_IMAGE}" />
        <div class="absolute inset-0 bg-[radial-gradient(circle_at_20%_20%,rgba(109,235,253,0.22),transparent_28%),linear-gradient(180deg,rgba(7,15,18,0.15),#070f12_95%)]"></div>
      </div>
      <div class="relative z-10 mb-10 max-w-3xl">
        <div class="inline-flex items-center gap-3 rounded-full border border-primary/20 bg-primary/10 px-4 py-2 mb-6">
          <span class="h-2 w-2 rounded-full bg-primary animate-pulse"></span>
          <span class="text-xs uppercase tracking-[0.28em] text-primary font-label font-bold">${context}</span>
        </div>
        <h1 class="text-4xl md:text-6xl font-headline font-black tracking-tighter mb-4">${title}</h1>
        <p class="text-lg text-on-surface-variant leading-relaxed">${subtitle}</p>
      </div>
      <div class="relative z-10 grid grid-cols-1 md:grid-cols-3 gap-8">
        ${stolinLoadingCard("analytics", "Model scores")}
        ${stolinLoadingCard("verified", "Weather feeds", "text-secondary")}
        ${stolinLoadingCard("trending_up", "Market prices")}
      </div>
    </section>
    <section class="grid grid-cols-1 gap-8 mb-16">
      <div class="glass-panel p-8 rounded-2xl relative overflow-hidden">
        <div class="flex flex-col md:flex-row justify-between items-start md:items-center mb-8 gap-4">
          <div>
            <div class="h-8 w-72 max-w-full rounded-xl bg-white/10 animate-pulse mb-3"></div>
            <div class="h-4 w-96 max-w-full rounded-full bg-white/10 animate-pulse"></div>
          </div>
          <div class="h-10 w-44 rounded-xl bg-white/10 animate-pulse"></div>
        </div>
        <div class="h-80 flex items-end gap-2 relative pt-10">
          <div class="absolute inset-0 border-b border-white/5 flex flex-col justify-between py-2">
            <div class="border-t border-white/5 w-full"></div>
            <div class="border-t border-white/5 w-full"></div>
            <div class="border-t border-white/5 w-full"></div>
          </div>
          ${[28, 42, 36, 58, 52, 70, 84, 76, 62, 46, 54, 38]
            .map(
              (height, index) => `
                <div class="relative z-10 flex-1 h-full flex items-end">
                  <div class="w-full ${index % 3 === 0 ? "bg-secondary/25" : "bg-primary/20"} rounded-sm animate-pulse" style="height:${height}%"></div>
                </div>
              `
            )
            .join("")}
        </div>
      </div>
    </section>
  `;
}

function showStolinLoading(root, options = {}) {
  if (!root) return;
  root.innerHTML = renderStolinLoadingState(options);
}

window.StolinLoading = {
  show: showStolinLoading,
  render: renderStolinLoadingState,
};
