(function () {
  const statusEl = document.getElementById("profile-status");
  const contentEl = document.getElementById("profile-content");
  const signOutButton = document.getElementById("profile-signout");

  let supabaseClient = null;

  function setStatus(message, tone = "neutral") {
    statusEl.textContent = message;
    statusEl.className =
      "rounded-2xl border px-4 py-3 text-sm " +
      (tone === "error"
        ? "border-red-400/30 bg-red-500/10 text-red-200"
        : "border-white/10 bg-white/5 text-slate-300");
  }

  function field(label, value) {
    return `
      <div class="rounded-2xl border border-white/5 bg-white/[0.03] p-5">
        <div class="text-[10px] uppercase tracking-[0.24em] text-slate-500">${label}</div>
        <div class="mt-2 break-all font-headline text-lg font-bold text-slate-100">${value || "--"}</div>
      </div>
    `;
  }

  function renderProfile(user) {
    const displayName = user.user_metadata?.display_name || user.email?.split("@")[0] || "Stolin user";
    contentEl.innerHTML = `
      <div class="mb-8">
        <div class="text-xs uppercase tracking-[0.28em] text-cyan-300">Signed In</div>
        <h1 class="mt-3 text-5xl font-black tracking-tight text-slate-50">${displayName}</h1>
        <p class="mt-3 max-w-2xl text-slate-400">Your profile is ready for user-specific watchlists, saved markets, and later trade-performance tracking.</p>
      </div>
      <div class="grid gap-4 md:grid-cols-2">
        ${field("Email", user.email)}
        ${field("User ID", user.id)}
        ${field("Created", user.created_at ? new Date(user.created_at).toLocaleString() : "")}
        ${field("Auth Provider", user.app_metadata?.provider || "email")}
      </div>
    `;
  }

  async function init() {
    try {
      const { client, config } = await window.StolinAuth.createClient();
      supabaseClient = client;

      if (!config.configured) {
        signOutButton.classList.add("hidden");
        contentEl.innerHTML = `<a class="inline-flex rounded-2xl bg-cyan-300 px-6 py-3 font-bold text-slate-950" href="/auth.html">Open Login</a>`;
        setStatus("Supabase Auth is not configured yet. Add SUPABASE_URL and SUPABASE_ANON_KEY.", "error");
        return;
      }

      const { data, error } = await supabaseClient.auth.getUser();
      if (error) throw error;

      if (!data.user) {
        signOutButton.classList.add("hidden");
        contentEl.innerHTML = `<a class="inline-flex rounded-2xl bg-cyan-300 px-6 py-3 font-bold text-slate-950" href="/auth.html">Sign In</a>`;
        setStatus("You are not signed in yet.");
        return;
      }

      setStatus("Profile loaded.");
      renderProfile(data.user);
    } catch (error) {
      signOutButton.classList.add("hidden");
      contentEl.innerHTML = `<a class="inline-flex rounded-2xl bg-cyan-300 px-6 py-3 font-bold text-slate-950" href="/auth.html">Back to Login</a>`;
      setStatus(window.StolinAuth.formatAuthError(error), "error");
    }
  }

  signOutButton.addEventListener("click", async () => {
    if (!supabaseClient) return;
    signOutButton.disabled = true;
    await supabaseClient.auth.signOut();
    window.location.assign("/auth.html");
  });

  init();
})();
