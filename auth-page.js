(function () {
  const form = document.getElementById("auth-form");
  const emailInput = document.getElementById("auth-email");
  const passwordInput = document.getElementById("auth-password");
  const nameInput = document.getElementById("auth-name");
  const statusEl = document.getElementById("auth-status");
  const submitButton = document.getElementById("auth-submit");
  const switchButton = document.getElementById("auth-switch");
  const modeLabel = document.getElementById("auth-mode-label");
  const nameField = document.getElementById("auth-name-field");

  let mode = "signin";
  let supabaseClient = null;

  function setStatus(message, tone = "neutral") {
    statusEl.textContent = message;
    statusEl.className =
      "rounded-2xl border px-4 py-3 text-sm " +
      (tone === "error"
        ? "border-red-400/30 bg-red-500/10 text-red-200"
        : tone === "success"
          ? "border-cyan-300/30 bg-cyan-300/10 text-cyan-100"
          : "border-white/10 bg-white/5 text-slate-300");
  }

  function updateMode(nextMode) {
    mode = nextMode;
    const signingUp = mode === "signup";
    modeLabel.textContent = signingUp ? "Create your profile" : "Sign in to Stolin Weather";
    submitButton.textContent = signingUp ? "Create Profile" : "Sign In";
    switchButton.textContent = signingUp ? "Already have an account? Sign in" : "Need an account? Create one";
    nameField.classList.toggle("hidden", !signingUp);
  }

  async function init() {
    try {
      const { client, config } = await window.StolinAuth.createClient();
      supabaseClient = client;

      if (!config.configured) {
        form.classList.add("opacity-50", "pointer-events-none");
        setStatus(
          "Supabase Auth is not configured yet. Add SUPABASE_URL and SUPABASE_ANON_KEY in Vercel/local env.",
          "error"
        );
        return;
      }

      const { data } = await supabaseClient.auth.getSession();
      if (data.session) {
        window.location.assign("/profile.html");
        return;
      }

      setStatus("Use email/password auth. Signup may require email confirmation depending on Supabase settings.");
    } catch (error) {
      form.classList.add("opacity-50", "pointer-events-none");
      setStatus(window.StolinAuth.formatAuthError(error), "error");
    }
  }

  switchButton.addEventListener("click", () => {
    updateMode(mode === "signin" ? "signup" : "signin");
  });

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!supabaseClient) return;

    submitButton.disabled = true;
    submitButton.textContent = mode === "signup" ? "Creating..." : "Signing in...";
    setStatus("Contacting Supabase Auth...");

    const email = emailInput.value.trim();
    const password = passwordInput.value;

    try {
      if (mode === "signup") {
        const { error } = await supabaseClient.auth.signUp({
          email,
          password,
          options: {
            data: {
              display_name: nameInput.value.trim(),
            },
          },
        });
        if (error) throw error;
        setStatus("Profile created. Check your email if confirmation is enabled, then sign in.", "success");
        updateMode("signin");
      } else {
        const { error } = await supabaseClient.auth.signInWithPassword({ email, password });
        if (error) throw error;
        window.location.assign("/profile.html");
      }
    } catch (error) {
      setStatus(window.StolinAuth.formatAuthError(error), "error");
    } finally {
      submitButton.disabled = false;
      submitButton.textContent = mode === "signup" ? "Create Profile" : "Sign In";
    }
  });

  updateMode("signin");
  init();
})();
