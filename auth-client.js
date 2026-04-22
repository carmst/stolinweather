(function () {
  async function loadAuthConfig() {
    const response = await fetch("/api/auth-config", { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`Auth config failed: ${response.status}`);
    }
    return response.json();
  }

  async function createClient() {
    const config = await loadAuthConfig();
    if (!config.configured) {
      return { client: null, config };
    }

    if (!window.supabase?.createClient) {
      throw new Error("Supabase client library did not load.");
    }

    return {
      client: window.supabase.createClient(config.supabaseUrl, config.supabaseAnonKey),
      config,
    };
  }

  function formatAuthError(error) {
    if (!error) return "";
    return error.message || String(error);
  }

  window.StolinAuth = {
    createClient,
    formatAuthError,
  };
})();
