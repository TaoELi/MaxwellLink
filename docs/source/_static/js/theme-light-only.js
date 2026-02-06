(function () {
  const THEME_KEY = "theme";

  function normalizeTheme(value) {
    return value === "dark" ? "dark" : "light";
  }

  function applyTheme(value) {
    const theme = normalizeTheme(value);
    const body = document.body;
    if (body) {
      body.dataset.theme = theme;
    }
    try {
      localStorage.setItem(THEME_KEY, theme);
    } catch (_) {
      // Ignore storage errors (e.g., private mode).
    }
  }

  function init() {
    let stored = null;
    try {
      stored = localStorage.getItem(THEME_KEY);
    } catch (_) {
      stored = null;
    }

    const theme = normalizeTheme(stored);
    applyTheme(theme);

    const toggles = document.getElementsByClassName("theme-toggle");
    Array.from(toggles).forEach((btn) => {
      btn.addEventListener(
        "click",
        (event) => {
          // Prevent Furo's 3-state (light/dark/auto) handler.
          event.stopImmediatePropagation();
          const current =
            document.body && document.body.dataset.theme === "dark"
              ? "dark"
              : "light";
          const next = current === "dark" ? "light" : "dark";
          applyTheme(next);
        },
        true
      );
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
