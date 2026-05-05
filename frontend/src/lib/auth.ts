// Token capture + storage for the AMX Studio SPA.
//
// Workflow:
//   1. The launcher embeds the bearer token in the URL: /?t=<token>.
//   2. captureTokenFromUrl() pulls it into localStorage on first load
//      and strips it from the URL bar so refresh / back / forward
//      don't keep a sensitive value in the browser history.
//   3. getStoredToken() returns it for every fetch wrapper.
//
// The token lives only in localStorage — never cookies, never the URL
// after capture, never console logs. PR-F's Settings page will ship a
// "rotate token" button that re-runs this flow.

const STORAGE_KEY = "amx.studio.token";

export function captureTokenFromUrl(): string | null {
  try {
    const url = new URL(window.location.href);
    const token = url.searchParams.get("t");
    if (!token) {
      return getStoredToken();
    }
    window.localStorage.setItem(STORAGE_KEY, token);
    url.searchParams.delete("t");
    const next = url.pathname + (url.search || "") + (url.hash || "");
    window.history.replaceState({}, document.title, next);
    return token;
  } catch {
    // localStorage may be disabled (private mode); fall back to URL.
    const params = new URLSearchParams(window.location.search);
    return params.get("t");
  }
}

export function getStoredToken(): string | null {
  try {
    return window.localStorage.getItem(STORAGE_KEY);
  } catch {
    return null;
  }
}

export function clearStoredToken(): void {
  try {
    window.localStorage.removeItem(STORAGE_KEY);
  } catch {
    /* ignore */
  }
}

/**
 * Build a query-string token suffix for SSE / EventSource URLs which
 * can't carry an Authorization header. Returns "" when no token has
 * been captured (the call still 401s; the SPA surfaces it as an
 * actionable toast).
 */
export function tokenQuerySuffix(): string {
  const token = getStoredToken();
  return token ? `t=${encodeURIComponent(token)}` : "";
}
