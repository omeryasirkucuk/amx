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

// In-memory fallback for contexts where localStorage is unavailable
// or partitioned — notably the SPA running inside a nested IDE
// webview iframe, where storage writes can silently fail and a
// "stored" token is never readable again. The module-level variable
// lives exactly as long as the SPA instance, which matches the
// token's lifetime (one per server run).
let memoryToken: string | null = null;
//: Where we stash the user's intended deep link before bouncing them
//: to ``/`` so the launcher can inject a fresh token via
//: ``?t=<...>``. Lives in sessionStorage so it survives the reload but
//: doesn't leak across tabs. Consumed exactly once on the next boot
//: (see :func:`consumeDeeplink`).
const DEEPLINK_KEY = "amx.studio.deeplink";

export function captureTokenFromUrl(): string | null {
  try {
    const url = new URL(window.location.href);
    const token = url.searchParams.get("t");
    if (!token) {
      return getStoredToken();
    }
    // Keep the in-memory copy regardless of storage health so every
    // later read works even when localStorage rejects the write.
    memoryToken = token;
    window.localStorage.setItem(STORAGE_KEY, token);
    url.searchParams.delete("t");
    const next = url.pathname + (url.search || "") + (url.hash || "");
    window.history.replaceState({}, document.title, next);
    return token;
  } catch {
    // localStorage may be disabled (private mode / partitioned
    // iframe); fall back to the URL and remember the value in memory
    // so API + SSE auth keep working after the URL is consumed.
    const params = new URLSearchParams(window.location.search);
    const token = params.get("t");
    if (token) memoryToken = token;
    return memoryToken;
  }
}

export function getStoredToken(): string | null {
  try {
    return window.localStorage.getItem(STORAGE_KEY) ?? memoryToken;
  } catch {
    return memoryToken;
  }
}

export function clearStoredToken(): void {
  memoryToken = null;
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

/**
 * Persist the user's intended deep link (path + search + hash) before
 * a forced reload, so the next boot can land back on the page they
 * were trying to read. Falls back silently when ``sessionStorage`` is
 * unavailable (private mode) — the worst case is the user lands on
 * the home page after a token rotate.
 */
export function rememberDeeplink(target?: string): void {
  try {
    const fallback =
      window.location.pathname + window.location.search + window.location.hash;
    const value = (target ?? fallback).trim();
    // Don't persist "/" — landing on home is the default and stashing
    // it would cause an infinite redirect on the next boot.
    if (!value || value === "/") {
      window.sessionStorage.removeItem(DEEPLINK_KEY);
      return;
    }
    window.sessionStorage.setItem(DEEPLINK_KEY, value);
  } catch {
    /* sessionStorage unavailable — fall through */
  }
}

/**
 * Read and clear the stashed deep link. Returns ``null`` when none was
 * stored, or when the value isn't a same-origin path. The same-origin
 * check is paranoid — sessionStorage is per-origin already, but
 * surfacing the rule here keeps an accidental ``http://evil.example/``
 * value from ever becoming a navigation target.
 */
export function consumeDeeplink(): string | null {
  try {
    const raw = window.sessionStorage.getItem(DEEPLINK_KEY);
    window.sessionStorage.removeItem(DEEPLINK_KEY);
    if (!raw) return null;
    if (!raw.startsWith("/") || raw.startsWith("//")) return null;
    return raw;
  } catch {
    return null;
  }
}
