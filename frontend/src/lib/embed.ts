// Embedded-host mode for the AMX Studio SPA.
//
// IDE integrations (e.g. the VS Code extension) render Studio inside
// a webview iframe and add `?embed=1` to the boot URL. In that mode
// the host provides its own navigation chrome (tree views, command
// palette entries), so the SPA hides its global nav and routes
// external links through the host instead of `target="_blank"`
// (iframes inside IDE webviews have no top-level browser context to
// open new tabs in).
//
// The flag is captured once into sessionStorage — like the bearer
// token — so client-side route changes and token-refresh bounces
// through `/` keep the mode without every URL having to carry the
// query param.

const STORAGE_KEY = "amx.studio.embed";

/** Message type the embedding shell listens for to open external URLs. */
export const OPEN_EXTERNAL_MESSAGE = "amx:openExternal";

/**
 * Capture `?embed=1` into sessionStorage and strip it from the URL.
 * Call before the React tree mounts, alongside captureTokenFromUrl().
 */
export function captureEmbedFromUrl(): void {
  try {
    const url = new URL(window.location.href);
    if (url.searchParams.get("embed") !== "1") return;
    window.sessionStorage.setItem(STORAGE_KEY, "1");
    url.searchParams.delete("embed");
    const next = url.pathname + (url.search || "") + (url.hash || "");
    window.history.replaceState({}, document.title, next);
  } catch {
    /* sessionStorage unavailable (private mode) — fall through to
       full-chrome rendering; the embed is cosmetic, not functional. */
  }
}

/** True when the SPA runs inside an embedding host's iframe. */
export function isEmbedded(): boolean {
  try {
    return window.sessionStorage.getItem(STORAGE_KEY) === "1";
  } catch {
    return false;
  }
}

/**
 * Route clicks on external links through the embedding host.
 *
 * The iframe is cross-origin to the host webview, so `target="_blank"`
 * has nowhere to open. Instead, intercept document-level clicks on
 * absolute http(s) anchors pointing off-origin and forward the URL to
 * the parent via postMessage; the host shell relays it to the IDE's
 * open-external API. No-op outside embed mode.
 */
export function installExternalLinkBridge(): void {
  if (!isEmbedded() || window.parent === window) return;
  document.addEventListener(
    "click",
    (event) => {
      const anchor = (event.target as HTMLElement | null)?.closest?.("a[href]");
      if (!anchor) return;
      const href = anchor.getAttribute("href") ?? "";
      let url: URL;
      try {
        url = new URL(href, window.location.href);
      } catch {
        return;
      }
      const external =
        (url.protocol === "http:" || url.protocol === "https:") &&
        url.origin !== window.location.origin;
      if (!external) return;
      event.preventDefault();
      event.stopPropagation();
      window.parent.postMessage({ type: OPEN_EXTERNAL_MESSAGE, url: url.toString() }, "*");
    },
    // Capture phase so the bridge wins over React Router's own
    // listeners and any stopPropagation in component handlers.
    true,
  );
}
