// In-memory fallbacks for token + embed-flag capture when web storage
// is unavailable (nested IDE webview iframes partition or reject it).
// Without these the SPA booted but every API call 401'd and the host
// never received the amx:embedReady boot signal.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// jsdom shares one window across the file — remember the real storage
// descriptors so each test starts from working storage again.
const originalLocal = Object.getOwnPropertyDescriptor(window, "localStorage");
const originalSession = Object.getOwnPropertyDescriptor(window, "sessionStorage");

const installThrowingStorage = (): void => {
  const throwing = {
    getItem: () => {
      throw new Error("storage disabled");
    },
    setItem: () => {
      throw new Error("storage disabled");
    },
    removeItem: () => {
      throw new Error("storage disabled");
    },
  };
  Object.defineProperty(window, "localStorage", { value: throwing, configurable: true });
  Object.defineProperty(window, "sessionStorage", { value: throwing, configurable: true });
};

const setLocation = (url: string): void => {
  window.history.replaceState({}, "", url);
};

beforeEach(() => {
  // Each test gets fresh module-level fallback state.
  vi.resetModules();
});

afterEach(() => {
  vi.unstubAllGlobals();
  if (originalLocal) Object.defineProperty(window, "localStorage", originalLocal);
  if (originalSession) Object.defineProperty(window, "sessionStorage", originalSession);
});

describe("auth token in-memory fallback", () => {
  it("serves the URL token from memory when localStorage throws", async () => {
    installThrowingStorage();
    setLocation("/?t=tok-mem");
    const auth = await import("../auth");

    expect(auth.captureTokenFromUrl()).toBe("tok-mem");
    // Later reads must keep working even though nothing was stored.
    expect(auth.getStoredToken()).toBe("tok-mem");
    expect(auth.tokenQuerySuffix()).toBe("t=tok-mem");
  });

  it("keeps the memory copy after the URL param is gone", async () => {
    installThrowingStorage();
    setLocation("/?t=tok-mem");
    const auth = await import("../auth");
    auth.captureTokenFromUrl();

    setLocation("/ask"); // client-side navigation strips the param
    expect(auth.getStoredToken()).toBe("tok-mem");
  });

  it("clearStoredToken drops the memory copy too", async () => {
    installThrowingStorage();
    setLocation("/?t=tok-mem");
    const auth = await import("../auth");
    auth.captureTokenFromUrl();
    auth.clearStoredToken();
    setLocation("/");
    expect(auth.getStoredToken()).toBeNull();
  });

  it("still prefers localStorage when it works", async () => {
    setLocation("/?t=tok-stored");
    const auth = await import("../auth");
    expect(auth.captureTokenFromUrl()).toBe("tok-stored");
    expect(window.localStorage.getItem("amx.studio.token")).toBe("tok-stored");
    expect(auth.getStoredToken()).toBe("tok-stored");
    window.localStorage.removeItem("amx.studio.token");
  });
});

describe("embed flag in-memory fallback", () => {
  it("engages embed mode when sessionStorage throws", async () => {
    installThrowingStorage();
    setLocation("/ask?embed=1");
    const embed = await import("../embed");

    embed.captureEmbedFromUrl();
    expect(embed.isEmbedded()).toBe(true);
  });

  it("stays off without the query param", async () => {
    installThrowingStorage();
    setLocation("/ask");
    const embed = await import("../embed");

    embed.captureEmbedFromUrl();
    expect(embed.isEmbedded()).toBe(false);
  });
});
