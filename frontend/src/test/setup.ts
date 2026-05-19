// Vitest global setup: register jest-dom matchers and stub
// browser APIs that tiptap / lucide-react read at module load.

import "@testing-library/jest-dom/vitest";

// jsdom does not implement matchMedia; AMX has no callers that read
// it directly but TipTap / dependencies may. Stub a minimal version.
if (typeof window !== "undefined" && !window.matchMedia) {
  Object.defineProperty(window, "matchMedia", {
    writable: true,
    value: (query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: () => {},
      removeListener: () => {},
      addEventListener: () => {},
      removeEventListener: () => {},
      dispatchEvent: () => false,
    }),
  });
}
