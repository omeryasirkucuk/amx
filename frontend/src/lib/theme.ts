// Theme bookkeeping for the visualizer.
//
// Three modes:
//   * "system" — defer to prefers-color-scheme (default).
//   * "light"  — pin .dark off.
//   * "dark"   — pin .dark on.
//
// The ThemeToggle component cycles through them; the choice
// persists in localStorage so a refresh (or a fresh /visualize
// invocation) keeps the user's pick. Tailwind's `darkMode: "class"`
// strategy reads ``.dark`` on <html>; we write that class here.

import { useEffect, useState } from "react";

export type Theme = "system" | "light" | "dark";

const STORAGE_KEY = "amx.visualizer.theme";

export function readStoredTheme(): Theme {
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (raw === "light" || raw === "dark" || raw === "system") return raw;
  } catch {
    /* private mode — fall through */
  }
  return "system";
}

export function writeStoredTheme(theme: Theme): void {
  try {
    window.localStorage.setItem(STORAGE_KEY, theme);
  } catch {
    /* ignore */
  }
}

export function applyTheme(theme: Theme): void {
  const root = document.documentElement;
  const resolved =
    theme === "system"
      ? window.matchMedia("(prefers-color-scheme: dark)").matches
        ? "dark"
        : "light"
      : theme;
  root.classList.toggle("dark", resolved === "dark");
  root.dataset.themeChoice = theme;
  root.dataset.themeResolved = resolved;
}

export function useTheme(): {
  theme: Theme;
  setTheme: (next: Theme) => void;
  resolved: "light" | "dark";
} {
  const [theme, setThemeState] = useState<Theme>(() => readStoredTheme());
  const [resolved, setResolved] = useState<"light" | "dark">(() => {
    const root = document.documentElement;
    return (root.dataset.themeResolved as "light" | "dark") || "light";
  });

  useEffect(() => {
    applyTheme(theme);
    writeStoredTheme(theme);
    setResolved(
      (document.documentElement.dataset.themeResolved as "light" | "dark") || "light",
    );
  }, [theme]);

  // React to OS-level changes when the user has chosen "system".
  useEffect(() => {
    if (theme !== "system") return;
    const media = window.matchMedia("(prefers-color-scheme: dark)");
    const handler = () => {
      applyTheme("system");
      setResolved(media.matches ? "dark" : "light");
    };
    media.addEventListener("change", handler);
    return () => media.removeEventListener("change", handler);
  }, [theme]);

  return { theme, setTheme: setThemeState, resolved };
}
