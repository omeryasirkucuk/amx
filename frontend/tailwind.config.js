/** @type {import('tailwindcss').Config} */
export default {
  darkMode: "class",
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      fontFamily: {
        sans: [
          "Inter",
          "ui-sans-serif",
          "system-ui",
          "-apple-system",
          "Segoe UI",
          "Roboto",
          "sans-serif",
        ],
        mono: [
          "JetBrains Mono",
          "ui-monospace",
          "SFMono-Regular",
          "Menlo",
          "monospace",
        ],
      },
      colors: {
        // Surface palette tuned for the visualizer's "classy" look.
        // CSS-variable backed so the dark/light toggle in PR-F is a
        // one-line :root swap.
        surface: {
          DEFAULT: "rgb(var(--surface) / <alpha-value>)",
          subtle: "rgb(var(--surface-subtle) / <alpha-value>)",
          raised: "rgb(var(--surface-raised) / <alpha-value>)",
          border: "rgb(var(--surface-border) / <alpha-value>)",
        },
        ink: {
          DEFAULT: "rgb(var(--ink) / <alpha-value>)",
          muted: "rgb(var(--ink-muted) / <alpha-value>)",
          dim: "rgb(var(--ink-dim) / <alpha-value>)",
        },
        accent: {
          DEFAULT: "rgb(var(--accent) / <alpha-value>)",
          soft: "rgb(var(--accent-soft) / <alpha-value>)",
          ink: "rgb(var(--accent-ink) / <alpha-value>)",
        },
        positive: "rgb(var(--positive) / <alpha-value>)",
        warning: "rgb(var(--warning) / <alpha-value>)",
        critical: "rgb(var(--critical) / <alpha-value>)",
      },
      boxShadow: {
        card: "0 1px 2px rgba(15, 23, 42, 0.05), 0 8px 24px rgba(15, 23, 42, 0.06)",
        floating:
          "0 12px 32px rgba(15, 23, 42, 0.12), 0 2px 6px rgba(15, 23, 42, 0.08)",
      },
    },
  },
  plugins: [],
};
