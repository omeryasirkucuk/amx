// ErrorBoundary — last line of defence between a component-level
// throw and a blank page. Wraps the route tree in App.tsx; any
// uncaught render or lifecycle error from a route or descendant
// component lands here instead of unmounting the whole SPA.
//
// React still requires class components for error boundaries — there
// is no hook equivalent. Keep the surface small: catch, log, render
// a minimal fallback with two recovery affordances — "Try again"
// (clears the captured error in place; cheap if the throw was
// transient or scoped to one render) and "Reload page" (hard reset,
// the right call when the bundle itself is suspect).
//
// The boundary also takes an optional ``resetKey``. The router wires
// the current pathname so navigating away from a broken route clears
// the error automatically — users don't get stuck on the fallback
// after they pick a different tab from the sidebar.

import { Component, type ErrorInfo, type ReactNode } from "react";

interface ErrorBoundaryProps {
  children: ReactNode;
  /** When this value changes, the boundary clears its captured error.
   *  Wire to ``useLocation().pathname`` so a route change resets the
   *  fallback without forcing a full page reload. */
  resetKey?: unknown;
  /** Render a contained fallback (fits the surrounding panel) instead of
   *  the full-viewport one. Use for per-route boundaries inside the app
   *  shell so a route crash keeps the sidebar/top bar interactive. */
  scoped?: boolean;
}

interface ErrorBoundaryState {
  error: Error | null;
  errorInfo: ErrorInfo | null;
}

export default class ErrorBoundary extends Component<
  ErrorBoundaryProps,
  ErrorBoundaryState
> {
  state: ErrorBoundaryState = { error: null, errorInfo: null };

  static getDerivedStateFromError(error: Error): Partial<ErrorBoundaryState> {
    return { error };
  }

  componentDidCatch(error: Error, errorInfo: ErrorInfo): void {
    // ``console.error`` is intentional here — surfaces the boundary
    // catch in the browser console for the user / dev to inspect, and
    // is the only client-side telemetry sink AMX Studio has today.
    console.error("[amx-studio] crashed:", error, errorInfo);
    this.setState({ errorInfo });
  }

  componentDidUpdate(prevProps: ErrorBoundaryProps): void {
    if (
      this.state.error &&
      prevProps.resetKey !== this.props.resetKey
    ) {
      this.setState({ error: null, errorInfo: null });
    }
  }

  private handleTryAgain = (): void => {
    this.setState({ error: null, errorInfo: null });
  };

  private handleReload = (): void => {
    window.location.reload();
  };

  private handleCopyDetails = async (): Promise<void> => {
    const { error, errorInfo } = this.state;
    if (!error) return;
    const detail = [
      `AMX Studio crash report`,
      `Time: ${new Date().toISOString()}`,
      `URL: ${window.location.href}`,
      `User-Agent: ${navigator.userAgent}`,
      ``,
      `Error: ${error.name}: ${error.message}`,
      error.stack ?? "(no stack)",
      ``,
      `Component stack:`,
      errorInfo?.componentStack ?? "(none)",
    ].join("\n");
    try {
      await navigator.clipboard.writeText(detail);
    } catch {
      // Clipboard API may be denied; fall back to a no-op (the
      // user can still copy from devtools).
    }
  };

  render(): ReactNode {
    const { error } = this.state;
    if (!error) return this.props.children;
    const scoped = this.props.scoped;
    return (
      <div
        style={{
          // Scoped boundaries sit inside the app shell's main canvas, so
          // they fill that area rather than the whole viewport and stay
          // transparent to keep the surrounding chrome visible.
          minHeight: scoped ? "16rem" : "100vh",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          padding: scoped ? "1.5rem" : "2rem",
          background: scoped ? "transparent" : "#0f0f0e",
          color: "#f5f4f2",
          fontFamily:
            '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
        }}
      >
        <div
          style={{
            maxWidth: "44rem",
            padding: "2rem 2.5rem",
            borderRadius: "14px",
            background: "#1a1918",
            border: "1px solid rgba(245,244,242,0.08)",
            boxShadow: "0 12px 32px rgba(0,0,0,0.35)",
          }}
        >
          <h1
            style={{
              marginTop: 0,
              fontSize: "1.4rem",
              letterSpacing: "-0.01em",
            }}
          >
            AMX Studio hit an unexpected error
          </h1>
          <p style={{ opacity: 0.85, lineHeight: 1.55 }}>
            The page above this message stopped rendering. Your data on the
            server is unaffected — the run history, pending review queue, and
            applied descriptions are stored independently of this UI.
          </p>
          <pre
            style={{
              background: "rgba(255,255,255,0.04)",
              padding: "0.75rem 1rem",
              borderRadius: "8px",
              overflow: "auto",
              maxHeight: "12rem",
              fontSize: "0.85rem",
              lineHeight: 1.5,
              margin: "1rem 0",
            }}
          >
            {error.name}: {error.message}
          </pre>
          <div style={{ display: "flex", gap: "0.75rem", flexWrap: "wrap" }}>
            <button
              type="button"
              onClick={this.handleTryAgain}
              style={{
                background: "#f5f4f2",
                color: "#0f0f0e",
                border: "none",
                padding: "0.55rem 1rem",
                borderRadius: "8px",
                fontWeight: 600,
                cursor: "pointer",
              }}
            >
              Try again
            </button>
            <button
              type="button"
              onClick={this.handleReload}
              style={{
                background: "transparent",
                color: "#f5f4f2",
                border: "1px solid rgba(245,244,242,0.25)",
                padding: "0.55rem 1rem",
                borderRadius: "8px",
                fontWeight: 600,
                cursor: "pointer",
              }}
            >
              Reload page
            </button>
            <button
              type="button"
              onClick={this.handleCopyDetails}
              style={{
                background: "transparent",
                color: "#f5f4f2",
                border: "1px solid rgba(245,244,242,0.25)",
                padding: "0.55rem 1rem",
                borderRadius: "8px",
                fontWeight: 600,
                cursor: "pointer",
              }}
            >
              Copy details to clipboard
            </button>
          </div>
        </div>
      </div>
    );
  }
}
