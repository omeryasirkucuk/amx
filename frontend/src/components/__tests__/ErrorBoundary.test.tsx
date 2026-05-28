import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";

import ErrorBoundary from "../ErrorBoundary";

function Boom(): never {
  throw new Error("kaboom");
}

describe("ErrorBoundary", () => {
  beforeEach(() => {
    // React logs caught render errors to console.error; silence the noise.
    vi.spyOn(console, "error").mockImplementation(() => {});
  });
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("renders children when nothing throws", () => {
    render(
      <ErrorBoundary>
        <div>all good</div>
      </ErrorBoundary>,
    );
    expect(screen.getByText("all good")).toBeTruthy();
  });

  it("renders the recovery fallback when a child throws", () => {
    render(
      <ErrorBoundary>
        <Boom />
      </ErrorBoundary>,
    );
    expect(screen.getByText(/AMX Studio hit an unexpected error/i)).toBeTruthy();
    expect(screen.getByRole("button", { name: /try again/i })).toBeTruthy();
  });

  it("scoped variant still renders the fallback (contained in-panel)", () => {
    render(
      <ErrorBoundary scoped>
        <Boom />
      </ErrorBoundary>,
    );
    expect(screen.getByText(/AMX Studio hit an unexpected error/i)).toBeTruthy();
  });

  it("clears the error when resetKey changes (route navigation)", () => {
    const { rerender } = render(
      <ErrorBoundary resetKey="/a">
        <Boom />
      </ErrorBoundary>,
    );
    expect(screen.getByText(/unexpected error/i)).toBeTruthy();
    // Navigating to a different route changes resetKey; the boundary clears
    // and re-renders its (now non-throwing) children instead of staying stuck.
    rerender(
      <ErrorBoundary resetKey="/b">
        <div>recovered</div>
      </ErrorBoundary>,
    );
    expect(screen.getByText("recovered")).toBeTruthy();
  });
});
