import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { fireEvent, screen, waitFor } from "@testing-library/react";

import PageNewRoute from "./PageNew";
import { renderWithProviders } from "../test/render";

const originalFetch = globalThis.fetch;

beforeEach(() => {
  globalThis.fetch = vi.fn(async (input: RequestInfo | URL) => {
    const url = typeof input === "string" ? input : input.toString();
    if (url.endsWith("/api/profiles/db") || url.endsWith("/api/profiles/docs")) {
      return new Response(JSON.stringify({ profiles: [] }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }
    if (url.startsWith("/api/lineage")) {
      return new Response(JSON.stringify({ artifacts: [], count: 0 }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }
    return new Response("{}", {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  }) as unknown as typeof fetch;
});

afterEach(() => {
  globalThis.fetch = originalFetch;
});

describe("PageNew route", () => {
  it("disables continue until a title is entered", async () => {
    renderWithProviders(<PageNewRoute />, { route: "/pages/new" });
    await waitFor(() =>
      expect(screen.getByLabelText(/Title/i)).toBeInTheDocument(),
    );
    const continueBtn = screen.getByRole("button", { name: /Continue/i });
    expect(continueBtn).toBeDisabled();
    fireEvent.change(screen.getByLabelText(/Title/i), {
      target: { value: "My doc" },
    });
    expect(continueBtn).not.toBeDisabled();
  });
});
