import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import { Route, Routes } from "react-router-dom";

import PageEditRoute from "./PageEdit";
import { renderWithProviders } from "../test/render";

const originalFetch = globalThis.fetch;

beforeEach(() => {
  globalThis.fetch = vi.fn(async () => {
    return new Response(
      JSON.stringify({
        id: "p-1",
        title: "Demo doc",
        slug: "demo-doc",
        status: "draft",
        markdown_body: "# Hello",
        rendered_html: null,
        created_at: "2026-05-18T00:00:00Z",
        updated_at: "2026-05-18T00:00:00Z",
        created_by: null,
        generation_prompt: null,
        model_used: null,
        assets: [],
        sources: [],
        versions: [],
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      },
    );
  }) as unknown as typeof fetch;
});

afterEach(() => {
  globalThis.fetch = originalFetch;
});

describe("PageEdit route", () => {
  it("hydrates the title from the loaded page", async () => {
    renderWithProviders(
      <Routes>
        <Route path="/pages/:pageId" element={<PageEditRoute />} />
      </Routes>,
      { route: "/pages/p-1" },
    );
    await waitFor(() => {
      const input = screen.getByLabelText("Page title") as HTMLInputElement;
      expect(input.value).toBe("Demo doc");
    });
  });
});
