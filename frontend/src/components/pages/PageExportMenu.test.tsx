import { describe, expect, it } from "vitest";
import { fireEvent, screen } from "@testing-library/react";

import PageExportMenu from "./PageExportMenu";
import { renderWithProviders } from "../../test/render";

describe("PageExportMenu", () => {
  it("opens the menu and exposes Markdown + PDF entries", () => {
    renderWithProviders(<PageExportMenu pageId="p-1" pageTitle="Demo" />);
    const button = screen.getByRole("button", { name: /export/i });
    fireEvent.click(button);
    expect(
      screen.getByRole("menuitem", { name: /Markdown/i }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("menuitem", { name: /PDF/i }),
    ).toBeInTheDocument();
  });
});
