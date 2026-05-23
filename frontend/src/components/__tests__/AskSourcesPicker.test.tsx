import { describe, expect, it, vi } from "vitest";
import { fireEvent, screen } from "@testing-library/react";

import { AskSourcesPicker } from "../AskSourcesPicker";
import { renderWithProviders } from "../../test/render";

describe("AskSourcesPicker", () => {
  const baseProps = {
    docProfiles: [{ name: "guides", indexedChunks: 12 }],
    codeProfiles: [{ name: "app-src", indexedSnippets: 88 }],
    lineageArtifacts: [{ name: "canvas-a" }, { name: "canvas-b" }],
    anchoredPagesCount: 2,
    assetKinds: [
      { kind: "notebooks", count: 3 },
      { kind: "queries", count: 1 },
      { kind: "streams", count: 0 },
      { kind: "pipelines", count: 0 },
    ],
    docOverride: null,
    codeOverride: null,
    lineageOverride: null,
    pagesEnabled: null,
    assetsOverride: null,
    onDocChange: vi.fn(),
    onCodeChange: vi.fn(),
    onLineageChange: vi.fn(),
    onPagesChange: vi.fn(),
    onAssetsChange: vi.fn(),
  };

  it("renders all five panels", () => {
    renderWithProviders(<AskSourcesPicker {...baseProps} />);
    expect(screen.getByText(/docs/i)).toBeInTheDocument();
    expect(screen.getByText(/code/i)).toBeInTheDocument();
    expect(screen.getByText(/lineage/i)).toBeInTheDocument();
    expect(screen.getByText(/pages/i)).toBeInTheDocument();
    expect(screen.getByText(/assets/i)).toBeInTheDocument();
  });

  it("invokes onAssetsChange with the wire-plural kind when a checkbox toggles", () => {
    const onAssetsChange = vi.fn();
    renderWithProviders(
      <AskSourcesPicker {...baseProps} onAssetsChange={onAssetsChange} />,
    );
    fireEvent.click(screen.getByRole("button", { name: /assets:/i }));
    fireEvent.click(screen.getByLabelText(/notebooks/i));
    expect(onAssetsChange).toHaveBeenCalledWith(["notebooks"]);
  });

  it("invokes onLineageChange when a canvas checkbox is toggled", () => {
    const onLineageChange = vi.fn();
    renderWithProviders(
      <AskSourcesPicker {...baseProps} onLineageChange={onLineageChange} />,
    );
    // Open the Lineage panel popover first; the canvas checkboxes
    // are rendered inside the popover, not on the trigger pill.
    fireEvent.click(screen.getByRole("button", { name: /lineage:/i }));
    fireEvent.click(screen.getByLabelText(/canvas-a/i));
    expect(onLineageChange).toHaveBeenCalledWith(["canvas-a"]);
  });

  it("invokes onPagesChange when the pages toggle flips to Off", () => {
    const onPagesChange = vi.fn();
    renderWithProviders(
      <AskSourcesPicker {...baseProps} onPagesChange={onPagesChange} />,
    );
    fireEvent.click(screen.getByRole("button", { name: /pages:/i }));
    fireEvent.click(screen.getByRole("button", { name: /^off$/i }));
    expect(onPagesChange).toHaveBeenCalledWith(false);
  });
});
