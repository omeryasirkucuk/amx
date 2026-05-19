import { describe, expect, it, vi } from "vitest";
import { fireEvent, screen } from "@testing-library/react";

import SourceAttacher from "./SourceAttacher";
import { renderWithProviders } from "../../test/render";

describe("SourceAttacher", () => {
  it("renders the attached source list and supports removal", () => {
    const onChange = vi.fn();
    const sources = [
      { kind: "upload" as const, path: "/x/a.pdf", original_name: "a.pdf" },
      { kind: "upload" as const, path: "/x/b.csv", original_name: "b.csv" },
    ];
    renderWithProviders(
      <SourceAttacher pageId="page-1" sources={sources} onChange={onChange} />,
    );
    expect(screen.getByText("a.pdf")).toBeInTheDocument();
    fireEvent.click(screen.getByLabelText("Remove a.pdf"));
    expect(onChange).toHaveBeenCalledWith([sources[1]]);
  });

  it("renders the drop zone label", () => {
    const onChange = vi.fn();
    renderWithProviders(
      <SourceAttacher pageId="page-1" sources={[]} onChange={onChange} />,
    );
    expect(
      screen.getByText(/Drop files or click to browse/i),
    ).toBeInTheDocument();
  });
});
