import { describe, expect, it, vi } from "vitest";
import { fireEvent, screen } from "@testing-library/react";

import PageEditor from "./PageEditor";
import { renderWithProviders } from "../../test/render";

describe("PageEditor", () => {
  it("toggles between rich editor and raw markdown modes", () => {
    const onChange = vi.fn();
    renderWithProviders(
      <PageEditor initialMarkdown={"# Hello"} onChange={onChange} />,
    );
    // Default shows the toolbar; click the toggle to switch to raw.
    const toggle = screen.getByRole("button", { name: /Raw markdown/i });
    fireEvent.click(toggle);
    const textarea = screen.getByRole("textbox") as HTMLTextAreaElement;
    expect(textarea.value).toContain("Hello");
    fireEvent.change(textarea, { target: { value: "# Updated" } });
    expect(onChange).toHaveBeenCalledWith("# Updated");
  });
});
