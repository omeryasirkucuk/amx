import { describe, expect, it, vi } from "vitest";
import { fireEvent, screen } from "@testing-library/react";

import PageEditor from "./PageEditor";
import { renderWithProviders } from "../../test/render";

describe("PageEditor", () => {
  it("renders the source textarea when view='source' and emits markdown on edit", () => {
    const onChange = vi.fn();
    renderWithProviders(
      <PageEditor
        initialMarkdown={"# Hello"}
        onChange={onChange}
        view="source"
      />,
    );
    const textarea = screen.getByRole("textbox") as HTMLTextAreaElement;
    expect(textarea.value).toContain("Hello");
    fireEvent.change(textarea, { target: { value: "# Updated" } });
    expect(onChange).toHaveBeenCalledWith("# Updated");
  });

  it("renders the preview pane when view='preview'", () => {
    renderWithProviders(
      <PageEditor
        initialMarkdown={"# Hello"}
        onChange={() => {}}
        view="preview"
      />,
    );
    // markdown-it renders the heading inside the preview pane.
    const heading = screen.getByRole("heading", { name: "Hello" });
    expect(heading).toBeTruthy();
  });
});
