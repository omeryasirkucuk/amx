import { describe, expect, it, vi } from "vitest";
import { fireEvent, screen } from "@testing-library/react";

import DataTable, { type DataTableColumn } from "./DataTable";
import { renderWithProviders } from "../../test/render";

interface Item {
  id: number;
  name: string;
}

const columns: DataTableColumn<Item>[] = [
  { id: "id", header: "ID", sortValue: (r) => r.id, cell: (r) => `#${r.id}` },
  { id: "name", header: "Name", cell: (r) => r.name },
];

describe("DataTable controlled (server) mode", () => {
  it("renders rows verbatim without client-side filtering", () => {
    // Pass two rows but claim a far larger total: the table must show both
    // (no internal slicing) and surface the server total in the pager.
    const rows: Item[] = [
      { id: 1, name: "alpha" },
      { id: 2, name: "beta" },
    ];
    renderWithProviders(
      <DataTable<Item>
        columns={columns}
        rows={rows}
        rowKey={(r) => r.id}
        searchable
        pageSize={50}
        server={{
          query: "zzz", // would eliminate every row if filtered client-side
          onQueryChange: () => {},
          activeFilter: "__all",
          onFilterChange: () => {},
          sort: { id: "id", direction: "desc" },
          onSortChange: () => {},
          page: 0,
          totalRows: 120,
          onPageChange: () => {},
        }}
      />,
    );
    // Both rows render despite the non-matching query (server owns search).
    expect(screen.getByText("#1")).toBeInTheDocument();
    expect(screen.getByText("#2")).toBeInTheDocument();
    // Pager reflects the server total, not the 2 rows on this page.
    expect(screen.getByText("120")).toBeInTheDocument();
  });

  it("delegates search and paging to the parent callbacks", () => {
    const onQueryChange = vi.fn();
    const onPageChange = vi.fn();
    const rows: Item[] = [{ id: 1, name: "alpha" }];
    renderWithProviders(
      <DataTable<Item>
        columns={columns}
        rows={rows}
        rowKey={(r) => r.id}
        searchable
        searchPlaceholder="Search…"
        pageSize={50}
        server={{
          query: "",
          onQueryChange,
          activeFilter: "__all",
          onFilterChange: () => {},
          sort: null,
          onSortChange: () => {},
          page: 0,
          totalRows: 120, // > pageSize so the pager renders
          onPageChange,
        }}
      />,
    );
    fireEvent.change(screen.getByPlaceholderText("Search…"), {
      target: { value: "nyc" },
    });
    expect(onQueryChange).toHaveBeenCalledWith("nyc");

    fireEvent.click(screen.getByText("Next"));
    expect(onPageChange).toHaveBeenCalledWith(1);
  });
});
