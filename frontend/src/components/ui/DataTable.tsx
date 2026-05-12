import {
  useMemo,
  useState,
  type ChangeEvent,
  type ReactNode,
} from "react";
import { ArrowDown, ArrowUp, ArrowUpDown, Search, X } from "lucide-react";

import { cn } from "../../lib/cn";
import Skeleton from "./Skeleton";

export interface DataTableColumn<T> {
  id: string;
  header: ReactNode;
  /** Cell renderer. Defaults to `String(row[id])` if omitted. */
  cell?: (row: T) => ReactNode;
  /** Sort accessor — return a primitive. Omit to disable column sort. */
  sortValue?: (row: T) => string | number | null | undefined;
  align?: "left" | "right" | "center";
  /** Tailwind width class — e.g. "w-24". Default: auto. */
  width?: string;
  /** Hide on small screens. */
  hideOnMobile?: boolean;
  /** Whether the cell wraps `<td>` content in `font-mono`. */
  mono?: boolean;
}

export interface DataTableFilter<T> {
  id: string;
  label: ReactNode;
  predicate: (row: T) => boolean;
  /** Optional badge text (e.g. row count). */
  badge?: ReactNode;
}

interface Props<T> {
  columns: DataTableColumn<T>[];
  rows: T[];
  /** Stable row key. */
  rowKey: (row: T) => string | number;
  /** Optional row click — typically a route navigation. */
  onRowClick?: (row: T) => void;
  /** Show the search input. Provide `searchAccessor` to control matching. */
  searchable?: boolean;
  searchPlaceholder?: string;
  /** Called per row to produce its searchable haystack. Defaults to a
      JSON.stringify of the row. */
  searchAccessor?: (row: T) => string;
  /** Optional filter chips. The "all" chip is rendered automatically. */
  filters?: DataTableFilter<T>[];
  /** Loading state — renders skeleton rows. */
  isLoading?: boolean;
  /** Fatal error message. */
  error?: string | null;
  /** Tone for the error row. ``warning`` is for advisory/recoverable
   * states (e.g. "history store not initialized — activate a DB
   * profile") where the red ``critical`` tone overstates the severity. */
  errorTone?: "critical" | "warning";
  /** Slot rendered when there are zero rows after filtering. */
  emptyState?: ReactNode;
  /** Page size; pass 0 to disable pagination. */
  pageSize?: number;
  className?: string;
  /** Optional initial sort. */
  initialSort?: { id: string; direction: "asc" | "desc" };
  /** Toolbar slot (right of search). */
  toolbar?: ReactNode;
}

/**
 * Hand-rolled data table — search, filter chips, click-to-sort,
 * pagination, sticky header, skeleton loading. Headless of any
 * specific column shape so each route picks its own. Generic over
 * the row type so column accessors stay typed.
 */
export default function DataTable<T>({
  columns,
  rows,
  rowKey,
  onRowClick,
  searchable,
  searchPlaceholder = "Search…",
  searchAccessor,
  filters,
  isLoading,
  error,
  errorTone = "critical",
  emptyState,
  pageSize = 50,
  className,
  initialSort,
  toolbar,
}: Props<T>) {
  const [query, setQuery] = useState("");
  const [activeFilter, setActiveFilter] = useState<string>("__all");
  const [sort, setSort] = useState<{ id: string; direction: "asc" | "desc" } | null>(
    initialSort ?? null,
  );
  const [page, setPage] = useState(0);

  const haystack = useMemo(() => {
    if (!searchable || !query) return null;
    const q = query.trim().toLowerCase();
    if (!q) return null;
    return q;
  }, [searchable, query]);

  const filtered = useMemo(() => {
    let acc = rows;
    if (filters && activeFilter !== "__all") {
      const f = filters.find((f) => f.id === activeFilter);
      if (f) acc = acc.filter(f.predicate);
    }
    if (haystack) {
      const accessor =
        searchAccessor ??
        ((row: T) => {
          try {
            return JSON.stringify(row).toLowerCase();
          } catch {
            return "";
          }
        });
      acc = acc.filter((r) => accessor(r).toLowerCase().includes(haystack));
    }
    if (sort) {
      const col = columns.find((c) => c.id === sort.id);
      if (col?.sortValue) {
        const dir = sort.direction === "asc" ? 1 : -1;
        acc = [...acc].sort((a, b) => {
          const av = col.sortValue!(a);
          const bv = col.sortValue!(b);
          if (av == null && bv == null) return 0;
          if (av == null) return 1;
          if (bv == null) return -1;
          if (av < bv) return -1 * dir;
          if (av > bv) return 1 * dir;
          return 0;
        });
      }
    }
    return acc;
  }, [rows, filters, activeFilter, haystack, sort, columns, searchAccessor]);

  const totalPages =
    pageSize > 0 ? Math.max(1, Math.ceil(filtered.length / pageSize)) : 1;
  const safePage = Math.min(page, totalPages - 1);
  const visible =
    pageSize > 0
      ? filtered.slice(safePage * pageSize, (safePage + 1) * pageSize)
      : filtered;

  function toggleSort(colId: string) {
    setSort((cur) => {
      if (!cur || cur.id !== colId) return { id: colId, direction: "desc" };
      if (cur.direction === "desc") return { id: colId, direction: "asc" };
      return null;
    });
    setPage(0);
  }

  function handleSearch(e: ChangeEvent<HTMLInputElement>) {
    setQuery(e.target.value);
    setPage(0);
  }

  function handleFilter(id: string) {
    setActiveFilter(id);
    setPage(0);
  }

  return (
    <div className={cn("flex flex-col gap-3", className)}>
      {(searchable || filters || toolbar) && (
        <div className="flex flex-wrap items-center gap-2">
          {searchable && (
            <div className="relative flex h-8 min-w-[14rem] items-center rounded-md border border-border bg-surface-raised pl-2.5 pr-2 focus-within:border-accent focus-within:ring-2 focus-within:ring-accent/20">
              <Search size={13} className="text-ink-dim" />
              <input
                type="search"
                value={query}
                onChange={handleSearch}
                placeholder={searchPlaceholder}
                className="ml-1.5 h-full flex-1 bg-transparent text-sm text-ink placeholder:text-ink-dim focus:outline-none"
              />
              {query && (
                <button
                  type="button"
                  onClick={() => setQuery("")}
                  aria-label="Clear search"
                  className="ml-1 rounded p-0.5 text-ink-dim hover:bg-surface-subtle hover:text-ink"
                >
                  <X size={12} />
                </button>
              )}
            </div>
          )}
          {filters && filters.length > 0 && (
            <div
              role="tablist"
              aria-label="Filters"
              className="inline-flex items-center gap-1 rounded-md bg-surface-subtle p-1"
            >
              <FilterChip
                label="All"
                isActive={activeFilter === "__all"}
                onClick={() => handleFilter("__all")}
                badge={rows.length}
              />
              {filters.map((f) => (
                <FilterChip
                  key={f.id}
                  label={f.label}
                  isActive={activeFilter === f.id}
                  onClick={() => handleFilter(f.id)}
                  badge={f.badge}
                />
              ))}
            </div>
          )}
          {toolbar && <div className="ml-auto flex items-center gap-2">{toolbar}</div>}
        </div>
      )}

      <div className="overflow-hidden rounded-lg border border-border bg-surface-raised shadow-xs">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="border-b border-border bg-surface-subtle/60 text-[10.5px] uppercase tracking-wider text-ink-dim">
              <tr>
                {columns.map((col) => {
                  const sortable = !!col.sortValue;
                  const isSorted = sort?.id === col.id;
                  return (
                    <th
                      key={col.id}
                      scope="col"
                      className={cn(
                        "px-4 py-2 text-left font-semibold",
                        col.align === "right" && "text-right",
                        col.align === "center" && "text-center",
                        col.width,
                        col.hideOnMobile && "hidden md:table-cell",
                      )}
                    >
                      {sortable ? (
                        <button
                          type="button"
                          onClick={() => toggleSort(col.id)}
                          className="inline-flex items-center gap-1 rounded text-ink-dim hover:text-ink"
                        >
                          {col.header}
                          {isSorted ? (
                            sort.direction === "asc" ? (
                              <ArrowUp size={11} />
                            ) : (
                              <ArrowDown size={11} />
                            )
                          ) : (
                            <ArrowUpDown size={11} className="opacity-40" />
                          )}
                        </button>
                      ) : (
                        col.header
                      )}
                    </th>
                  );
                })}
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {isLoading ? (
                Array.from({ length: 6 }).map((_, i) => (
                  <tr key={`sk-${i}`}>
                    {columns.map((col) => (
                      <td
                        key={col.id}
                        className={cn(
                          "px-4 py-2.5",
                          col.hideOnMobile && "hidden md:table-cell",
                        )}
                      >
                        <Skeleton className="h-3 w-3/4" />
                      </td>
                    ))}
                  </tr>
                ))
              ) : error ? (
                <tr>
                  <td
                    colSpan={columns.length}
                    className={
                      errorTone === "warning"
                        ? "px-4 py-6 text-center text-sm text-warning"
                        : "px-4 py-6 text-center text-sm text-critical"
                    }
                  >
                    {error}
                  </td>
                </tr>
              ) : visible.length === 0 ? (
                <tr>
                  <td
                    colSpan={columns.length}
                    className="px-4 py-10 text-center"
                  >
                    {emptyState ?? (
                      <span className="text-sm text-ink-dim">No rows.</span>
                    )}
                  </td>
                </tr>
              ) : (
                visible.map((row) => (
                  <tr
                    key={rowKey(row)}
                    onClick={onRowClick ? () => onRowClick(row) : undefined}
                    className={cn(
                      onRowClick &&
                        "cursor-pointer transition-colors duration-fast hover:bg-surface-subtle/50",
                    )}
                  >
                    {columns.map((col) => {
                      const value = col.cell
                        ? col.cell(row)
                        : String((row as Record<string, unknown>)[col.id] ?? "");
                      return (
                        <td
                          key={col.id}
                          className={cn(
                            "px-4 py-2.5 align-middle text-ink",
                            col.align === "right" && "text-right",
                            col.align === "center" && "text-center",
                            col.mono && "font-mono text-xs",
                            col.hideOnMobile && "hidden md:table-cell",
                          )}
                        >
                          {value}
                        </td>
                      );
                    })}
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        {pageSize > 0 && totalPages > 1 && (
          <div className="flex items-center justify-between gap-2 border-t border-border bg-surface-subtle/40 px-4 py-2 text-xs text-ink-dim">
            <span>
              Showing{" "}
              <span className="font-medium text-ink">
                {safePage * pageSize + 1}–{Math.min((safePage + 1) * pageSize, filtered.length)}
              </span>{" "}
              of <span className="font-medium text-ink">{filtered.length}</span>
            </span>
            <div className="flex items-center gap-1">
              <button
                type="button"
                onClick={() => setPage((p) => Math.max(0, p - 1))}
                disabled={safePage === 0}
                className="rounded px-2 py-1 hover:bg-surface-subtle hover:text-ink disabled:cursor-not-allowed disabled:opacity-40"
              >
                Prev
              </button>
              <span className="px-1 text-ink-muted">
                {safePage + 1} / {totalPages}
              </span>
              <button
                type="button"
                onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
                disabled={safePage === totalPages - 1}
                className="rounded px-2 py-1 hover:bg-surface-subtle hover:text-ink disabled:cursor-not-allowed disabled:opacity-40"
              >
                Next
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function FilterChip({
  label,
  isActive,
  onClick,
  badge,
}: {
  label: ReactNode;
  isActive: boolean;
  onClick: () => void;
  badge?: ReactNode;
}) {
  return (
    <button
      type="button"
      role="tab"
      aria-selected={isActive}
      onClick={onClick}
      className={cn(
        "inline-flex h-6 items-center gap-1.5 rounded px-2 text-xs font-medium transition-colors duration-fast",
        isActive
          ? "bg-surface-raised text-ink shadow-xs"
          : "text-ink-muted hover:text-ink",
      )}
    >
      {label}
      {badge != null && (
        <span
          className={cn(
            "rounded-full px-1 text-[10px]",
            isActive ? "bg-accent-soft text-accent-ink" : "bg-surface-raised text-ink-dim",
          )}
        >
          {badge}
        </span>
      )}
    </button>
  );
}
