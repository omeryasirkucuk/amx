// Documentation Pages list route.
// Renders a table of pages with quick actions, plus a "New page" CTA.
// Collapses to a card layout on narrow screens.

import { useNavigate } from "react-router-dom";
import { FileText, Plus, Trash2 } from "lucide-react";

import PageHeader from "../components/PageHeader";
import EmptyState from "../components/EmptyState";
import { Badge, Button, DataTable, useToast } from "../components/ui";
import type { DataTableColumn, BadgeTone } from "../components/ui";
import { useDeletePage, usePagesList, type Page } from "../hooks/usePages";

const STATUS_TONE: Record<string, BadgeTone> = {
  draft: "neutral",
  published: "positive",
  deleted: "warning",
};

export default function PagesRoute() {
  const navigate = useNavigate();
  const list = usePagesList();
  const toast = useToast();
  const pages = (list.data ?? []).filter((p) => p.status !== "deleted");

  const columns: DataTableColumn<Page>[] = [
    {
      id: "title",
      header: "Title",
      cell: (row) => (
        <span className="font-medium text-ink">{row.title || "Untitled"}</span>
      ),
      sortValue: (row) => row.title,
    },
    {
      id: "status",
      header: "Status",
      cell: (row) => (
        <Badge tone={STATUS_TONE[row.status] ?? "neutral"}>{row.status}</Badge>
      ),
      sortValue: (row) => row.status,
      hideOnMobile: true,
    },
    {
      id: "updated_at",
      header: "Updated",
      cell: (row) => (
        <span className="text-xs text-ink-muted">
          {formatTimestamp(row.updated_at)}
        </span>
      ),
      sortValue: (row) => row.updated_at,
      hideOnMobile: true,
    },
    {
      id: "actions",
      header: "",
      cell: (row) => <RowActions page={row} onDeleted={() => toast.push({ title: "Page deleted" })} />,
      width: "w-28",
      align: "right",
    },
  ];

  return (
    <div className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-6">
      <PageHeader
        title="Documentation pages"
        actions={
          <Button
            variant="primary"
            leadingIcon={<Plus size={14} />}
            onClick={() => navigate("/pages/new")}
          >
            New page
          </Button>
        }
      />
      {list.isLoading ? (
        <div className="text-sm text-ink-dim">Loading pages...</div>
      ) : pages.length === 0 ? (
        <EmptyState
          icon={FileText}
          title="No pages yet"
          description="Documentation pages combine your DB profiles, docs, and lineage context into a single LLM-generated write-up you can edit, export, and share."
          actions={
            <Button
              variant="primary"
              leadingIcon={<Plus size={14} />}
              onClick={() => navigate("/pages/new")}
            >
              Create your first page
            </Button>
          }
        />
      ) : (
        <DataTable
          columns={columns}
          rows={pages}
          rowKey={(p) => p.id}
          onRowClick={(p) => navigate(`/pages/${p.id}`)}
          searchable
          searchPlaceholder="Search pages..."
          searchAccessor={(p) => `${p.title} ${p.slug} ${p.status}`}
          error={list.error ? (list.error as Error).message : null}
        />
      )}
    </div>
  );
}

function RowActions({ page, onDeleted }: { page: Page; onDeleted: () => void }) {
  const del = useDeletePage(page.id);
  return (
    <div className="flex items-center justify-end gap-1" onClick={(e) => e.stopPropagation()}>
      <button
        type="button"
        onClick={() => {
          if (!window.confirm(`Delete "${page.title || "Untitled"}"?`)) return;
          del.mutate(undefined, { onSuccess: () => onDeleted() });
        }}
        disabled={del.isPending}
        aria-label="Delete page"
        className="rounded p-1.5 text-ink-dim hover:bg-surface-subtle hover:text-critical"
      >
        <Trash2 size={14} />
      </button>
    </div>
  );
}

function formatTimestamp(value: string): string {
  try {
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return value;
    return d.toLocaleString();
  } catch {
    return value;
  }
}
