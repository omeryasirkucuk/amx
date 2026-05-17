/**
 * Lineage welcome hub — canvas-first entry point.
 *
 * Three big tiles let the user pick a mode:
 *   1. **AI generate** — opens the create wizard, system populates a
 *      fresh canvas from cached extractor signal.
 *   2. **Draw manually** — opens a blank canvas with an add-node
 *      picker + drag-to-connect so the user can author lineage
 *      from scratch.
 *   3. **Open saved** — jumps to the artifact list for already-
 *      rendered diagrams.
 *
 * The old route shape (browse table on `/lineage`) is preserved at
 * `/lineage/saved` for users who want the table view directly.
 */

import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import {
  ArrowRight,
  FolderOpen,
  PencilLine,
  Sparkles,
  Workflow,
} from "lucide-react";

import { lineageList } from "../lib/api";
import PageHeader from "../components/PageHeader";
import LineageCreateModal from "../components/LineageCreateModal";

export default function Lineage() {
  const navigate = useNavigate();
  const [createOpen, setCreateOpen] = useState(false);
  const list = useQuery({
    queryKey: ["lineage-artifacts"],
    queryFn: () => lineageList(),
  });
  const savedCount = list.data?.count ?? 0;

  return (
    <div className="flex h-full flex-col gap-6">
      <PageHeader
        title="Lineage"
        description="Open a blank canvas to draw lineage by hand, let the system infer it for you from one anchor table, or jump back into an artifact you already rendered."
      />

      <div className="grid gap-4 md:grid-cols-3">
        <Tile
          icon={<Sparkles className="h-6 w-6" />}
          accent="bg-amber-100 text-amber-700"
          title="AI generate"
          description="Pick a table — AMX walks every cached extractor (FK, view DDL, query history, codebase, prior LLM suggestions) and fills a fresh canvas for you."
          ctaLabel="Pick a table →"
          onClick={() => setCreateOpen(true)}
        />
        <Tile
          icon={<PencilLine className="h-6 w-6" />}
          accent="bg-blue-100 text-blue-700"
          title="Draw manually"
          description="Start from a blank canvas. Add tables from your catalogue, drag handles to draw edges, save the result as a custom lineage diagram."
          ctaLabel="Open blank canvas →"
          onClick={() => navigate("/lineage/new")}
        />
        <Tile
          icon={<FolderOpen className="h-6 w-6" />}
          accent="bg-emerald-100 text-emerald-700"
          title="Open saved"
          description={
            savedCount > 0
              ? `Browse the ${savedCount} artifact${savedCount === 1 ? "" : "s"} you've already rendered. Right-click an edge to approve / reject / delete.`
              : "Nothing rendered yet. Use AI generate or Draw manually to make your first one."
          }
          ctaLabel={savedCount > 0 ? `Browse ${savedCount} →` : "Browse →"}
          onClick={() => navigate("/lineage/saved")}
        />
      </div>

      <div className="rounded-md border border-dashed border-surface-border bg-surface-muted px-3 py-3 text-xs text-fg-muted">
        <Workflow className="mb-1 inline h-3.5 w-3.5" /> Everything is
        cache-first — the canvas never opens a live DB connection unless
        you explicitly ask it to refresh from the wire. AI suggestions
        are opt-in and budget-gated.
      </div>

      <LineageCreateModal open={createOpen} onClose={() => setCreateOpen(false)} />
    </div>
  );
}

interface TileProps {
  icon: React.ReactNode;
  accent: string;
  title: string;
  description: string;
  ctaLabel: string;
  onClick: () => void;
}

function Tile({ icon, accent, title, description, ctaLabel, onClick }: TileProps) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="group flex h-full flex-col gap-3 rounded-xl border border-surface-border bg-surface-raised p-5 text-left transition hover:border-accent-default hover:shadow-card"
    >
      <span
        className={`inline-flex h-10 w-10 items-center justify-center rounded-lg ${accent}`}
      >
        {icon}
      </span>
      <h3 className="text-base font-semibold text-fg-default">{title}</h3>
      <p className="flex-1 text-sm text-fg-muted">{description}</p>
      <span className="inline-flex items-center gap-1 text-sm font-medium text-accent-default">
        {ctaLabel}
        <ArrowRight className="h-4 w-4 transition group-hover:translate-x-0.5" />
      </span>
    </button>
  );
}
