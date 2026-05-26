/**
 * Rich top toolbar — Dataloom-style icon bar with every canvas action.
 *
 * Layout: left cluster (add-* shortcuts), middle (AI generate),
 * right (history + view + export). Every button carries a tooltip with
 * its keyboard shortcut. Disabled state suppresses the click and
 * dims the icon.
 */

import { ReactNode } from "react";
import clsx from "clsx";
import {
  ArrowDownToLine,
  CaseSensitive,
  Combine,
  Download,
  FileCode2,
  FilePlus,
  Filter,
  Image as ImageIcon,
  LayoutGrid,
  Layers3,
  Network,
  Plus,
  Redo2,
  Save,
  Search,
  Share2,
  Sparkles,
  StickyNote,
  Table,
  Type,
  Undo2,
  Upload,
  Waypoints,
} from "lucide-react";

import { SavedLineagesMenu } from "./SavedLineagesMenu";

interface ToolbarProps {
  onAddTable: () => void;
  onAddFilter: () => void;
  onAddJoin: () => void;
  onAddAggregate: () => void;
  onAddFunction: () => void;
  onAddComment: () => void;
  onAddText: () => void;
  onAddLogo: () => void;
  onUndo: () => void;
  onRedo: () => void;
  canUndo: boolean;
  canRedo: boolean;
  onAutoLayout: () => void;
  onGenerateAI: () => void;
  generating: boolean;
  onSearch: () => void;
  onTrackAttribute: () => void;
  onSave: () => void;
  onExportPng: () => void;
  onShare: () => void;
  onImportSql: () => void;
  onExportSql: () => void;
  primaryProfile: string;
  /** Whether the canvas currently has user work that would be lost
   *  by loading a different artifact — drives the confirm in the
   *  saved-lineages dropdown. */
  hasUnsavedWork: boolean;
  /** Currently loaded artifact id (or null for a fresh canvas). The
   *  saved-lineages dropdown uses this to mark the active row and to
   *  skip the confirm when the user picks the artifact already open. */
  activeArtifactId: number | null;
  /** Called with the chosen artifact id after the optional confirm. */
  onOpenSavedArtifact: (id: number) => void;
  /** Called when the user deletes the currently-loaded saved
   *  artifact — Canvas clears state + strips the URL param. */
  onActiveSavedArtifactDeleted: () => void;
  /** Manual trigger for the "find every known edge between the
   *  tables on canvas" pass. Surfaces deterministic FK / view
   *  DDL / query log edges the anchor-centric LLM skips. */
  onDiscoverRelated: () => void;
  /** Clear the canvas back to a blank state. The Canvas owns the
   *  unsaved-work confirm so this prop is just the user intent
   *  signal. */
  onNewLineage: () => void;
  /** Open the native lineage fetch dialog — pull a table's lineage
   *  straight from the database's own lineage system. */
  onFetchNative: () => void;
}

interface IconButtonProps {
  label: string;
  shortcut?: string;
  onClick: () => void;
  disabled?: boolean;
  children: ReactNode;
  active?: boolean;
}

function IconBtn({ label, shortcut, onClick, disabled, children, active }: IconButtonProps) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      title={shortcut ? `${label} (${shortcut})` : label}
      aria-label={label}
      className={clsx(
        "group relative inline-flex h-8 w-8 items-center justify-center rounded-md border border-transparent text-fg-muted transition",
        "hover:border-surface-border hover:bg-surface hover:text-ink",
        disabled && "cursor-not-allowed opacity-40 hover:bg-transparent hover:text-fg-muted",
        active && "border-accent-default/50 bg-accent-soft text-accent-ink",
      )}
    >
      {children}
    </button>
  );
}

function Divider() {
  return <span className="mx-1 h-5 w-px bg-surface-border" />;
}

export function Toolbar(p: ToolbarProps) {
  return (
    <div className="flex flex-wrap items-center gap-0.5 rounded-xl border border-surface-border bg-surface-raised px-2 py-1 shadow-lg">
      <SavedLineagesMenu
        hasUnsavedWork={p.hasUnsavedWork}
        activeArtifactId={p.activeArtifactId}
        onPick={p.onOpenSavedArtifact}
        onActiveArtifactDeleted={p.onActiveSavedArtifactDeleted}
      />
      <IconBtn label="New lineage" onClick={p.onNewLineage}>
        <FilePlus size={15} />
      </IconBtn>

      <Divider />

      <IconBtn label="Add table" shortcut="D" onClick={p.onAddTable}>
        <Table size={15} />
      </IconBtn>
      <IconBtn label="Add filter" shortcut="F" onClick={p.onAddFilter}>
        <Filter size={15} />
      </IconBtn>
      <IconBtn label="Add function" shortcut="E" onClick={p.onAddFunction}>
        <FileCode2 size={15} />
      </IconBtn>
      <IconBtn label="Add group-by" shortcut="G" onClick={p.onAddAggregate}>
        <Layers3 size={15} />
      </IconBtn>
      <IconBtn label="Add join" shortcut="J" onClick={p.onAddJoin}>
        <Combine size={15} />
      </IconBtn>
      <IconBtn label="Add comment" shortcut="C" onClick={p.onAddComment}>
        <StickyNote size={15} />
      </IconBtn>
      <IconBtn label="Add text" shortcut="T" onClick={p.onAddText}>
        <CaseSensitive size={15} />
      </IconBtn>
      <IconBtn label="Add logo" shortcut="I" onClick={p.onAddLogo}>
        <ImageIcon size={15} />
      </IconBtn>

      <Divider />

      <IconBtn label="Undo" shortcut="⌘Z" onClick={p.onUndo} disabled={!p.canUndo}>
        <Undo2 size={15} />
      </IconBtn>
      <IconBtn label="Redo" shortcut="⌘⇧Z" onClick={p.onRedo} disabled={!p.canRedo}>
        <Redo2 size={15} />
      </IconBtn>

      <Divider />

      <IconBtn label="Auto-arrange" shortcut="L" onClick={p.onAutoLayout}>
        <LayoutGrid size={15} />
      </IconBtn>
      <IconBtn label="Discover related edges" onClick={p.onDiscoverRelated}>
        <Network size={15} />
      </IconBtn>

      <Divider />

      <button
        type="button"
        onClick={p.onGenerateAI}
        disabled={!p.primaryProfile || p.generating}
        className={clsx(
          "inline-flex h-8 items-center gap-1.5 rounded-md px-2.5 text-[12px] font-medium transition",
          "bg-accent-default text-accent-ink hover:brightness-110",
          (!p.primaryProfile || p.generating) && "cursor-not-allowed opacity-50",
        )}
        title="Generate lineage with AMX AI"
      >
        <Sparkles size={14} className={p.generating ? "animate-pulse" : ""} />
        {p.generating ? "Streaming…" : "AI Generate"}
      </button>

      <IconBtn
        label="Fetch lineage from the database (Unity Catalog)"
        onClick={p.onFetchNative}
      >
        <Waypoints size={15} />
      </IconBtn>

      <Divider />

      <IconBtn label="Search (⌘K)" shortcut="⌘K" onClick={p.onSearch}>
        <Search size={15} />
      </IconBtn>
      <IconBtn
        label="Track attribute (⌘⇧F)"
        shortcut="⌘⇧F"
        onClick={p.onTrackAttribute}
      >
        <Type size={15} />
      </IconBtn>

      <Divider />

      <IconBtn label="Save canvas" shortcut="⌘S" onClick={p.onSave}>
        <Save size={15} />
      </IconBtn>
      <IconBtn label="Export PNG" onClick={p.onExportPng}>
        <Download size={15} />
      </IconBtn>
      <IconBtn label="Share" onClick={p.onShare}>
        <Share2 size={15} />
      </IconBtn>

      <Divider />

      <IconBtn label="Import SQL" onClick={p.onImportSql}>
        <Upload size={15} />
      </IconBtn>
      <IconBtn label="Export SQL" onClick={p.onExportSql}>
        <ArrowDownToLine size={15} />
      </IconBtn>

      <span className="ml-2 flex items-center gap-1 rounded-md border border-surface-border px-2 py-1 text-[10px] uppercase tracking-wide text-fg-muted">
        <Plus size={10} />
        {p.primaryProfile || "no profile"}
      </span>
    </div>
  );
}
