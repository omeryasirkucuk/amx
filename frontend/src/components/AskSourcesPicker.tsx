import { useEffect, useRef, useState } from "react";
import { Check, ChevronDown, FileText } from "lucide-react";

import { cn } from "../lib/cn";

/** Summary of one configured doc profile, as surfaced by /api/ask/context. */
export interface DocProfileSummary {
  name: string;
  indexedChunks: number;
}

/** Summary of one configured code profile, as surfaced by /api/ask/context. */
export interface CodeProfileSummary {
  name: string;
  indexedSnippets: number;
}

/** A lineage canvas the active DB scope is linked to. */
export interface LineageArtifactSummary {
  name: string;
}

export interface AskSourcesPickerProps {
  docProfiles: DocProfileSummary[];
  codeProfiles: CodeProfileSummary[];
  lineageArtifacts: LineageArtifactSummary[];
  anchoredPagesCount: number;
  /** ``null`` = Auto, ``[]`` = Off, ``string[]`` = explicit pick. */
  docOverride: string[] | null;
  codeOverride: string[] | null;
  lineageOverride: string[] | null;
  /** ``null`` = Auto, ``true``/``false`` = explicit on/off. Pages are
   *  entity-anchored so there is no multi-select — only the gate. */
  pagesEnabled: boolean | null;
  onDocChange: (next: string[] | null) => void;
  onCodeChange: (next: string[] | null) => void;
  onLineageChange: (next: string[] | null) => void;
  onPagesChange: (next: boolean | null) => void;
  disabled?: boolean;
}

type PanelKey = "docs" | "code" | "lineage" | "pages";

/**
 * Four-panel source picker for AskChat. Each panel is a self-contained
 * trigger pill that opens a popover; the four pills stack into a
 * single column on narrow viewports and spread out into a row on
 * wider screens. Docs / Code / Lineage share the Auto / Off /
 * multi-select pattern, while Pages exposes only Auto / Off because
 * pages are anchored to entities and there's no list to pick from.
 */
export function AskSourcesPicker({
  docProfiles,
  codeProfiles,
  lineageArtifacts,
  anchoredPagesCount,
  docOverride,
  codeOverride,
  lineageOverride,
  pagesEnabled,
  onDocChange,
  onCodeChange,
  onLineageChange,
  onPagesChange,
  disabled,
}: AskSourcesPickerProps) {
  const [openPanel, setOpenPanel] = useState<PanelKey | null>(null);

  function togglePanel(panel: PanelKey) {
    setOpenPanel((prev) => (prev === panel ? null : panel));
  }
  function closePanel() {
    setOpenPanel(null);
  }

  return (
    <div className="grid w-full grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-4">
      <ListPanel
        label="Docs"
        items={docProfiles.map((p) => ({ name: p.name, count: p.indexedChunks }))}
        mode={docOverride}
        autoCount={docProfiles.length}
        emptyHint="No doc profiles configured."
        open={openPanel === "docs"}
        onToggleOpen={() => togglePanel("docs")}
        onClose={closePanel}
        onChange={onDocChange}
        countSuffix="chunks"
        disabled={disabled}
      />
      <ListPanel
        label="Code"
        items={codeProfiles.map((p) => ({ name: p.name, count: p.indexedSnippets }))}
        mode={codeOverride}
        autoCount={codeProfiles.length}
        emptyHint="No code profiles configured."
        open={openPanel === "code"}
        onToggleOpen={() => togglePanel("code")}
        onClose={closePanel}
        onChange={onCodeChange}
        countSuffix="snippets"
        disabled={disabled}
      />
      <ListPanel
        label="Lineage"
        items={lineageArtifacts.map((a) => ({ name: a.name, count: 0 }))}
        mode={lineageOverride}
        autoCount={lineageArtifacts.length}
        emptyHint="No lineage canvases linked to this scope."
        open={openPanel === "lineage"}
        onToggleOpen={() => togglePanel("lineage")}
        onClose={closePanel}
        onChange={onLineageChange}
        countSuffix=""
        disabled={disabled}
        useCheckboxLabels
      />
      <PagesPanel
        anchoredPagesCount={anchoredPagesCount}
        enabled={pagesEnabled}
        open={openPanel === "pages"}
        onToggleOpen={() => togglePanel("pages")}
        onClose={closePanel}
        onChange={onPagesChange}
        disabled={disabled}
      />
    </div>
  );
}

interface ListItem {
  name: string;
  count: number;
}

interface ListPanelProps {
  label: string;
  items: ListItem[];
  mode: string[] | null;
  autoCount: number;
  emptyHint: string;
  open: boolean;
  onToggleOpen: () => void;
  onClose: () => void;
  onChange: (next: string[] | null) => void;
  countSuffix: string;
  disabled?: boolean;
  /** Render each list row as ``<label><input type="checkbox">…</label>``
   *  instead of the bare ``<button role="option">`` form. Used by the
   *  Lineage panel so screen readers (and our vitest spec) can pick up
   *  each canvas by its label. */
  useCheckboxLabels?: boolean;
}

function ListPanel({
  label,
  items,
  mode,
  autoCount,
  emptyHint,
  open,
  onToggleOpen,
  onClose,
  onChange,
  countSuffix,
  disabled,
  useCheckboxLabels,
}: ListPanelProps) {
  const wrapperRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!open) return;
    function onDocClick(e: MouseEvent) {
      if (!wrapperRef.current) return;
      if (!wrapperRef.current.contains(e.target as Node)) onClose();
    }
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    document.addEventListener("mousedown", onDocClick);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDocClick);
      document.removeEventListener("keydown", onKey);
    };
  }, [open, onClose]);

  const triggerLabel = describeListMode(label, mode, autoCount);
  const isCustom = mode !== null;

  function toggleItem(name: string) {
    if (mode === null) {
      onChange([name]);
      return;
    }
    if (mode.includes(name)) {
      onChange(mode.filter((p) => p !== name));
      return;
    }
    onChange([...mode, name]);
  }

  return (
    <div className="relative" ref={wrapperRef}>
      <button
        type="button"
        disabled={disabled}
        onClick={onToggleOpen}
        aria-haspopup="dialog"
        aria-expanded={open}
        className={cn(
          "flex h-7 w-full items-center gap-1.5 rounded-md border px-2 text-[11px] font-medium transition-colors duration-fast",
          disabled
            ? "cursor-default border-surface-border bg-transparent text-ink-dim"
            : isCustom
              ? "border-accent/30 bg-accent-soft text-accent-ink hover:bg-accent-soft/80"
              : "border-border bg-surface text-ink-muted hover:border-accent/40 hover:text-ink",
        )}
      >
        <FileText size={12} className="opacity-70" />
        <span className="min-w-0 flex-1 truncate text-left">{triggerLabel}</span>
        <ChevronDown size={12} className="opacity-70" />
      </button>
      {open && (
        <div className="absolute bottom-full left-0 z-30 mb-1 w-72 overflow-hidden rounded-md border border-border bg-surface-raised shadow-md animate-fade-in">
          <div className="px-3 py-2">
            <div className="mb-1.5 flex items-center justify-between">
              <span className="text-[10.5px] font-medium uppercase tracking-wider text-ink-dim">
                {label}
              </span>
              <div className="flex gap-1 text-[10.5px]">
                <button
                  type="button"
                  onClick={() => onChange(null)}
                  className={cn(
                    "rounded px-1.5 py-0.5",
                    mode === null
                      ? "bg-accent-soft/60 text-accent-ink"
                      : "text-ink-dim hover:bg-surface-subtle hover:text-ink",
                  )}
                >
                  Auto ({autoCount})
                </button>
                <button
                  type="button"
                  onClick={() => onChange([])}
                  className={cn(
                    "rounded px-1.5 py-0.5",
                    mode !== null && mode.length === 0
                      ? "bg-warning-soft text-warning"
                      : "text-ink-dim hover:bg-surface-subtle hover:text-ink",
                  )}
                >
                  Off
                </button>
              </div>
            </div>
            {items.length === 0 ? (
              <p className="text-[10.5px] text-ink-dim">{emptyHint}</p>
            ) : (
              <ul role="listbox" aria-multiselectable className="space-y-0.5">
                {items.map((p) => {
                  const selected = mode !== null && mode.includes(p.name);
                  if (useCheckboxLabels) {
                    return (
                      <li key={p.name}>
                        <label
                          className={cn(
                            "flex w-full items-center gap-2 rounded px-2 py-1 text-xs hover:bg-surface-subtle",
                            selected && "bg-accent-soft/40",
                          )}
                        >
                          <input
                            type="checkbox"
                            checked={selected}
                            onChange={() => toggleItem(p.name)}
                            className="h-3 w-3 accent-accent"
                          />
                          <span className="min-w-0 flex-1">
                            <span className="block truncate font-mono text-ink">
                              {p.name}
                            </span>
                          </span>
                        </label>
                      </li>
                    );
                  }
                  return (
                    <li key={p.name}>
                      <button
                        type="button"
                        role="option"
                        aria-selected={selected}
                        onClick={() => toggleItem(p.name)}
                        className={cn(
                          "flex w-full items-center justify-between gap-2 rounded px-2 py-1 text-left text-xs hover:bg-surface-subtle",
                          selected && "bg-accent-soft/40",
                        )}
                      >
                        <span className="min-w-0 flex-1">
                          <span className="block truncate font-mono text-ink">
                            {p.name}
                          </span>
                          {p.count > 0 && countSuffix && (
                            <span className="block truncate text-[10px] text-ink-dim">
                              {p.count} {countSuffix}
                            </span>
                          )}
                        </span>
                        {selected && <Check size={12} className="text-accent" />}
                      </button>
                    </li>
                  );
                })}
              </ul>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

interface PagesPanelProps {
  anchoredPagesCount: number;
  enabled: boolean | null;
  open: boolean;
  onToggleOpen: () => void;
  onClose: () => void;
  onChange: (next: boolean | null) => void;
  disabled?: boolean;
}

function PagesPanel({
  anchoredPagesCount,
  enabled,
  open,
  onToggleOpen,
  onClose,
  onChange,
  disabled,
}: PagesPanelProps) {
  const wrapperRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!open) return;
    function onDocClick(e: MouseEvent) {
      if (!wrapperRef.current) return;
      if (!wrapperRef.current.contains(e.target as Node)) onClose();
    }
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    document.addEventListener("mousedown", onDocClick);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDocClick);
      document.removeEventListener("keydown", onKey);
    };
  }, [open, onClose]);

  const triggerLabel = describePagesMode(enabled, anchoredPagesCount);
  const isCustom = enabled !== null;

  return (
    <div className="relative" ref={wrapperRef}>
      <button
        type="button"
        disabled={disabled}
        onClick={onToggleOpen}
        aria-haspopup="dialog"
        aria-expanded={open}
        className={cn(
          "flex h-7 w-full items-center gap-1.5 rounded-md border px-2 text-[11px] font-medium transition-colors duration-fast",
          disabled
            ? "cursor-default border-surface-border bg-transparent text-ink-dim"
            : isCustom
              ? "border-accent/30 bg-accent-soft text-accent-ink hover:bg-accent-soft/80"
              : "border-border bg-surface text-ink-muted hover:border-accent/40 hover:text-ink",
        )}
      >
        <FileText size={12} className="opacity-70" />
        <span className="min-w-0 flex-1 truncate text-left">{triggerLabel}</span>
        <ChevronDown size={12} className="opacity-70" />
      </button>
      {open && (
        <div className="absolute bottom-full left-0 z-30 mb-1 w-64 overflow-hidden rounded-md border border-border bg-surface-raised shadow-md animate-fade-in">
          <div className="px-3 py-2">
            <div className="mb-1.5 flex items-center justify-between">
              <span className="text-[10.5px] font-medium uppercase tracking-wider text-ink-dim">
                Pages
              </span>
            </div>
            <p className="mb-2 text-[10.5px] text-ink-dim">
              {anchoredPagesCount > 0
                ? `${anchoredPagesCount} anchored to this scope.`
                : "No pages anchored to this scope."}
            </p>
            <div className="flex gap-1 text-[11px]">
              <button
                type="button"
                onClick={() => onChange(null)}
                className={cn(
                  "flex-1 rounded px-2 py-1",
                  enabled === null
                    ? "bg-accent-soft/60 text-accent-ink"
                    : "text-ink-dim hover:bg-surface-subtle hover:text-ink",
                )}
              >
                Auto
              </button>
              <button
                type="button"
                onClick={() => onChange(true)}
                className={cn(
                  "flex-1 rounded px-2 py-1",
                  enabled === true
                    ? "bg-accent-soft/60 text-accent-ink"
                    : "text-ink-dim hover:bg-surface-subtle hover:text-ink",
                )}
              >
                On
              </button>
              <button
                type="button"
                onClick={() => onChange(false)}
                className={cn(
                  "flex-1 rounded px-2 py-1",
                  enabled === false
                    ? "bg-warning-soft text-warning"
                    : "text-ink-dim hover:bg-surface-subtle hover:text-ink",
                )}
              >
                Off
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function describeListMode(
  label: string,
  mode: string[] | null,
  autoCount: number,
): string {
  if (mode === null) return `${label}: Auto (${autoCount})`;
  if (mode.length === 0) return `${label}: Off`;
  return `${label}: ${mode.length}`;
}

function describePagesMode(enabled: boolean | null, autoCount: number): string {
  if (enabled === null) return `Pages: Auto (${autoCount})`;
  if (enabled === false) return "Pages: Off";
  return "Pages: On";
}
