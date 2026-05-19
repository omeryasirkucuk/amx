// Three-way segmented control for the Pages editor.
// Persists the chosen mode in localStorage so the user's preference
// survives reloads, with a sane fallback when storage is blocked.

import { useEffect } from "react";
import { Code2, Eye, PenLine, type LucideIcon } from "lucide-react";

import { cn } from "../../lib/cn";

export type EditorView = "edit" | "preview" | "source";

const STORAGE_KEY = "amx-pages-editor-view";

const OPTIONS: Array<{ id: EditorView; label: string; icon: LucideIcon }> = [
  { id: "edit", label: "Edit", icon: PenLine },
  { id: "preview", label: "Preview", icon: Eye },
  { id: "source", label: "Source", icon: Code2 },
];

interface Props {
  value: EditorView;
  onChange: (next: EditorView) => void;
}

export function loadStoredView(fallback: EditorView = "edit"): EditorView {
  if (typeof window === "undefined") return fallback;
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (raw === "edit" || raw === "preview" || raw === "source") return raw;
  } catch {
    /* private mode etc. */
  }
  return fallback;
}

export default function EditorViewSwitcher({ value, onChange }: Props) {
  useEffect(() => {
    try {
      window.localStorage.setItem(STORAGE_KEY, value);
    } catch {
      /* ignored */
    }
  }, [value]);

  return (
    <div
      role="tablist"
      aria-label="Editor view"
      className="inline-flex items-center gap-0.5 rounded-md border border-border bg-surface p-0.5"
    >
      {OPTIONS.map((opt) => {
        const Icon = opt.icon;
        const active = opt.id === value;
        return (
          <button
            key={opt.id}
            type="button"
            role="tab"
            aria-selected={active}
            onClick={() => onChange(opt.id)}
            className={cn(
              "inline-flex items-center gap-1.5 rounded px-2.5 py-1 text-xs font-medium transition-colors",
              active
                ? "bg-accent-soft text-accent-ink"
                : "text-ink-muted hover:bg-surface-subtle hover:text-ink",
            )}
          >
            <Icon size={12} />
            {opt.label}
          </button>
        );
      })}
    </div>
  );
}
