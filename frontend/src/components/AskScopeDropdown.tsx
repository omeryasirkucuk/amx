import { useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Check, ChevronDown, Layers } from "lucide-react";

import { apiFetch } from "../lib/api";
import { cn } from "../lib/cn";

interface DbProfileSummary {
  name: string;
  is_active: boolean;
  backend?: string;
}

interface DbProfilesResponse {
  profiles: DbProfileSummary[];
}

interface Props {
  /** Currently selected scope. ``null`` = "All profiles" (config default). */
  scope: string[] | null;
  /** Updates the local + remote sticky scope. ``null`` clears the override. */
  onChange: (next: string[] | null) => void;
  /** Auto-detected focus profile from prior turns. Read-only display. */
  focus?: string | null;
  /** Disable interaction while a question is in flight. */
  disabled?: boolean;
}

/**
 * Multi-select dropdown for the /ask scope. Lives next to the
 * question input. Shape decisions (driven by user feedback):
 * - DROPDOWN, not chips — long profile lists scale better.
 * - Sticky per chat session (parent owns the lifecycle: a new session
 *   resets ``scope`` to ``null``).
 * - "All profiles" is a special toggle that clears all individual
 *   selections; toggling any individual profile clears "All".
 * - Read-only "Focus: X (auto)" line when the conversation has
 *   gravitated toward one profile in recent turns.
 */
export default function AskScopeDropdown({
  scope,
  onChange,
  focus,
  disabled,
}: Props) {
  const [open, setOpen] = useState(false);
  const wrapperRef = useRef<HTMLDivElement | null>(null);

  const profiles = useQuery({
    queryKey: ["db-profiles", "list"],
    queryFn: () => apiFetch<DbProfilesResponse>("/api/profiles/db"),
    staleTime: 30_000,
  });

  useEffect(() => {
    if (!open) return;
    function onClick(e: MouseEvent) {
      if (!wrapperRef.current) return;
      if (!wrapperRef.current.contains(e.target as Node)) setOpen(false);
    }
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") setOpen(false);
    }
    document.addEventListener("mousedown", onClick);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onClick);
      document.removeEventListener("keydown", onKey);
    };
  }, [open]);

  const allProfiles = profiles.data?.profiles ?? [];
  const isAllSelected = scope === null;
  const triggerLabel = isAllSelected
    ? `All profiles${allProfiles.length ? ` (${allProfiles.length})` : ""}`
    : scope.length === 0
      ? "No profiles selected"
      : scope.length === 1
        ? scope[0]
        : `${scope.length} profiles`;

  function toggleProfile(name: string) {
    if (isAllSelected) {
      // Switching from "All" to a single explicit pick.
      onChange([name]);
      return;
    }
    if (scope.includes(name)) {
      const next = scope.filter((p) => p !== name);
      onChange(next.length === 0 ? null : next);
      return;
    }
    onChange([...scope, name]);
  }

  function selectAll() {
    onChange(null);
  }

  return (
    <div className="relative" ref={wrapperRef}>
      <button
        type="button"
        disabled={disabled}
        onClick={() => setOpen((v) => !v)}
        title="Pick which DB profiles this chat queries"
        aria-haspopup="listbox"
        aria-expanded={open}
        className={cn(
          "flex h-7 items-center gap-1.5 rounded-md border px-2 text-[11px] font-medium transition-colors duration-fast",
          disabled
            ? "cursor-default border-surface-border bg-transparent text-ink-dim"
            : isAllSelected
              ? "border-border bg-surface text-ink-muted hover:border-accent/40 hover:text-ink"
              : "border-accent/30 bg-accent-soft text-accent-ink hover:bg-accent-soft/80",
        )}
      >
        <Layers size={12} className="opacity-70" />
        <span className="font-mono uppercase tracking-wider opacity-70 text-[9px]">
          Scope
        </span>
        <span className="max-w-[10rem] truncate">{triggerLabel}</span>
        <ChevronDown size={12} className="opacity-70" />
      </button>
      {open && (
        <div className="absolute right-0 top-full z-30 mt-1 w-72 overflow-hidden rounded-md border border-border bg-surface-raised shadow-md animate-fade-in">
          <button
            type="button"
            onClick={selectAll}
            className={cn(
              "flex w-full items-center justify-between border-b border-border px-3 py-2 text-left text-xs hover:bg-surface-subtle",
              isAllSelected && "bg-accent-soft/40 text-accent-ink",
            )}
          >
            <span className="font-medium">All profiles</span>
            {isAllSelected && <Check size={12} className="text-accent" />}
          </button>
          {profiles.isLoading ? (
            <div className="px-3 py-3 text-xs text-ink-dim">Loading profiles…</div>
          ) : profiles.error ? (
            <div className="px-3 py-3 text-xs text-critical">
              {(profiles.error as Error).message}
            </div>
          ) : allProfiles.length === 0 ? (
            <div className="px-3 py-3 text-xs text-ink-dim">
              No DB profiles configured. Add one under Settings.
            </div>
          ) : (
            <ul className="max-h-72 overflow-y-auto py-1" role="listbox" aria-multiselectable>
              {allProfiles.map((profile) => {
                const selected = !isAllSelected && scope.includes(profile.name);
                return (
                  <li key={profile.name}>
                    <button
                      type="button"
                      role="option"
                      aria-selected={selected}
                      onClick={() => toggleProfile(profile.name)}
                      className={cn(
                        "flex w-full items-center justify-between gap-2 px-3 py-1.5 text-left text-xs hover:bg-surface-subtle",
                        selected && "bg-accent-soft/40",
                      )}
                    >
                      <span className="min-w-0 flex-1">
                        <span className="block truncate font-mono text-ink">
                          {profile.name}
                        </span>
                        {profile.backend && (
                          <span className="block truncate text-[10.5px] text-ink-dim">
                            {profile.backend}
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
          {focus && (
            <div className="border-t border-border bg-surface-subtle/40 px-3 py-1.5 text-[10.5px] text-ink-dim">
              Focus: <span className="font-mono text-ink-muted">{focus}</span>
              <span className="ml-1 opacity-70">(auto-detected)</span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
