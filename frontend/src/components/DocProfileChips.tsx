import { useQuery } from "@tanstack/react-query";
import { Check, FileText } from "lucide-react";

import { apiFetch } from "../lib/api";
import { cn } from "../lib/cn";

interface DocProfileSummary {
  name: string;
  paths: string[];
  is_active: boolean;
  linked_db_profiles?: string[];
}

interface DocProfilesResponse {
  profiles: DocProfileSummary[];
  active: string | null;
}

interface Props {
  /** Currently selected doc profiles. ``null`` = use config default
   *  (cfg.active_doc_profile / cfg.run_doc_profiles on disk). An
   *  explicit empty array means "no docs for this run". */
  selected: string[] | null;
  onChange: (next: string[] | null) => void;
  disabled?: boolean;
}

/**
 * Multi-select chip row for doc profiles, rendered inside the Run
 * dialog. PR E (RAG hardening) — lets the user pick which doc
 * profiles feed the RAG agent for *this* run only without flipping
 * the active profile on disk.
 *
 * UX rules:
 * - ``selected === null`` renders "All doc profiles" toggle as
 *   active and highlights the config-default profile in muted.
 * - Toggling any individual chip flips selected to a concrete list
 *   (one item) or appends to the existing array.
 * - The "All" chip clears the selection back to null.
 * - When no doc profiles are configured, renders an inline hint
 *   pointing the user at Settings → Docs.
 */
export default function DocProfileChips({ selected, onChange, disabled }: Props) {
  const profiles = useQuery({
    queryKey: ["profiles", "docs", "list-for-run"],
    queryFn: () => apiFetch<DocProfilesResponse>("/api/profiles/docs"),
    staleTime: 30_000,
  });
  const list = profiles.data?.profiles ?? [];
  const activeName = profiles.data?.active ?? null;
  const isAllSelected = selected === null;

  if (profiles.isLoading) {
    return (
      <div className="mt-3 text-[11px] text-ink-dim">Loading doc profiles…</div>
    );
  }
  if (profiles.error) {
    return null; // silent — the run still works without a doc override
  }
  if (list.length === 0) {
    return (
      <div className="mt-3 inline-flex items-center gap-1.5 rounded-md bg-surface-subtle px-2 py-1 text-[11px] text-ink-dim">
        <FileText size={11} className="opacity-70" />
        No doc profiles configured.
      </div>
    );
  }

  function toggleProfile(name: string) {
    if (disabled) return;
    if (isAllSelected) {
      onChange([name]);
      return;
    }
    if (selected.includes(name)) {
      const next = selected.filter((p) => p !== name);
      onChange(next);
      return;
    }
    onChange([...selected, name]);
  }

  return (
    <div className="mt-3">
      <div className="mb-1 flex items-center justify-between gap-2">
        <span className="text-[10px] font-medium uppercase tracking-wider text-ink-dim">
          Doc profiles
        </span>
        <button
          type="button"
          disabled={disabled}
          onClick={() => onChange(null)}
          className={cn(
            "inline-flex items-center gap-1 rounded-md border px-2 py-0.5 text-[10.5px] transition-colors duration-fast",
            disabled && "cursor-default opacity-50",
            isAllSelected
              ? "border-accent/40 bg-accent-soft text-accent-ink"
              : "border-border bg-surface text-ink-muted hover:border-accent/40 hover:text-ink",
          )}
          title={
            activeName
              ? `Use the config default (${activeName})`
              : "Use the config default"
          }
        >
          {isAllSelected && <Check size={10} />}
          Config default
        </button>
      </div>
      <div className="flex flex-wrap gap-1">
        {list.map((p) => {
          const checked = !isAllSelected && selected.includes(p.name);
          const isConfigActive = p.name === activeName;
          return (
            <button
              key={p.name}
              type="button"
              disabled={disabled}
              onClick={() => toggleProfile(p.name)}
              className={cn(
                "inline-flex items-center gap-1 rounded-md border px-2 py-0.5 font-mono text-[10.5px] transition-colors duration-fast",
                disabled && "cursor-default opacity-50",
                checked
                  ? "border-accent/40 bg-accent-soft text-accent-ink"
                  : isAllSelected && isConfigActive
                    ? "border-border bg-surface-subtle text-ink-muted"
                    : "border-border bg-surface text-ink-muted hover:border-accent/40 hover:text-ink",
              )}
              title={
                p.paths.length > 0
                  ? `${p.name} (${p.paths.length} path${p.paths.length === 1 ? "" : "s"})`
                  : p.name
              }
            >
              {checked && <Check size={10} />}
              {p.name}
            </button>
          );
        })}
      </div>
    </div>
  );
}
