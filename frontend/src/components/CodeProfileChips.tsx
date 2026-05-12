import { useQuery } from "@tanstack/react-query";
import { Check, Code2 } from "lucide-react";

import { apiFetch } from "../lib/api";
import { cn } from "../lib/cn";

interface CodeProfileSummary {
  name: string;
  path: string;
  is_active: boolean;
  linked_db_profiles?: string[];
}

interface CodeProfilesResponse {
  profiles: CodeProfileSummary[];
  active: string | null;
}

interface Props {
  /** Currently selected code profiles. ``null`` = use config default
   *  (cfg.active_code_profile / cfg.run_code_profiles on disk). An
   *  explicit empty array means "no code retrieval for this run". */
  selected: string[] | null;
  onChange: (next: string[] | null) => void;
  disabled?: boolean;
}

/**
 * Multi-select chip row for code profiles, rendered inside the Run
 * dialog. PR δ (code-RAG hardening) — lets the user pick which code
 * profiles feed the Code agent for *this* run only without flipping
 * the active profile on disk. Structurally identical to
 * :component:`DocProfileChips` so the dialog reads symmetrically.
 */
export default function CodeProfileChips({ selected, onChange, disabled }: Props) {
  const profiles = useQuery({
    queryKey: ["profiles", "code", "list-for-run"],
    queryFn: () => apiFetch<CodeProfilesResponse>("/api/profiles/code"),
    staleTime: 30_000,
  });
  const list = profiles.data?.profiles ?? [];
  const activeName = profiles.data?.active ?? null;
  const isAllSelected = selected === null;

  if (profiles.isLoading) {
    return (
      <div className="mt-3 text-[11px] text-ink-dim">Loading code profiles…</div>
    );
  }
  if (profiles.error) {
    return null; // silent — the run still works without a code override
  }
  if (list.length === 0) {
    return (
      <div className="mt-3 inline-flex items-center gap-1.5 rounded-md bg-surface-subtle px-2 py-1 text-[11px] text-ink-dim">
        <Code2 size={11} className="opacity-70" />
        No code profiles configured.
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
          Code profiles
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
              title={p.path ? `${p.name} (${p.path})` : p.name}
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
