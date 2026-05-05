import { useEffect, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { Check, ChevronDown, Plus } from "lucide-react";

import { apiFetch } from "../../lib/api";
import { cn } from "../../lib/cn";

type Kind = "db" | "llm";

interface ProfileSummary {
  name: string;
  is_active: boolean;
  /** Backend (e.g. "postgresql") for DB; provider (e.g. "openrouter") for LLM. */
  backend?: string;
  provider?: string;
  /** Shown as the secondary line in the dropdown. */
  model?: string;
  database?: string;
  catalog?: string;
  host?: string;
}

interface ProfilesResponse {
  profiles: ProfileSummary[];
}

interface Props {
  kind: Kind;
  label: string;
  /** Currently-active profile name (or null). */
  activeName: string | null | undefined;
  /** Tooltip on the trigger (backend / model). */
  tooltip?: string | undefined;
  /** Trigger style — "pill" for compact TopBar, "row" for full-width sidebar. */
  variant?: "pill" | "row";
}

/**
 * Dropdown trigger that exposes every saved DB or LLM profile for
 * one-click activation. Two trigger styles share the same dropdown
 * body: a compact pill (TopBar) and a full-width row (Sidebar). The
 * first menu item jumps to Settings to add a new profile, mirroring
 * the CLI's /db add and /llm add flows.
 */
export default function ProfilePicker({
  kind,
  label,
  activeName,
  tooltip,
  variant = "pill",
}: Props) {
  const [open, setOpen] = useState(false);
  const wrapperRef = useRef<HTMLDivElement | null>(null);
  const queryClient = useQueryClient();
  const navigate = useNavigate();

  const profiles = useQuery({
    queryKey: ["profiles", kind, "list"],
    queryFn: () => apiFetch<ProfilesResponse>(`/api/profiles/${kind}`),
    enabled: open,
    retry: false,
    staleTime: 30_000,
  });

  const activate = useMutation({
    mutationFn: (name: string) =>
      apiFetch(`/api/profiles/${kind}/${encodeURIComponent(name)}/activate`, {
        method: "POST",
      }),
    onSuccess: () => {
      // After switching, every downstream query needs to re-fetch:
      // catalogs/databases/schemas all key off the active profile.
      queryClient.invalidateQueries({ queryKey: ["context"] });
      queryClient.invalidateQueries({ queryKey: ["profiles", kind] });
      queryClient.invalidateQueries({ queryKey: ["live-catalogs"] });
      queryClient.invalidateQueries({ queryKey: ["live-databases"] });
      queryClient.invalidateQueries({ queryKey: ["live-schemas"] });
      queryClient.invalidateQueries({ queryKey: ["live-assets"] });
      queryClient.invalidateQueries({ queryKey: ["recent-runs"] });
      setOpen(false);
    },
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

  const display = activeName || `Select ${label.toLowerCase()}`;
  const list = profiles.data?.profiles ?? [];

  return (
    <div className="relative" ref={wrapperRef}>
      {variant === "row" ? (
        <button
          type="button"
          onClick={() => setOpen((v) => !v)}
          title={tooltip}
          aria-haspopup="listbox"
          aria-expanded={open}
          className="flex w-full items-center justify-between gap-2 rounded px-2 py-0.5 text-left transition-colors duration-fast hover:bg-surface-subtle"
        >
          <span className="text-[10px] uppercase tracking-wider text-ink-dim">
            {label}
          </span>
          <span className="flex min-w-0 items-center gap-1">
            <span
              className={cn(
                "max-w-[10rem] truncate font-mono text-[11px]",
                activeName ? "text-ink" : "text-warning",
              )}
            >
              {display}
            </span>
            <ChevronDown size={11} className="text-ink-dim" />
          </span>
        </button>
      ) : (
        <button
          type="button"
          onClick={() => setOpen((v) => !v)}
          title={tooltip}
          className={cn(
            "inline-flex h-7 items-center gap-1.5 rounded-md border px-2 font-medium transition-colors duration-fast",
            activeName
              ? "border-accent/20 bg-accent-soft text-accent-ink hover:bg-accent-soft/80"
              : "border-warning/40 bg-warning-soft text-warning hover:bg-warning-soft/80",
          )}
        >
          <span className="text-[10px] uppercase tracking-wider opacity-70">
            {label}
          </span>
          <span className="max-w-[8rem] truncate font-mono text-[11px]">
            {display}
          </span>
          <ChevronDown size={12} className="opacity-70" />
        </button>
      )}
      {open && (
        <div
          className={cn(
            "absolute z-30 mt-1 w-72 overflow-hidden rounded-md border border-border bg-surface-raised shadow-md animate-fade-in",
            variant === "row" ? "left-0" : "right-0",
          )}
        >
          <button
            type="button"
            onClick={() => {
              setOpen(false);
              navigate(`/settings?tab=${kind}`);
            }}
            className="flex w-full items-center gap-2 border-b border-border px-3 py-2 text-left text-xs font-medium text-accent-ink hover:bg-accent-soft/40"
          >
            <Plus size={13} />
            Add {label.toLowerCase()} profile
          </button>
          {profiles.isLoading ? (
            <div className="px-3 py-3 text-xs text-ink-dim">Loading profiles…</div>
          ) : profiles.error ? (
            <div className="px-3 py-3 text-xs text-critical">
              {(profiles.error as Error).message}
            </div>
          ) : list.length === 0 ? (
            <div className="px-3 py-3 text-xs text-ink-dim">
              No {label.toLowerCase()} profiles saved yet.
            </div>
          ) : (
            <ul className="max-h-72 overflow-y-auto py-1">
              {list.map((p) => {
                const isActive = p.is_active || p.name === activeName;
                const secondary = subtitleFor(kind, p);
                return (
                  <li key={p.name}>
                    <button
                      type="button"
                      onClick={() => activate.mutate(p.name)}
                      disabled={activate.isPending || isActive}
                      className={cn(
                        "flex w-full items-start justify-between gap-2 px-3 py-1.5 text-left text-xs hover:bg-surface-subtle disabled:cursor-default",
                        isActive && "bg-surface-subtle/60",
                      )}
                    >
                      <span className="min-w-0 flex-1">
                        <span className="block truncate font-mono text-ink">
                          {p.name}
                        </span>
                        {secondary && (
                          <span className="block truncate text-[10.5px] text-ink-dim">
                            {secondary}
                          </span>
                        )}
                      </span>
                      {isActive && <Check size={12} className="mt-0.5 text-accent" />}
                    </button>
                  </li>
                );
              })}
            </ul>
          )}
          {activate.isError && (
            <div className="border-t border-border px-3 py-1.5 text-[11px] text-critical">
              {activate.error instanceof Error
                ? activate.error.message
                : "Activation failed."}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function subtitleFor(kind: Kind, p: ProfileSummary): string {
  if (kind === "db") {
    const parts = [p.backend, p.host, p.database || p.catalog].filter(Boolean);
    return parts.join(" · ");
  }
  // LLM
  const parts = [p.provider, p.model].filter(Boolean);
  return parts.join(" · ");
}
