/**
 * ProfileFilterChips — multi-select chip bar for cross-profile filtering
 * in the Lineage, History, and Pages list views.
 *
 * Props:
 *   selected  — array of active profile names. Empty array = "all profiles".
 *   onChange  — called with the new selection.
 *   profiles  — list of profile names to render chips for.
 *
 * Preset buttons:
 *   "All profiles"  — clears selection (shows everything)
 *   "Mine only"     — filters to currentUser's profile
 *   "Others"        — filters to profiles not owned by currentUser
 *
 * Mobile-responsive: chips wrap to multiple lines on narrow screens;
 * labels are truncated via sm: / md: breakpoints.
 */

import { Check, Users } from "lucide-react";
import { cn } from "../lib/cn";

interface Props {
  /** The full list of DB profile names available in this workspace. */
  profiles: string[];
  /** Currently active filter. Empty array = all profiles (no filter). */
  selected: string[];
  onChange: (next: string[]) => void;
  /** The current user's "primary" profile name, used for Mine / Others presets. */
  currentProfile?: string | null;
  className?: string;
}

/**
 * Chip bar that lets users narrow a multi-profile list to a subset of
 * DB profiles. Used on the Lineage saved-list, History runs, and Pages
 * list surfaces. The component is purely presentational — callers own
 * the selected state and wire it into React Query keys.
 */
export default function ProfileFilterChips({
  profiles,
  selected,
  onChange,
  currentProfile,
  className,
}: Props) {
  const isAll = selected.length === 0;

  function toggleProfile(name: string) {
    if (selected.includes(name)) {
      const next = selected.filter((p) => p !== name);
      onChange(next);
    } else {
      onChange([...selected, name]);
    }
  }

  function selectAll() {
    onChange([]);
  }

  function selectMine() {
    if (currentProfile) onChange([currentProfile]);
    else onChange([]);
  }

  function selectOthers() {
    if (!currentProfile) return;
    onChange(profiles.filter((p) => p !== currentProfile));
  }

  if (profiles.length === 0) return null;

  return (
    <div className={cn("flex flex-wrap items-center gap-1.5", className)}>
      {/* Preset: All profiles */}
      <button
        type="button"
        onClick={selectAll}
        className={cn(
          "inline-flex items-center gap-1 rounded-md border px-2 py-0.5 text-[10.5px] font-medium transition-colors duration-fast",
          isAll
            ? "border-accent/40 bg-accent-soft text-accent-ink"
            : "border-border bg-surface text-ink-muted hover:border-accent/40 hover:text-ink",
        )}
        title="Show all profiles"
      >
        {isAll && <Check size={10} />}
        <Users size={10} className={cn(!isAll && "opacity-60")} />
        <span className="hidden sm:inline">All profiles</span>
        <span className="sm:hidden">All</span>
      </button>

      {/* Preset: Mine only */}
      {currentProfile && (
        <button
          type="button"
          onClick={selectMine}
          className={cn(
            "inline-flex items-center gap-1 rounded-md border px-2 py-0.5 text-[10.5px] transition-colors duration-fast",
            !isAll && selected.length === 1 && selected[0] === currentProfile
              ? "border-accent/40 bg-accent-soft text-accent-ink"
              : "border-border bg-surface text-ink-muted hover:border-accent/40 hover:text-ink",
          )}
          title={`Show only ${currentProfile}`}
        >
          Mine only
        </button>
      )}

      {/* Preset: Others */}
      {currentProfile && profiles.length > 1 && (
        <button
          type="button"
          onClick={selectOthers}
          className={cn(
            "inline-flex items-center gap-1 rounded-md border px-2 py-0.5 text-[10.5px] transition-colors duration-fast",
            !isAll &&
              selected.every((p) => p !== currentProfile) &&
              selected.length > 0
              ? "border-accent/40 bg-accent-soft text-accent-ink"
              : "border-border bg-surface text-ink-muted hover:border-accent/40 hover:text-ink",
          )}
          title="Show profiles from other users"
        >
          Others
        </button>
      )}

      {/* Separator */}
      <span className="h-4 border-l border-border" aria-hidden="true" />

      {/* Individual profile chips */}
      {profiles.map((name) => {
        const active = selected.includes(name);
        return (
          <button
            key={name}
            type="button"
            onClick={() => toggleProfile(name)}
            className={cn(
              "inline-flex items-center gap-1 rounded-md border px-2 py-0.5 font-mono text-[10.5px] transition-colors duration-fast",
              active
                ? "border-accent/40 bg-accent-soft text-accent-ink"
                : "border-border bg-surface text-ink-muted hover:border-accent/40 hover:text-ink",
            )}
            title={name}
          >
            {active && <Check size={10} />}
            <span className="max-w-[8rem] truncate sm:max-w-[12rem]">{name}</span>
          </button>
        );
      })}
    </div>
  );
}
