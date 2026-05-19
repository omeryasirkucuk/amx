// Popover wrapped around the "Re-generate" button.
// Lets the user attach an optional steering prompt before firing
// the generation. The steering text is folded into the saved
// version note so the user can scan it in the versions drawer.
// Submits via the caller-supplied onSubmit so the actual mutation
// stays owned by the route (which holds the loading flag and toast
// integration).

import { useEffect, useRef, useState } from "react";
import { RefreshCw, Wand2 } from "lucide-react";

import { cn } from "../../lib/cn";

interface Props {
  pending: boolean;
  onSubmit: (steering: string) => void;
}

export default function RegeneratePopover({ pending, onSubmit }: Props) {
  const [open, setOpen] = useState(false);
  const [steering, setSteering] = useState("");
  const ref = useRef<HTMLDivElement | null>(null);

  // Close on outside click so the popover stays cheap to dismiss.
  useEffect(() => {
    if (!open) return;
    function onClickAway(e: MouseEvent) {
      if (!ref.current) return;
      if (!ref.current.contains(e.target as Node)) setOpen(false);
    }
    window.addEventListener("mousedown", onClickAway);
    return () => window.removeEventListener("mousedown", onClickAway);
  }, [open]);

  function submit(e: React.FormEvent) {
    e.preventDefault();
    onSubmit(steering.trim());
    setOpen(false);
  }

  return (
    <div ref={ref} className="relative w-full">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        disabled={pending}
        aria-haspopup="dialog"
        aria-expanded={open}
        className={cn(
          "inline-flex w-full items-center justify-center gap-1.5 rounded-md border px-3 py-1.5 text-xs font-medium transition-colors",
          pending
            ? "cursor-not-allowed border-border bg-surface text-ink-dim"
            : "border-border bg-surface text-ink hover:border-accent/40 hover:bg-surface-subtle",
        )}
      >
        {pending ? (
          <RefreshCw size={12} className="animate-spin" />
        ) : (
          <Wand2 size={12} />
        )}
        Re-generate
      </button>
      {open && (
        <form
          onSubmit={submit}
          role="dialog"
          aria-label="Re-generate page"
          className="absolute right-0 z-20 mt-1 w-72 rounded-md border border-border bg-surface-raised p-3 shadow-md"
        >
          <label className="block text-[11px] font-medium text-ink-muted">
            Steering prompt
            <span className="ml-1 font-normal text-ink-dim">(optional)</span>
          </label>
          <textarea
            autoFocus
            value={steering}
            onChange={(e) => setSteering(e.target.value)}
            placeholder="e.g. focus on column-level transforms"
            rows={3}
            className="mt-1 w-full rounded border border-border bg-surface px-2 py-1.5 text-xs text-ink focus:border-accent/60 focus:outline-none"
          />
          <p className="mt-1 text-[10px] text-ink-dim">
            Leaves the original intent in place; the steering text rides
            on the next version row so you can compare runs later.
          </p>
          <div className="mt-2 flex items-center justify-end gap-2">
            <button
              type="button"
              onClick={() => setOpen(false)}
              className="rounded px-2 py-1 text-[11px] text-ink-muted hover:text-ink"
            >
              Cancel
            </button>
            <button
              type="submit"
              className="inline-flex items-center gap-1 rounded bg-accent px-3 py-1 text-[11px] font-medium text-white hover:bg-accent/90"
            >
              <Wand2 size={11} />
              Generate
            </button>
          </div>
        </form>
      )}
    </div>
  );
}
