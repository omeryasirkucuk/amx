// InstallBanner — sticky top-right card that surfaces every backend
// pip install in flight. Wired to the global SSE bus at
// /api/installs/events (see amx/web/routers/installs.py); driven by
// pip.install.begin / progress / done / failed events that the
// installer in amx/utils/optional_deps.py publishes whenever a feature
// triggers a lazy import.
//
// Why a global banner instead of inline JobProgress on each Settings
// row: lazy installs fire from many call sites (DB connection test,
// LLM activate, /docs scan, BERTScore, weasyprint, …). Anchoring the
// UI to the SSE bus rather than to a specific button means every flow
// gets the same affordance for free, and a long install no longer
// looks like a hung request to whichever page the user happens to be
// on.

import { useEffect, useState } from "react";
import { Loader2, CheckCircle2, AlertCircle } from "lucide-react";

import { useEventSource, type SseEvent } from "../lib/sse";
import { cn } from "../lib/cn";

interface InstallState {
  installId: string;
  feature: string;
  packages: string[];
  phase: string;
  detail: string;
  startedAt: number;
  status: "running" | "done" | "failed";
  closingAt?: number;
  tail?: string[];
}

const PHASE_LABEL: Record<string, string> = {
  collecting: "Resolving",
  downloading: "Downloading",
  installing: "Installing",
  installed: "Finalizing",
  tail: "Working",
};

function formatElapsed(ms: number): string {
  if (ms < 1000) return "0s";
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const r = s % 60;
  return `${m}m ${r}s`;
}

function reduce(prev: Map<string, InstallState>, evt: SseEvent): Map<string, InstallState> {
  const installId = String(evt.install_id ?? "");
  if (!installId) return prev;
  const next = new Map(prev);
  const existing = next.get(installId);

  switch (evt.type) {
    case "pip.install.begin": {
      next.set(installId, {
        installId,
        feature: String(evt.feature ?? "extras"),
        packages: Array.isArray(evt.packages) ? (evt.packages as string[]) : [],
        phase: "starting",
        detail: "",
        startedAt: Date.now(),
        status: "running",
      });
      break;
    }
    case "pip.install.progress": {
      if (!existing) break;
      const phase = String(evt.phase ?? existing.phase);
      let detail = existing.detail;
      if (phase === "collecting") detail = String(evt.package ?? "");
      else if (phase === "downloading") {
        detail = String(evt.artifact ?? "");
        if (evt.size) detail += ` (${evt.size})`;
      } else if (phase === "installing") detail = String(evt.packages ?? "");
      else if (phase === "installed") detail = String(evt.installed ?? "");
      next.set(installId, { ...existing, phase, detail });
      break;
    }
    case "pip.install.done": {
      if (!existing) break;
      next.set(installId, {
        ...existing,
        status: "done",
        phase: "done",
        detail: "",
        closingAt: Date.now() + 2000,
      });
      break;
    }
    case "pip.install.failed": {
      if (!existing) break;
      next.set(installId, {
        ...existing,
        status: "failed",
        phase: "failed",
        detail: `pip exited ${evt.returncode ?? "?"}`,
        tail: Array.isArray(evt.tail) ? (evt.tail as string[]) : undefined,
        closingAt: Date.now() + 10000,
      });
      break;
    }
  }
  return next;
}

export default function InstallBanner() {
  // The events stream is global and never terminates (no job.done
  // sentinel) — keep the SSE hook subscribed for the full lifetime of
  // the app shell. terminalTypes is set to an empty tuple so the hook
  // never auto-closes.
  const { events } = useEventSource({
    path: "/api/installs/events",
    terminalTypes: [],
  });

  const [installs, setInstalls] = useState<Map<string, InstallState>>(new Map());
  const [, tick] = useState(0);

  // Fold every event we have ever seen into the install map. The hook
  // caps its events array at MAX_EVENTS so the fold cost stays bounded
  // even for a long-lived session.
  useEffect(() => {
    setInstalls((prev) => {
      let next = prev;
      for (const evt of events) {
        next = reduce(next, evt);
      }
      return next;
    });
  }, [events]);

  // Sweep finished installs after their closingAt deadline, and keep
  // the elapsed counter ticking on running rows.
  useEffect(() => {
    if (installs.size === 0) return;
    const t = setInterval(() => {
      const now = Date.now();
      let mutated = false;
      const next = new Map(installs);
      for (const [id, s] of installs) {
        if (s.closingAt && now >= s.closingAt) {
          next.delete(id);
          mutated = true;
        }
      }
      if (mutated) setInstalls(next);
      else tick((n) => n + 1);
    }, 500);
    return () => clearInterval(t);
  }, [installs]);

  if (installs.size === 0) return null;

  return (
    <div
      className="fixed right-4 top-4 z-40 flex w-80 flex-col gap-2"
      role="status"
      aria-live="polite"
    >
      {Array.from(installs.values()).map((s) => (
        <InstallCard key={s.installId} state={s} />
      ))}
    </div>
  );
}

function InstallCard({ state }: { state: InstallState }) {
  const elapsed = formatElapsed(Date.now() - state.startedAt);
  const running = state.status === "running";
  const done = state.status === "done";
  const failed = state.status === "failed";

  return (
    <div
      className={cn(
        "rounded-md border bg-surface-raised shadow-md transition-all",
        running && "border-accent/40",
        done && "border-positive/40",
        failed && "border-critical/40",
      )}
    >
      <div className="flex items-start gap-2 p-3">
        <div className="mt-0.5 shrink-0">
          {running && <Loader2 size={16} className="animate-spin text-accent" />}
          {done && <CheckCircle2 size={16} className="text-positive" />}
          {failed && <AlertCircle size={16} className="text-critical" />}
        </div>
        <div className="min-w-0 flex-1">
          <div className="flex items-center justify-between gap-2">
            <div className="truncate text-xs font-medium text-ink">
              {running && `Installing libraries for ${state.feature}`}
              {done && `Installed libraries for ${state.feature}`}
              {failed && `Install failed for ${state.feature}`}
            </div>
            <div className="shrink-0 text-[10px] tabular-nums text-muted">{elapsed}</div>
          </div>
          {running && (state.detail || state.phase) && (
            <div className="mt-1 truncate text-[11px] text-muted">
              {PHASE_LABEL[state.phase] ?? state.phase}
              {state.detail ? ` · ${state.detail}` : ""}
            </div>
          )}
          {failed && state.tail && state.tail.length > 0 && (
            <pre className="mt-2 max-h-32 overflow-auto rounded bg-surface-subtle p-2 text-[10px] text-muted">
              {state.tail.slice(-6).join("\n")}
            </pre>
          )}
        </div>
      </div>
    </div>
  );
}
