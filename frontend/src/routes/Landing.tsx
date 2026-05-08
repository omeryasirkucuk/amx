import { useEffect, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import {
  ArrowRight,
  Database,
  History as HistoryIcon,
  PlayCircle,
  ScrollText,
  Sparkles,
} from "lucide-react";

import Logo from "../components/brand/Logo";
import StatusPill from "../components/StatusPill";
import { api, apiFetch, type RunRow } from "../lib/api";
import { cn } from "../lib/cn";
import {
  humanizeCommand,
  relativeTime,
  shortModel,
  statusLabel,
  statusTone,
  summarizeScope,
} from "../lib/runDisplay";
import { useUi } from "../lib/store";

// AMX Studio's calm operations entry. Lives at the bare ``/`` route
// (the historical Overview dashboard moved to ``/overview``). The
// Studio user is already past the "what is AMX" question -- they
// pip-installed it and ran ``amx studio``. What they want on first
// paint is a small "you're in your workspace, here's the next
// natural step" surface, not a marketing splash and not a stat
// dashboard. amxcli.com (mkdocs Material) covers the marketing
// audience; this page is intentionally narrower in scope.
//
// Layout
// 1. Hero -- pixel-art wordmark + one-line tagline.
// 2. Status badge -- which LLM model + DB profile count are
//    configured. Adds a pending-review chip when the user has runs
//    waiting on approval (only when count > 0; never an empty
//    "0 pending" placeholder).
// 3. Four action surfaces -- Browse / New run / Ask / Audit. Each
//    card is a full-bleed click target (the whole card is the link),
//    not a nested button-in-card, so a one-pixel mouse move counts.
// 4. Recent activity -- five most recent runs across every command
//    kind. Hidden when empty so a fresh install isn't padded with
//    a "(none)" stub.
// 5. Footer link to ``/overview`` for users who want the stat
//    dashboard view (token / cost / success rate breakdown).

interface DbProfileSummary {
  name: string;
  backend?: string;
}
interface DbProfilesResponse {
  profiles: DbProfileSummary[];
}

export default function Landing() {
  // The Landing is a calm entry surface; the sidebar tree is the
  // browse affordance for everything else but on this page it
  // mostly just visually crowds the hero band. Tuck it away by
  // default; the topbar Toggle Sidebar control still exposes the
  // tree for users who want to jump straight into a profile.
  const setSidebarCollapsed = useUi((s) => s.setSidebarCollapsed);
  useEffect(() => {
    setSidebarCollapsed(true);
  }, [setSidebarCollapsed]);

  const ctx = useQuery({
    queryKey: ["context"],
    queryFn: () => api.context(),
  });
  const profiles = useQuery({
    queryKey: ["profiles", "db", "landing"],
    queryFn: () => apiFetch<DbProfilesResponse>("/api/profiles/db"),
    retry: false,
  });
  const recent = useQuery({
    queryKey: ["recent-runs", "landing"],
    queryFn: () => api.recentRuns(8, "all"),
    retry: false,
  });

  const profileCount = profiles.data?.profiles?.length ?? 0;
  const hasLlm = !!ctx.data?.llm_model;

  // Pending review hint: count runs that finished with
  // ``ready_for_review`` (analyze / rerun / generate flows). Aggregated
  // from the recent-runs feed -- good enough for the landing chip
  // without adding a dedicated endpoint.
  const pendingReviewCount = useMemo(() => {
    const runs = recent.data?.runs ?? [];
    return runs.filter((r) => (r.status ?? "") === "ready_for_review").length;
  }, [recent.data]);

  // Audit card subtitle hint: count cross-user applies in the last
  // bunch. Today the SPA already pulls /api/history/apply-events for
  // the Audit page; we re-use the same endpoint here at limit=20 so
  // the landing chip is honest without a custom aggregator.
  const applyEvents = useQuery({
    queryKey: ["apply-events", "landing"],
    queryFn: () => api.applyEvents({ limit: 20 }),
    retry: false,
  });
  const recentAppliesCount = applyEvents.data?.events?.length ?? 0;

  const recentRuns = (recent.data?.runs ?? []).slice(0, 5);

  return (
    <div className="mx-auto max-w-3xl space-y-10 py-10">
      {/* Hero -- brand mark + one-line tagline. The wordmark uses
          the same pixel-art glyph the docs site / topbar share, so
          the Studio feels like a continuation of the marketing
          surface without copying its copy. Centered horizontally so
          the entry surface reads as a deliberate "you've arrived"
          moment rather than a left-pinned form header. */}
      <header className="flex flex-col items-center space-y-3 text-center">
        <div className="flex items-baseline gap-2">
          <Logo size={28} />
          <span className="text-2xl font-semibold tracking-tight text-ink">
            Studio
          </span>
        </div>
        <p className="text-base text-ink-muted">
          Your workspace for AI-inferred database metadata.
        </p>
      </header>

      {/* Status line -- concrete grounding. "Connected to X, with Y
          DB profiles" answers "is anything ready" without dumping a
          stat dashboard. Centered so it reads as a single "what's
          ready" line under the hero, rather than a left-pinned
          status bar. */}
      <section className="flex flex-wrap items-center justify-center gap-2 text-sm">
        {hasLlm ? (
          <span className="inline-flex items-center gap-1.5 rounded-full border border-surface-border bg-surface-subtle/40 px-3 py-1 font-mono text-xs text-ink-muted">
            <span className="h-1.5 w-1.5 rounded-full bg-positive" />
            LLM · {shortModel(ctx.data?.llm_model)}
          </span>
        ) : (
          <Link
            to="/settings"
            className="inline-flex items-center gap-1.5 rounded-full border border-warning/40 bg-warning-soft/40 px-3 py-1 text-xs text-warning-ink hover:bg-warning-soft/60"
          >
            ⚠ No LLM profile yet — open Settings to add one
          </Link>
        )}
        {profileCount > 0 ? (
          <span className="inline-flex items-center gap-1.5 rounded-full border border-surface-border bg-surface-subtle/40 px-3 py-1 font-mono text-xs text-ink-muted">
            {profileCount} DB profile{profileCount === 1 ? "" : "s"}
          </span>
        ) : (
          <Link
            to="/settings"
            className="inline-flex items-center gap-1.5 rounded-full border border-warning/40 bg-warning-soft/40 px-3 py-1 text-xs text-warning-ink hover:bg-warning-soft/60"
          >
            ⚠ No DB profile yet — open Settings to add one
          </Link>
        )}
        {pendingReviewCount > 0 && (
          <Link
            to="/runs"
            className="inline-flex items-center gap-1.5 rounded-full border border-accent/40 bg-accent-soft/40 px-3 py-1 text-xs text-accent-ink hover:bg-accent-soft/60"
          >
            {pendingReviewCount} pending review
          </Link>
        )}
      </section>

      {/* Action grid. Each card is the click target; no nested
          buttons. Disabled cards (e.g. New run / Ask without an LLM
          profile) drop the link wrapper so the visual stays but the
          tile is unreachable until the user fixes the precondition. */}
      <section className="grid gap-3 md:grid-cols-2">
        <ActionCard
          to={profileCount > 0 ? "/" : "/settings"}
          icon={Database}
          title={profileCount > 0 ? "Browse" : "Add your first DB profile"}
          description={
            profileCount > 0
              ? "Walk the catalog tree — pick a database, schema, or table."
              : "Tell Studio where your tables live so the rest of the workspace lights up."
          }
          tone={profileCount > 0 ? "default" : "warning"}
          hint={profileCount > 0 ? "Sidebar" : undefined}
        />
        <ActionCard
          to={hasLlm && profileCount > 0 ? "/runs/new" : null}
          icon={PlayCircle}
          title="New run"
          description="Generate descriptions for a picked schema or set of tables."
          disabledReason={
            !hasLlm
              ? "Configure an LLM profile in Settings first."
              : profileCount === 0
                ? "Add a DB profile in Settings first."
                : null
          }
        />
        <ActionCard
          to={hasLlm ? "/ask" : null}
          icon={Sparkles}
          title="Ask"
          description="Question your catalog in natural language. Tool-calling agent answers from the indexed metadata."
          disabledReason={
            !hasLlm ? "Configure an LLM profile in Settings first." : null
          }
        />
        <ActionCard
          to="/audit"
          icon={ScrollText}
          title="Audit"
          description="Recent applies and the change history for every column AMX has written to."
          hint={
            recentAppliesCount > 0
              ? `${recentAppliesCount} recent event${recentAppliesCount === 1 ? "" : "s"}`
              : undefined
          }
        />
      </section>

      {/* Recent activity -- the workspace memory. Only renders when
          the user has actually run something; a fresh install sees
          the action grid + a clear path to the first action and
          nothing else. */}
      {recentRuns.length > 0 && (
        <section>
          <div className="flex items-baseline justify-between">
            <h2 className="text-sm font-semibold text-ink">Recent activity</h2>
            <Link
              to="/runs"
              className="text-xs text-accent hover:text-accent-ink"
            >
              View all →
            </Link>
          </div>
          <ul className="mt-2 divide-y divide-surface-border rounded-md border border-surface-border bg-surface-raised">
            {recentRuns.map((row) => (
              <RecentRow key={row.id} row={row} />
            ))}
          </ul>
        </section>
      )}

      {/* Footer escape hatch to the dashboard for users who want the
          stat tiles + token / cost view. Intentionally muted -- it's
          a "if you want it" affordance, not a primary CTA. */}
      <footer className="border-t border-surface-border pt-4 text-xs text-ink-dim">
        <Link
          to="/overview"
          className="inline-flex items-center gap-1 hover:text-ink"
        >
          Open the Overview dashboard for token + cost details
          <ArrowRight size={11} />
        </Link>
      </footer>
    </div>
  );
}

interface ActionCardProps {
  to: string | null;
  icon: typeof Database;
  title: string;
  description: string;
  /** Optional small uppercase eyebrow on the right (e.g. ``Sidebar``,
   *  ``2 sessions``). Hidden when not provided. */
  hint?: string;
  /** Renders the card as warning-toned (e.g. "no profile yet") so
   *  the user's eye is drawn to the unfinished setup step. */
  tone?: "default" | "warning";
  /** When set, the card renders un-clickable + greyed out. The
   *  reason becomes the body text so the user knows what to fix. */
  disabledReason?: string | null;
}

function ActionCard({
  to,
  icon: Icon,
  title,
  description,
  hint,
  tone = "default",
  disabledReason,
}: ActionCardProps) {
  const isDisabled = !to || !!disabledReason;
  const body = (
    <>
      <div className="flex items-center gap-2">
        <Icon
          size={16}
          className={cn(
            tone === "warning" ? "text-warning" : "text-accent",
            isDisabled && "text-ink-dim",
          )}
        />
        <span
          className={cn(
            "text-base font-semibold",
            isDisabled ? "text-ink-muted" : "text-ink",
          )}
        >
          {title}
        </span>
        {hint && (
          <span className="ml-auto text-[10px] uppercase tracking-wider text-ink-dim">
            {hint}
          </span>
        )}
      </div>
      <p className={cn("mt-1.5 text-sm", isDisabled ? "text-ink-dim" : "text-ink-muted")}>
        {disabledReason || description}
      </p>
    </>
  );
  const className = cn(
    "block rounded-xl border bg-surface-raised px-4 py-3 transition-colors duration-fast",
    tone === "warning"
      ? "border-warning/40 hover:border-warning/60"
      : "border-surface-border",
    isDisabled
      ? "cursor-not-allowed opacity-60"
      : "hover:border-accent/40 hover:bg-surface-subtle/40",
  );
  if (isDisabled) {
    return (
      <div className={className} aria-disabled="true">
        {body}
      </div>
    );
  }
  return (
    <Link to={to as string} className={className}>
      {body}
    </Link>
  );
}

function RecentRow({ row }: { row: RunRow }) {
  return (
    <li>
      <Link
        to={`/runs/${row.id}`}
        className="flex items-center gap-3 px-4 py-2 text-sm hover:bg-surface-subtle/50"
      >
        <HistoryIcon size={13} className="shrink-0 text-ink-dim" />
        <span className="font-mono text-xs text-ink-dim">#{row.id}</span>
        <span className="font-medium text-ink truncate" title={row.command}>
          {humanizeCommand(row.command, row.scope_json ?? row.scope)}
        </span>
        <span className="truncate text-ink-muted text-xs">
          · {summarizeScope(row.scope_json ?? row.scope)}
        </span>
        <span className="ml-auto inline-flex items-center gap-2">
          {row.started_at != null && (
            <span className="hidden font-mono text-[11px] text-ink-dim md:inline">
              {relativeTime(row.started_at)}
            </span>
          )}
          <StatusPill tone={statusTone(row.status)}>
            {statusLabel(row.status)}
          </StatusPill>
        </span>
      </Link>
    </li>
  );
}
