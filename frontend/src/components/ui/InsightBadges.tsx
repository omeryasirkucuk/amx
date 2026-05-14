// Shared visual vocabulary for surfacing per-suggestion confidence
// and logprob across multiple pages. Both helpers used to live as
// private functions inside ``RunDetail.tsx``; pulling them into a
// common module so the Compare page (and any future surface that
// renders run_results cells) reads with the same glyphs / tones as
// the run-detail page.
//
// Behaviour is intentionally byte-for-byte identical to the original
// inline copies -- this is a pure refactor with no styling changes.

import StatusPill from "../StatusPill";

/** Numeric logprob_score. Closer to 0 means the model was very
 *  confident in its top alternative; very negative means it was
 *  guessing. Renders ``logprob —`` when no score was recorded
 *  (legacy run_results rows or providers that don't expose
 *  logprobs). */
export function LogprobBadge({ score }: { score: number | null }) {
  if (score == null) {
    return (
      <span
        className="font-mono text-[10px] text-ink-dim"
        title="No logprob recorded for this suggestion."
      >
        logprob —
      </span>
    );
  }
  return (
    <span
      className="font-mono text-[10px] text-ink-muted"
      title="Average log-probability — closer to 0 = more confident; very negative = the model was guessing."
    >
      logprob {score.toFixed(3)}
    </span>
  );
}

/** Categorical confidence band ("high" | "medium" | "low") rendered
 *  as a coloured StatusPill. Shows the underlying logprob in the
 *  hover title when present so reviewers can correlate the band with
 *  the raw signal without leaving the page. */
export function ConfidencePill({
  value,
  score,
}: {
  value: string;
  score: number | null;
}) {
  const tone =
    value === "high" ? "positive" : value === "low" ? "warning" : "neutral";
  return (
    <span title={score != null ? `logprob ${score.toFixed(3)}` : undefined}>
      <StatusPill tone={tone}>{value}</StatusPill>
    </span>
  );
}

/** Per-alternative band visual vocabulary. Lifted out of RunDetail so
 *  RunsCompare and any future surface that renders ``alternatives_json``
 *  rows reads with the same glyphs / tones. */
export const ALT_BAND_STYLES: Record<
  string,
  { stripe: string; pill: string; label: string }
> = {
  HIGH: {
    stripe: "bg-positive",
    pill: "border border-positive/40 bg-positive-soft text-positive",
    label: "HIGH",
  },
  MED: {
    stripe: "bg-warning",
    pill: "border border-warning/40 bg-warning-soft text-warning",
    label: "MED",
  },
  LOW: {
    stripe: "bg-critical",
    pill: "border border-critical/40 bg-critical-soft text-critical",
    label: "LOW",
  },
};

/** Short tag the per-alternative pill prefixes (``SC: HIGH 0.78``) so
 *  the reader always knows which scorer drove the value. */
export const ALT_SIGNAL_ABBREV: Record<string, string> = {
  logprob: "LP",
  self_consistency: "SC",
  self_decl: "SD",
  judge: "JU",
};

/** Compact per-alternative score pill rendered in stacked alternatives
 *  lists. The label is ``{SIGNAL_ABBREV}: {band} {score}`` so the user
 *  always knows which scorer drove the value — e.g. ``SC: HIGH 0.78``.
 *  Falls back to no pill when the alternative carries no band (legacy
 *  ``list[str]`` rows or runs whose ``confidence_signal`` is ``"none"``).
 *
 *  Distinct from the rich ``ConfidenceBadge`` in RunDetail which adds a
 *  click-to-expand popup with the full signal breakdown; this compact
 *  variant is the right fit for narrow Compare cells. */
export function AltScoreBadge({
  band,
  score,
  signal,
}: {
  band?: string | null;
  score?: number | null;
  signal?: string | null;
}) {
  const upper = (band || "").toUpperCase();
  if (!upper || !ALT_BAND_STYLES[upper]) return null;
  const style = ALT_BAND_STYLES[upper];
  const scoreText = typeof score === "number" ? score.toFixed(2) : null;
  const abbrev = signal ? ALT_SIGNAL_ABBREV[signal] ?? null : null;
  return (
    <span
      className={
        "inline-flex shrink-0 items-center gap-1 rounded-full px-2 py-0.5 " +
        "text-[10px] font-medium " +
        style.pill
      }
    >
      {abbrev && <span className="opacity-70">{abbrev}:</span>}
      {style.label}
      {scoreText && <span className="opacity-70">{scoreText}</span>}
    </span>
  );
}
