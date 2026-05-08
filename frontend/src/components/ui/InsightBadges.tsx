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
