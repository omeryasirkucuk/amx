/** Tiny badge surfacing the diversity mode of a run's alternatives.
 *  Hidden when the value is missing (legacy rows recorded before the
 *  column shipped, or single-alternative runs that never had a mode
 *  contract). Compact so it sits inline with the existing confidence +
 *  logprob row on RunDetail or in the run column header on RunsCompare.
 *
 *  Per Definition 1 (NLP standard): semantic ⇒ same meaning / different
 *  words; lexical ⇒ shared vocabulary / shifted meaning. Do NOT
 *  re-invert.
 */
export default function AlternativesModeBadge({
  mode,
}: {
  mode?: "semantic" | "lexical" | null;
}) {
  if (mode !== "semantic" && mode !== "lexical") return null;
  const label = mode === "semantic" ? "Semantic" : "Lexical";
  const title =
    mode === "semantic"
      ? "Alternatives paraphrase the chosen description — same meaning, different wording"
      : "Alternatives share core vocabulary with the chosen description but may shift meaning";
  const tone =
    mode === "semantic"
      ? "border-accent/40 bg-accent-soft/40 text-accent"
      : "border-surface-border bg-surface text-ink-muted";
  return (
    <span
      title={title}
      className={
        "inline-flex items-center rounded border px-1.5 py-[1px] " +
        "text-[10px] uppercase tracking-wider " +
        tone
      }
    >
      {label}
    </span>
  );
}
