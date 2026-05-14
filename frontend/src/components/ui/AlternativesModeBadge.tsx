/** Tiny badge surfacing the diversity mode of a run's alternatives.
 *  Hidden when the value is missing (legacy rows recorded before the
 *  column shipped, or single-alternative runs that never had a mode
 *  contract). Compact so it sits inline with the existing confidence +
 *  logprob row on RunDetail or in the run column header on RunsCompare.
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
      ? "Alternatives explore different MEANINGS"
      : "Alternatives are same-meaning phrasing variants";
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
