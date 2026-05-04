import { cn } from "../lib/cn";

interface Props {
  tone?: "neutral" | "accent" | "positive" | "warning" | "critical";
  children: React.ReactNode;
}

export default function StatusPill({ tone = "neutral", children }: Props) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[11px] font-medium",
        tone === "neutral" && "bg-surface-subtle text-ink-muted",
        tone === "accent" && "bg-accent-soft text-accent-ink",
        tone === "positive" && "bg-positive/10 text-positive",
        tone === "warning" && "bg-warning/10 text-warning",
        tone === "critical" && "bg-critical/10 text-critical",
      )}
    >
      {children}
    </span>
  );
}
