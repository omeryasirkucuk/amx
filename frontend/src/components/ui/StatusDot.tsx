import { cn } from "../../lib/cn";

type Tone = "neutral" | "positive" | "warning" | "critical" | "info" | "accent";

interface Props {
  tone?: Tone;
  pulse?: boolean;
  size?: "sm" | "md";
  className?: string;
  label?: string;
}

const toneClasses: Record<Tone, string> = {
  neutral: "bg-ink-dim",
  positive: "bg-positive",
  warning: "bg-warning",
  critical: "bg-critical",
  info: "bg-info",
  accent: "bg-accent",
};

export default function StatusDot({
  tone = "neutral",
  pulse = false,
  size = "md",
  className,
  label,
}: Props) {
  const dim = size === "sm" ? "h-1.5 w-1.5" : "h-2 w-2";
  return (
    <span
      className={cn(
        "inline-block shrink-0 rounded-full",
        dim,
        toneClasses[tone],
        pulse && "amx-pulse",
        className,
      )}
      role={label ? "img" : undefined}
      aria-label={label}
      aria-hidden={label ? undefined : true}
    />
  );
}
