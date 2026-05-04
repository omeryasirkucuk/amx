import type { ReactNode } from "react";

import { cn } from "../../lib/cn";

export type BadgeTone =
  | "neutral"
  | "accent"
  | "positive"
  | "warning"
  | "critical"
  | "info";

interface Props {
  tone?: BadgeTone;
  /** Adds a small leading dot (good for status badges). */
  dot?: boolean;
  /** Pulse animation on the dot — for live/running states. */
  pulse?: boolean;
  size?: "sm" | "md";
  className?: string;
  children: ReactNode;
}

const toneClasses: Record<BadgeTone, string> = {
  neutral: "bg-surface-subtle text-ink-muted",
  accent: "bg-accent-soft text-accent-ink",
  positive: "bg-positive-soft text-positive",
  warning: "bg-warning-soft text-warning",
  critical: "bg-critical-soft text-critical",
  info: "bg-info-soft text-info",
};

const dotToneClasses: Record<BadgeTone, string> = {
  neutral: "bg-ink-dim",
  accent: "bg-accent",
  positive: "bg-positive",
  warning: "bg-warning",
  critical: "bg-critical",
  info: "bg-info",
};

export default function Badge({
  tone = "neutral",
  dot = false,
  pulse = false,
  size = "md",
  className,
  children,
}: Props) {
  const sizeClass =
    size === "sm"
      ? "px-1.5 py-0 text-[10px]"
      : "px-2 py-0.5 text-[11px]";
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 rounded-full font-medium",
        sizeClass,
        toneClasses[tone],
        className,
      )}
    >
      {dot && (
        <span
          className={cn(
            "h-1.5 w-1.5 shrink-0 rounded-full",
            dotToneClasses[tone],
            pulse && "amx-pulse",
          )}
          aria-hidden="true"
        />
      )}
      {children}
    </span>
  );
}
