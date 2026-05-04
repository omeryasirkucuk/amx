import type { ComponentType, ReactNode, SVGProps } from "react";

import { cn } from "../lib/cn";

interface Props {
  icon?: ComponentType<SVGProps<SVGSVGElement>>;
  title: ReactNode;
  description?: ReactNode;
  actions?: ReactNode;
  className?: string;
  /** Compact: smaller padding for embedded contexts (table footers etc.). */
  compact?: boolean;
}

/**
 * Empty state — minimal, left-aligned, low-key. Replaces the old
 * dashed-border + accent-circle decoration that telegraphed
 * "AI-generated dashboard" at a glance. Use a thin border to mark
 * the boundary; let the type carry the message.
 */
export default function EmptyState({
  icon: Icon,
  title,
  description,
  actions,
  className,
  compact,
}: Props) {
  return (
    <div
      className={cn(
        "flex items-start gap-3 rounded-lg border border-border bg-surface-subtle/40 text-ink",
        compact ? "px-4 py-4" : "px-5 py-6",
        className,
      )}
    >
      {Icon && (
        <span className="mt-0.5 inline-flex h-7 w-7 shrink-0 items-center justify-center rounded-md bg-surface-raised text-ink-dim">
          <Icon className="h-4 w-4" />
        </span>
      )}
      <div className="min-w-0 flex-1">
        <h3 className="text-sm font-semibold text-ink">{title}</h3>
        {description && (
          <p className="mt-1 text-sm text-ink-muted">{description}</p>
        )}
        {actions && <div className="mt-3 flex flex-wrap gap-2">{actions}</div>}
      </div>
    </div>
  );
}
