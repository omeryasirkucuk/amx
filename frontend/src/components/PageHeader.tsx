import type { ReactNode } from "react";

import { cn } from "../lib/cn";

interface Props {
  eyebrow?: string;
  title: ReactNode;
  description?: ReactNode;
  actions?: ReactNode;
  className?: string;
}

// Consistent page-header band so every route looks like part of the
// same product. Gives titles / breadcrumbs / action buttons one
// vertical rhythm instead of each route inventing its own.
export default function PageHeader({
  eyebrow,
  title,
  description,
  actions,
  className,
}: Props) {
  return (
    <header className={cn("mb-6 flex items-start justify-between gap-6", className)}>
      <div>
        {eyebrow && (
          <div className="mb-1 text-[11px] font-semibold uppercase tracking-[0.08em] text-ink-dim">
            {eyebrow}
          </div>
        )}
        <h1 className="text-2xl font-semibold tracking-tight">{title}</h1>
        {description && (
          <p className="mt-1.5 max-w-2xl text-sm text-ink-muted">{description}</p>
        )}
      </div>
      {actions && <div className="flex shrink-0 items-center gap-2">{actions}</div>}
    </header>
  );
}
