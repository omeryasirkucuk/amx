import type { ReactNode } from "react";
import { Link } from "react-router-dom";
import { ChevronRight } from "lucide-react";

import { cn } from "../lib/cn";

export interface Crumb {
  label: ReactNode;
  to?: string;
}

interface Props {
  title: ReactNode;
  /** Opt-in only. Most pages should omit this — long blurbs read as
      filler. Reserve for the rare page that genuinely needs context. */
  description?: ReactNode;
  actions?: ReactNode;
  breadcrumbs?: Crumb[];
  /** @deprecated — kept for backwards compatibility while routes still
      pass it. Renders as a small uppercase label above the title.
      New code should rely on breadcrumbs instead. */
  eyebrow?: ReactNode;
  className?: string;
}

/**
 * Page header: breadcrumb + title + optional actions. Description is
 * intentionally opt-in (and rare) — most routes communicate purpose
 * through their data and surrounding chrome, not a paragraph above it.
 */
export default function PageHeader({
  title,
  description,
  actions,
  breadcrumbs,
  eyebrow,
  className,
}: Props) {
  return (
    <header className={cn("mb-6 flex flex-col gap-2", className)}>
      {eyebrow && !breadcrumbs?.length && (
        <div className="text-[10.5px] font-semibold uppercase tracking-[0.08em] text-ink-dim">
          {eyebrow}
        </div>
      )}
      {breadcrumbs && breadcrumbs.length > 0 && (
        <nav aria-label="Breadcrumb">
          <ol className="flex flex-wrap items-center gap-1 text-xs text-ink-dim">
            {breadcrumbs.map((c, i) => (
              <li key={i} className="flex items-center gap-1">
                {i > 0 && (
                  <ChevronRight
                    size={12}
                    className="text-ink-dim/60"
                    aria-hidden="true"
                  />
                )}
                {c.to && i < breadcrumbs.length - 1 ? (
                  <Link
                    to={c.to}
                    className="rounded text-ink-muted hover:text-ink"
                  >
                    {c.label}
                  </Link>
                ) : (
                  <span
                    aria-current={i === breadcrumbs.length - 1 ? "page" : undefined}
                    className={
                      i === breadcrumbs.length - 1 ? "text-ink-muted" : undefined
                    }
                  >
                    {c.label}
                  </span>
                )}
              </li>
            ))}
          </ol>
        </nav>
      )}
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between sm:gap-6">
        <div className="min-w-0">
          <h1 className="text-[22px] font-semibold tracking-tight text-ink">
            {title}
          </h1>
          {description && (
            <p className="mt-1.5 max-w-2xl text-sm text-ink-muted">
              {description}
            </p>
          )}
        </div>
        {actions && (
          <div className="flex flex-wrap items-center gap-2 sm:shrink-0">
            {actions}
          </div>
        )}
      </div>
    </header>
  );
}
