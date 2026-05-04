import type { ReactNode, ComponentType, SVGProps } from "react";

import { cn } from "../lib/cn";

interface Props {
  icon?: ComponentType<SVGProps<SVGSVGElement>>;
  title: ReactNode;
  description?: ReactNode;
  actions?: ReactNode;
  className?: string;
}

// A single empty-state component every list view shares so the SPA
// renders consistent "nothing here yet" panels with explainers and a
// CTA. The icon, copy and actions vary; the centered layout doesn't.
export default function EmptyState({
  icon: Icon,
  title,
  description,
  actions,
  className,
}: Props) {
  return (
    <div
      className={cn(
        "flex flex-col items-center justify-center rounded-xl border border-dashed border-surface-border bg-surface-subtle/40 px-8 py-16 text-center",
        className,
      )}
    >
      {Icon && (
        <div className="mb-4 inline-flex h-12 w-12 items-center justify-center rounded-full bg-accent-soft text-accent-ink">
          <Icon className="h-6 w-6" />
        </div>
      )}
      <h3 className="text-base font-semibold">{title}</h3>
      {description && (
        <p className="mt-1.5 max-w-md text-sm text-ink-muted">{description}</p>
      )}
      {actions && <div className="mt-5 flex items-center justify-center gap-2">{actions}</div>}
    </div>
  );
}
