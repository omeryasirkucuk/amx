import type { ReactNode } from "react";

import { cn } from "../../lib/cn";

interface Props {
  children: ReactNode;
  className?: string;
}

/**
 * Keycap presentation. Use to surface keyboard shortcuts inline (e.g.
 * "Press <Kbd>⌘K</Kbd> to search"). Pure visual — does not bind keys.
 */
export default function Kbd({ children, className }: Props) {
  return (
    <kbd
      className={cn(
        "inline-flex h-5 min-w-[1.25rem] items-center justify-center rounded border border-border bg-surface-subtle px-1 font-mono text-[10.5px] font-medium text-ink-muted shadow-xs",
        className,
      )}
    >
      {children}
    </kbd>
  );
}
