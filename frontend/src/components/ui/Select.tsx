import { forwardRef, type SelectHTMLAttributes } from "react";
import { ChevronDown } from "lucide-react";

import { cn } from "../../lib/cn";

interface Props extends SelectHTMLAttributes<HTMLSelectElement> {
  invalid?: boolean;
}

const Select = forwardRef<HTMLSelectElement, Props>(function Select(
  { className, invalid, children, ...rest },
  ref,
) {
  return (
    <div className="relative inline-flex w-full">
      <select
        ref={ref}
        className={cn(
          "h-9 w-full appearance-none rounded-md border bg-surface-raised pl-3 pr-8 text-sm text-ink transition-colors duration-fast",
          "focus:border-accent focus:outline-none focus:ring-2 focus:ring-accent/20",
          "disabled:cursor-not-allowed disabled:opacity-60",
          invalid
            ? "border-critical focus:border-critical focus:ring-critical/20"
            : "border-border",
          className,
        )}
        {...rest}
      >
        {children}
      </select>
      <ChevronDown
        size={14}
        className="pointer-events-none absolute right-2.5 top-1/2 -translate-y-1/2 text-ink-dim"
      />
    </div>
  );
});

export default Select;
