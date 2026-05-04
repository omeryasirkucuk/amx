import { forwardRef, type InputHTMLAttributes } from "react";
import { Check, Minus } from "lucide-react";

import { cn } from "../../lib/cn";

interface Props extends Omit<InputHTMLAttributes<HTMLInputElement>, "type" | "size"> {
  label?: React.ReactNode;
  description?: React.ReactNode;
  indeterminate?: boolean;
}

const Checkbox = forwardRef<HTMLInputElement, Props>(function Checkbox(
  { label, description, indeterminate, className, checked, disabled, ...rest },
  ref,
) {
  return (
    <label
      className={cn(
        "group inline-flex cursor-pointer items-start gap-2.5",
        disabled && "cursor-not-allowed opacity-60",
        className,
      )}
    >
      <span className="relative mt-0.5 inline-flex h-4 w-4 shrink-0 items-center justify-center">
        <input
          ref={ref}
          type="checkbox"
          checked={checked}
          disabled={disabled}
          className="peer absolute inset-0 h-full w-full cursor-pointer opacity-0"
          {...rest}
        />
        <span
          aria-hidden="true"
          className={cn(
            "h-4 w-4 rounded border bg-surface-raised transition-colors duration-fast",
            "peer-focus-visible:ring-2 peer-focus-visible:ring-accent peer-focus-visible:ring-offset-1 peer-focus-visible:ring-offset-bg",
            checked || indeterminate
              ? "border-accent bg-accent"
              : "border-border-strong group-hover:border-ink-muted",
          )}
        />
        {(checked || indeterminate) && (
          <span className="pointer-events-none absolute inset-0 flex items-center justify-center text-white">
            {indeterminate ? <Minus size={11} strokeWidth={3} /> : <Check size={11} strokeWidth={3} />}
          </span>
        )}
      </span>
      {(label || description) && (
        <span className="flex flex-col gap-0.5 leading-tight">
          {label && <span className="text-sm text-ink">{label}</span>}
          {description && (
            <span className="text-xs text-ink-dim">{description}</span>
          )}
        </span>
      )}
    </label>
  );
});

export default Checkbox;
