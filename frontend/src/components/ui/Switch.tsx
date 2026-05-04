import { forwardRef, type InputHTMLAttributes } from "react";

import { cn } from "../../lib/cn";

interface Props extends Omit<InputHTMLAttributes<HTMLInputElement>, "type" | "size"> {
  label?: React.ReactNode;
  description?: React.ReactNode;
}

const Switch = forwardRef<HTMLInputElement, Props>(function Switch(
  { label, description, className, checked, disabled, ...rest },
  ref,
) {
  return (
    <label
      className={cn(
        "group inline-flex cursor-pointer items-start gap-3",
        disabled && "cursor-not-allowed opacity-60",
        className,
      )}
    >
      <span className="relative inline-flex h-5 w-9 shrink-0 items-center">
        <input
          ref={ref}
          type="checkbox"
          role="switch"
          checked={checked}
          disabled={disabled}
          className="peer absolute inset-0 h-full w-full cursor-pointer opacity-0"
          {...rest}
        />
        <span
          aria-hidden="true"
          className={cn(
            "h-5 w-9 rounded-full border transition-colors duration-fast",
            "peer-focus-visible:ring-2 peer-focus-visible:ring-accent peer-focus-visible:ring-offset-1 peer-focus-visible:ring-offset-bg",
            checked
              ? "border-accent bg-accent"
              : "border-border-strong bg-surface-subtle group-hover:border-ink-muted",
          )}
        />
        <span
          aria-hidden="true"
          className={cn(
            "pointer-events-none absolute top-0.5 h-4 w-4 rounded-full bg-white shadow-xs transition-all duration-fast",
            checked ? "left-[18px]" : "left-0.5",
          )}
        />
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

export default Switch;
