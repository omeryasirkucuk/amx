import { forwardRef, type InputHTMLAttributes, type ReactNode } from "react";

import { cn } from "../../lib/cn";

interface Props extends InputHTMLAttributes<HTMLInputElement> {
  invalid?: boolean;
  leadingIcon?: ReactNode;
  trailingIcon?: ReactNode;
}

const Input = forwardRef<HTMLInputElement, Props>(function Input(
  { className, invalid, leadingIcon, trailingIcon, ...rest },
  ref,
) {
  if (leadingIcon || trailingIcon) {
    return (
      <div
        className={cn(
          "flex h-9 items-center gap-2 rounded-md border bg-surface-raised px-3 transition-colors duration-fast",
          "focus-within:border-accent focus-within:ring-2 focus-within:ring-accent/20",
          invalid
            ? "border-critical focus-within:border-critical focus-within:ring-critical/20"
            : "border-border",
          className,
        )}
      >
        {leadingIcon && (
          <span className="text-ink-dim">{leadingIcon}</span>
        )}
        <input
          ref={ref}
          className="h-full flex-1 bg-transparent text-sm text-ink placeholder:text-ink-dim focus:outline-none disabled:cursor-not-allowed disabled:opacity-60"
          {...rest}
        />
        {trailingIcon && (
          <span className="text-ink-dim">{trailingIcon}</span>
        )}
      </div>
    );
  }
  return (
    <input
      ref={ref}
      className={cn(
        "h-9 w-full rounded-md border bg-surface-raised px-3 text-sm text-ink transition-colors duration-fast",
        "placeholder:text-ink-dim",
        "focus:border-accent focus:outline-none focus:ring-2 focus:ring-accent/20",
        "disabled:cursor-not-allowed disabled:opacity-60",
        invalid
          ? "border-critical focus:border-critical focus:ring-critical/20"
          : "border-border",
        className,
      )}
      {...rest}
    />
  );
});

export default Input;
