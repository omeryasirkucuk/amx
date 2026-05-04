import { forwardRef, type ButtonHTMLAttributes, type ReactNode } from "react";
import { Loader2 } from "lucide-react";

import { cn } from "../../lib/cn";

export type ButtonVariant = "primary" | "secondary" | "ghost" | "danger" | "subtle";
export type ButtonSize = "sm" | "md" | "lg";

interface Props extends Omit<ButtonHTMLAttributes<HTMLButtonElement>, "type"> {
  variant?: ButtonVariant;
  size?: ButtonSize;
  loading?: boolean;
  leadingIcon?: ReactNode;
  trailingIcon?: ReactNode;
  fullWidth?: boolean;
  type?: "button" | "submit" | "reset";
}

const sizeClasses: Record<ButtonSize, string> = {
  sm: "h-7 px-2.5 text-xs gap-1.5",
  md: "h-8 px-3 text-sm gap-2",
  lg: "h-10 px-4 text-[15px] gap-2",
};

const variantClasses: Record<ButtonVariant, string> = {
  primary:
    "bg-accent text-white hover:bg-accent-ink active:bg-accent-ink shadow-xs disabled:bg-ink-dim disabled:text-white",
  secondary:
    "bg-surface-raised text-ink border border-border hover:bg-surface-subtle active:bg-surface-subtle disabled:text-ink-dim",
  ghost:
    "bg-transparent text-ink-muted hover:bg-surface-subtle hover:text-ink active:bg-surface-subtle disabled:text-ink-dim",
  danger:
    "bg-critical text-white hover:opacity-90 active:opacity-95 shadow-xs disabled:opacity-50",
  subtle:
    "bg-surface-subtle text-ink-muted hover:bg-border hover:text-ink active:bg-border disabled:text-ink-dim",
};

const Button = forwardRef<HTMLButtonElement, Props>(function Button(
  {
    variant = "secondary",
    size = "md",
    loading = false,
    leadingIcon,
    trailingIcon,
    fullWidth,
    className,
    disabled,
    children,
    type = "button",
    ...rest
  },
  ref,
) {
  const isDisabled = disabled || loading;
  return (
    <button
      ref={ref}
      type={type}
      disabled={isDisabled}
      className={cn(
        "inline-flex shrink-0 items-center justify-center rounded-md font-medium transition-colors duration-fast",
        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent focus-visible:ring-offset-1 focus-visible:ring-offset-bg",
        "disabled:cursor-not-allowed",
        sizeClasses[size],
        variantClasses[variant],
        fullWidth && "w-full",
        className,
      )}
      {...rest}
    >
      {loading ? (
        <Loader2 className="h-3.5 w-3.5 animate-spin" />
      ) : (
        leadingIcon
      )}
      {children}
      {trailingIcon}
    </button>
  );
});

export default Button;
