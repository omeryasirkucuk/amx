import { forwardRef, type ButtonHTMLAttributes, type ReactNode } from "react";

import { cn } from "../../lib/cn";

type Variant = "ghost" | "subtle" | "primary" | "danger";
type Size = "sm" | "md" | "lg";

interface Props extends ButtonHTMLAttributes<HTMLButtonElement> {
  icon: ReactNode;
  /** Required for accessibility — IconButton has no visible label. */
  label: string;
  variant?: Variant;
  size?: Size;
}

const sizeClasses: Record<Size, string> = {
  sm: "h-6 w-6",
  md: "h-8 w-8",
  lg: "h-10 w-10",
};

const variantClasses: Record<Variant, string> = {
  ghost:
    "bg-transparent text-ink-muted hover:bg-surface-subtle hover:text-ink",
  subtle:
    "bg-surface-subtle text-ink-muted hover:bg-border hover:text-ink",
  primary:
    "bg-accent text-white hover:bg-accent-ink",
  danger:
    "bg-critical text-white hover:opacity-90",
};

const IconButton = forwardRef<HTMLButtonElement, Props>(function IconButton(
  { icon, label, variant = "ghost", size = "md", className, ...rest },
  ref,
) {
  return (
    <button
      ref={ref}
      type="button"
      aria-label={label}
      title={label}
      className={cn(
        "inline-flex shrink-0 items-center justify-center rounded-md transition-colors duration-fast",
        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent focus-visible:ring-offset-1 focus-visible:ring-offset-bg",
        "disabled:cursor-not-allowed disabled:opacity-50",
        sizeClasses[size],
        variantClasses[variant],
        className,
      )}
      {...rest}
    >
      {icon}
    </button>
  );
});

export default IconButton;
