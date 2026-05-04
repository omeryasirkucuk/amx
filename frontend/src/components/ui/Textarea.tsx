import { forwardRef, type TextareaHTMLAttributes } from "react";

import { cn } from "../../lib/cn";

interface Props extends TextareaHTMLAttributes<HTMLTextAreaElement> {
  invalid?: boolean;
}

const Textarea = forwardRef<HTMLTextAreaElement, Props>(function Textarea(
  { className, invalid, rows = 4, ...rest },
  ref,
) {
  return (
    <textarea
      ref={ref}
      rows={rows}
      className={cn(
        "block w-full resize-y rounded-md border bg-surface-raised px-3 py-2 text-sm text-ink transition-colors duration-fast",
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

export default Textarea;
