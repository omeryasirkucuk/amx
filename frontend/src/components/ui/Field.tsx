import { useId, type ReactNode, cloneElement, isValidElement } from "react";

import { cn } from "../../lib/cn";

interface Props {
  label?: ReactNode;
  description?: ReactNode;
  /** Inline error message — pass falsy when valid. */
  error?: ReactNode;
  /** Mark visually as required (no validation here). */
  required?: boolean;
  /** Optional hint shown right of the label. */
  hint?: ReactNode;
  className?: string;
  /**
   * Single form control. Field will inject `id` + `aria-invalid` +
   * `aria-describedby` so the label and error wire up automatically.
   */
  children: ReactNode;
}

/**
 * Form field wrapper — replaces the verbose label+input+helptext
 * markup that was repeated across Settings, RunNew, Ask. Accepts any
 * single child input; injects `id` + ARIA wiring so consumers don't
 * have to build it by hand.
 */
export default function Field({
  label,
  description,
  error,
  required,
  hint,
  className,
  children,
}: Props) {
  const id = useId();
  const errorId = `${id}-error`;
  const descId = `${id}-desc`;

  let control = children;
  if (isValidElement(children)) {
    const props = children.props as Record<string, unknown>;
    const describedBy = [
      error ? errorId : null,
      description ? descId : null,
      props["aria-describedby"] as string | undefined,
    ]
      .filter(Boolean)
      .join(" ") || undefined;
    control = cloneElement(children, {
      id: (props.id as string | undefined) ?? id,
      "aria-invalid": error ? true : (props["aria-invalid"] as boolean | undefined),
      "aria-describedby": describedBy,
      invalid: !!error || (props.invalid as boolean | undefined),
    } as Record<string, unknown>);
  }

  return (
    <div className={cn("flex flex-col gap-1.5", className)}>
      {(label || hint) && (
        <div className="flex items-baseline justify-between gap-2">
          {label && (
            <label
              htmlFor={id}
              className="text-xs font-medium text-ink-muted"
            >
              {label}
              {required && (
                <span className="ml-0.5 text-critical" aria-hidden="true">
                  *
                </span>
              )}
            </label>
          )}
          {hint && <span className="text-[11px] text-ink-dim">{hint}</span>}
        </div>
      )}
      {control}
      {description && !error && (
        <p id={descId} className="text-xs text-ink-dim">
          {description}
        </p>
      )}
      {error && (
        <p id={errorId} className="text-xs text-critical">
          {error}
        </p>
      )}
    </div>
  );
}
