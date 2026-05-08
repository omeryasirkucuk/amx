import { useEffect, useRef, type ReactNode } from "react";
import { createPortal } from "react-dom";
import { X } from "lucide-react";

import { cn } from "../../lib/cn";
import IconButton from "./IconButton";

interface DialogProps {
  open: boolean;
  onClose: () => void;
  title?: ReactNode;
  description?: ReactNode;
  /** Footer content — typically action buttons. */
  footer?: ReactNode;
  /** Disable close-on-backdrop. Useful for destructive confirms. */
  preventBackdropClose?: boolean;
  /** Hide close icon button (header still renders title). */
  hideCloseButton?: boolean;
  /** Width preset. ``xl`` opts the panel into a flex column whose
   *  body scrolls — used by the Compare result modal where the inner
   *  tables can be much taller than the viewport. */
  size?: "sm" | "md" | "lg" | "xl";
  className?: string;
  children?: ReactNode;
}

const sizeClasses: Record<NonNullable<DialogProps["size"]>, string> = {
  sm: "max-w-sm",
  md: "max-w-md",
  lg: "max-w-2xl",
  xl: "max-w-[min(1200px,95vw)] max-h-[92vh] flex flex-col",
};

/**
 * Modal dialog. Uses a portal to <body>, traps focus inside while
 * open, restores focus on close, and dismisses on Escape. Backdrop
 * click closes unless `preventBackdropClose` is set.
 */
export default function Dialog({
  open,
  onClose,
  title,
  description,
  footer,
  preventBackdropClose,
  hideCloseButton,
  size = "md",
  className,
  children,
}: DialogProps) {
  const panelRef = useRef<HTMLDivElement | null>(null);
  const previouslyFocused = useRef<HTMLElement | null>(null);

  useEffect(() => {
    if (!open) return;
    previouslyFocused.current = document.activeElement as HTMLElement | null;
    // Move focus into the dialog after render.
    const t = setTimeout(() => {
      const focusable = panelRef.current?.querySelector<HTMLElement>(
        "[autofocus], button, [href], input, select, textarea, [tabindex]:not([tabindex='-1'])",
      );
      (focusable ?? panelRef.current)?.focus();
    }, 0);
    document.body.style.overflow = "hidden";
    return () => {
      clearTimeout(t);
      document.body.style.overflow = "";
      previouslyFocused.current?.focus?.();
    };
  }, [open]);

  useEffect(() => {
    if (!open) return;
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") {
        e.stopPropagation();
        onClose();
      } else if (e.key === "Tab") {
        const focusable = panelRef.current?.querySelectorAll<HTMLElement>(
          "button, [href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex='-1'])",
        );
        if (!focusable || focusable.length === 0) return;
        const first = focusable[0];
        const last = focusable[focusable.length - 1];
        if (e.shiftKey && document.activeElement === first) {
          last.focus();
          e.preventDefault();
        } else if (!e.shiftKey && document.activeElement === last) {
          first.focus();
          e.preventDefault();
        }
      }
    }
    window.addEventListener("keydown", onKey, true);
    return () => window.removeEventListener("keydown", onKey, true);
  }, [open, onClose]);

  if (!open) return null;

  const node = (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center px-4 py-8 animate-fade-in"
      role="presentation"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget && !preventBackdropClose) onClose();
      }}
    >
      <div
        className="absolute inset-0 bg-ink/40 backdrop-blur-sm"
        aria-hidden="true"
      />
      <div
        ref={panelRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={title ? "amx-dialog-title" : undefined}
        aria-describedby={description ? "amx-dialog-desc" : undefined}
        tabIndex={-1}
        className={cn(
          "relative w-full overflow-hidden rounded-2xl border border-border bg-surface-raised shadow-lg animate-scale-in",
          sizeClasses[size],
          className,
        )}
      >
        {(title || !hideCloseButton) && (
          <header
            className={cn(
              "flex items-start justify-between gap-4 px-5 pt-5",
              size === "xl" && "shrink-0",
            )}
          >
            <div className="min-w-0">
              {title && (
                <h2
                  id="amx-dialog-title"
                  className="text-base font-semibold text-ink"
                >
                  {title}
                </h2>
              )}
              {description && (
                <p
                  id="amx-dialog-desc"
                  className="mt-1 text-sm text-ink-muted"
                >
                  {description}
                </p>
              )}
            </div>
            {!hideCloseButton && (
              <IconButton
                icon={<X size={16} />}
                label="Close"
                size="sm"
                onClick={onClose}
                className="-mr-1 -mt-1"
              />
            )}
          </header>
        )}
        {children && (
          <div
            className={cn(
              "px-5 py-4 text-sm text-ink",
              size === "xl" && "min-h-0 flex-1 overflow-auto",
            )}
          >
            {children}
          </div>
        )}
        {footer && (
          <footer
            className={cn(
              "flex items-center justify-end gap-2 border-t border-border bg-surface-subtle/40 px-5 py-3",
              size === "xl" && "shrink-0",
            )}
          >
            {footer}
          </footer>
        )}
      </div>
    </div>
  );

  return createPortal(node, document.body);
}
