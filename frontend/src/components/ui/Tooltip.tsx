import {
  cloneElement,
  isValidElement,
  useEffect,
  useId,
  useRef,
  useState,
  type ReactElement,
  type ReactNode,
} from "react";

import { cn } from "../../lib/cn";

type Placement = "top" | "bottom" | "left" | "right";

interface Props {
  content: ReactNode;
  placement?: Placement;
  /** Open delay in ms. */
  delay?: number;
  /** Disable the tooltip — useful when content is empty. */
  disabled?: boolean;
  className?: string;
  children: ReactElement;
}

/**
 * Lightweight tooltip — hover/focus triggered, ESC dismisses. Not a
 * full Radix replacement (no collision detection, no portal) but
 * sufficient for the visualizer's TopBar pills, IconButton labels,
 * and inline help icons. Place tooltips on elements with positioning
 * context; the trigger gets `position: relative` automatically.
 */
export default function Tooltip({
  content,
  placement = "top",
  delay = 250,
  disabled,
  className,
  children,
}: Props) {
  const [open, setOpen] = useState(false);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const tooltipId = useId();

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") setOpen(false);
    }
    if (!open) return;
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open]);

  function show() {
    if (disabled || !content) return;
    if (timerRef.current) clearTimeout(timerRef.current);
    timerRef.current = setTimeout(() => setOpen(true), delay);
  }
  function hide() {
    if (timerRef.current) clearTimeout(timerRef.current);
    setOpen(false);
  }

  const placementClasses: Record<Placement, string> = {
    top: "bottom-full left-1/2 mb-1.5 -translate-x-1/2",
    bottom: "top-full left-1/2 mt-1.5 -translate-x-1/2",
    left: "right-full top-1/2 mr-1.5 -translate-y-1/2",
    right: "left-full top-1/2 ml-1.5 -translate-y-1/2",
  };

  if (!isValidElement(children)) return children;

  const triggerProps = children.props as Record<string, unknown>;
  const trigger = cloneElement(children, {
    onMouseEnter: (e: React.MouseEvent) => {
      show();
      const orig = triggerProps.onMouseEnter as
        | ((e: React.MouseEvent) => void)
        | undefined;
      orig?.(e);
    },
    onMouseLeave: (e: React.MouseEvent) => {
      hide();
      const orig = triggerProps.onMouseLeave as
        | ((e: React.MouseEvent) => void)
        | undefined;
      orig?.(e);
    },
    onFocus: (e: React.FocusEvent) => {
      show();
      const orig = triggerProps.onFocus as
        | ((e: React.FocusEvent) => void)
        | undefined;
      orig?.(e);
    },
    onBlur: (e: React.FocusEvent) => {
      hide();
      const orig = triggerProps.onBlur as
        | ((e: React.FocusEvent) => void)
        | undefined;
      orig?.(e);
    },
    "aria-describedby": open ? tooltipId : undefined,
  } as Record<string, unknown>);

  return (
    <span className="relative inline-flex">
      {trigger}
      {open && (
        <span
          id={tooltipId}
          role="tooltip"
          className={cn(
            "pointer-events-none absolute z-50 max-w-xs whitespace-nowrap rounded-md bg-ink px-2 py-1 text-[11px] font-medium text-bg shadow-md animate-fade-in",
            placementClasses[placement],
            className,
          )}
        >
          {content}
        </span>
      )}
    </span>
  );
}
