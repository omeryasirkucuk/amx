import { Info } from "lucide-react";

import { cn } from "../../lib/cn";
import Tooltip from "./Tooltip";

interface Props {
  /** The explanation surfaced on hover/focus. Keep it to one short line. */
  text: string;
  /** Tooltip placement; defaults to "top". */
  placement?: "top" | "bottom" | "left" | "right";
  className?: string;
  /** Override icon size (default 11px). */
  size?: number;
}

/**
 * Tiny information affordance — a 12px circled "i" that reveals a
 * short explanation on hover or keyboard focus. Use sparingly next
 * to technical labels (StatCard headers, picker pills, form options
 * with non-obvious effects).
 */
export default function InfoHint({
  text,
  placement = "top",
  className,
  size = 11,
}: Props) {
  return (
    <Tooltip content={text} placement={placement}>
      <span
        role="button"
        tabIndex={0}
        aria-label={typeof text === "string" ? text : "More info"}
        className={cn(
          "inline-flex items-center justify-center rounded-full text-ink-dim transition-colors duration-fast hover:text-ink-muted focus:text-ink-muted focus:outline-none",
          className,
        )}
      >
        <Info size={size} aria-hidden="true" />
      </span>
    </Tooltip>
  );
}
