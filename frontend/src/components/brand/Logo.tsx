import { cn } from "../../lib/cn";

interface Props {
  size?: number;
  /** Override accent color for the bottom bar. Defaults to var(--accent). */
  accent?: boolean;
  className?: string;
}

/**
 * AMX brand mark — three horizontal bars of decreasing width,
 * suggesting matrix rows / data tiers. The bottom bar uses the
 * accent color (amber by default) so the mark always carries
 * AMX's signature even on neutral surfaces. The two upper bars
 * inherit `currentColor` so the mark adapts to theme + context
 * (sidebar ink, hero overlays, etc.).
 */
export default function Logo({ size = 20, accent = true, className }: Props) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      role="img"
      aria-label="AMX"
      className={cn("shrink-0", className)}
    >
      <rect x="2" y="5" width="20" height="3" rx="1.5" fill="currentColor" />
      <rect
        x="2"
        y="10.5"
        width="14"
        height="3"
        rx="1.5"
        fill="currentColor"
        opacity="0.7"
      />
      <rect
        x="2"
        y="16"
        width="8"
        height="3"
        rx="1.5"
        fill={accent ? "rgb(var(--accent))" : "currentColor"}
      />
    </svg>
  );
}
