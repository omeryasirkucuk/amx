import { cn } from "../../lib/cn";

interface Props {
  className?: string;
  /** Convenience preset matching common Tailwind utilities. */
  shape?: "line" | "block" | "circle";
}

/**
 * Animated placeholder. Replace "Loading…" text strings with shaped
 * skeletons that match the final layout so the page never reflows.
 *
 * Example: `<Skeleton className="h-4 w-32" />` for a label,
 * `<Skeleton shape="circle" className="h-8 w-8" />` for an avatar.
 */
export default function Skeleton({ className, shape = "line" }: Props) {
  const shapeClass =
    shape === "circle"
      ? "rounded-full"
      : shape === "block"
        ? "rounded-lg"
        : "rounded";
  return (
    <span
      aria-hidden="true"
      className={cn("amx-skeleton block", shapeClass, className)}
    />
  );
}
