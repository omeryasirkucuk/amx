import type { ReactNode } from "react";

import Badge, { type BadgeTone } from "./ui/Badge";

interface Props {
  tone?: BadgeTone;
  children: ReactNode;
}

/**
 * Compatibility shim: StatusPill is the original badge component
 * used across the routes. New code should import `Badge` directly
 * from `components/ui`. This wrapper keeps the existing call sites
 * (RunsList, Home, etc.) working without an immediate rewrite.
 */
export default function StatusPill({ tone = "neutral", children }: Props) {
  return <Badge tone={tone}>{children}</Badge>;
}
