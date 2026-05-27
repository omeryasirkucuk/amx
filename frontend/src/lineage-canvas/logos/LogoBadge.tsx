/**
 * Small inline logo badge for DataFrameNode header.
 *
 * Renders the logo (16×16) when a key is set, otherwise a faded
 * placeholder icon. Click opens the LogoPicker; the consumer wires
 * the pick handler. The badge is a decorative source-system indicator
 * (e.g. the Databricks brand), not a link.
 */

import { Image as ImageIcon } from "lucide-react";

import { pickLogoSrc, useLogoIndex } from "./registry";

interface Props {
  logoKey: string | undefined;
  onClick: () => void;
}

export function LogoBadge({ logoKey, onClick }: Props) {
  const index = useLogoIndex();
  const row = logoKey ? index.get(logoKey) : undefined;
  const src = pickLogoSrc(row);
  return (
    <button
      type="button"
      onClick={(e) => {
        e.stopPropagation();
        onClick();
      }}
      className="nodrag flex h-5 w-5 items-center justify-center rounded border border-transparent text-fg-muted transition hover:border-surface-border hover:bg-surface"
      title={row ? `Logo: ${row.label}` : "Set logo badge"}
    >
      {src ? (
        <img
          src={src}
          alt={row?.label || logoKey || ""}
          className="h-4 w-4 object-contain"
          draggable={false}
        />
      ) : (
        <ImageIcon size={11} />
      )}
    </button>
  );
}
