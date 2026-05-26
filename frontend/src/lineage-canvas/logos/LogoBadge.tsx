/**
 * Small inline logo badge for DataFrameNode header.
 *
 * Renders the logo (16×16) when a key is set, otherwise a faded
 * placeholder icon. Click opens the LogoPicker; the consumer wires
 * the pick handler.
 */

import { Image as ImageIcon } from "lucide-react";

import { pickLogoSrc, useLogoIndex } from "./registry";

interface Props {
  logoKey: string | undefined;
  onClick: () => void;
  /** When set, the badge becomes an external link (e.g. "open in
   *  Databricks") instead of the logo picker trigger. */
  href?: string;
}

export function LogoBadge({ logoKey, onClick, href }: Props) {
  const index = useLogoIndex();
  const row = logoKey ? index.get(logoKey) : undefined;
  const src = pickLogoSrc(row);
  const className =
    "nodrag flex h-5 w-5 items-center justify-center rounded border border-transparent text-fg-muted transition hover:border-surface-border hover:bg-surface";
  const content = src ? (
    <img
      src={src}
      alt={row?.label || logoKey || ""}
      className="h-4 w-4 object-contain"
      draggable={false}
    />
  ) : (
    <ImageIcon size={11} />
  );

  if (href) {
    return (
      <a
        href={href}
        target="_blank"
        rel="noopener noreferrer"
        onClick={(e) => e.stopPropagation()}
        className={className}
        title="Open in Databricks"
      >
        {content}
      </a>
    );
  }

  return (
    <button
      type="button"
      onClick={(e) => {
        e.stopPropagation();
        onClick();
      }}
      className={className}
      title={row ? `Logo: ${row.label}` : "Set logo badge"}
    >
      {content}
    </button>
  );
}
