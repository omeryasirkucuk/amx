/**
 * LogoNode — standalone external-system node.
 *
 * Represents an external system (Power BI, Tableau, S3, …) on the
 * canvas. Square card with the brand logo centered and an editable
 * label below. Connectable via left/right handles like a table.
 *
 * The logo image is resolved from the registry by ``data.logoKey`` so
 * a custom upload that shadows a default (same key, source='custom')
 * automatically swaps in on next render.
 */

import { memo, useState } from "react";
import { Handle, NodeProps, NodeResizer, Position, useReactFlow } from "reactflow";
import clsx from "clsx";

import { pickLogoSrc, useLogoIndex } from "../logos/registry";
import type { LogoNodeData } from "../types";

function LogoNodeImpl({ id, data, selected }: NodeProps<LogoNodeData>) {
  const rf = useReactFlow();
  const index = useLogoIndex();
  const row = data.logoKey ? index.get(data.logoKey) : undefined;
  const src = pickLogoSrc(row);
  const [label, setLabel] = useState<string>(data.label || row?.label || "");

  function commitLabel(next: string) {
    setLabel(next);
    rf.setNodes((nodes) =>
      nodes.map((n) =>
        n.id === id ? { ...n, data: { ...n.data, label: next } } : n,
      ),
    );
  }

  return (
    <div
      className={clsx(
        "relative flex h-full w-full flex-col items-center justify-between rounded-lg border bg-surface-raised p-2 text-ink shadow-lg",
        selected ? "border-accent-default" : "border-surface-border",
      )}
    >
      <NodeResizer
        isVisible={selected}
        minWidth={96}
        minHeight={96}
        keepAspectRatio
      />
      <Handle
        type="target"
        position={Position.Left}
        id="in"
        className="lcv-handle"
        style={{ background: "rgb(var(--accent))" }}
      />
      <div className="flex flex-1 items-center justify-center overflow-hidden">
        {src ? (
          <img
            src={src}
            alt={row?.label || data.logoKey || ""}
            className="max-h-full max-w-full object-contain"
            draggable={false}
          />
        ) : (
          <span className="font-mono text-[11px] text-fg-muted">
            (missing logo)
          </span>
        )}
      </div>
      <input
        value={label}
        onChange={(e) => commitLabel(e.target.value)}
        placeholder={row?.label || data.logoKey || ""}
        spellCheck={false}
        className="nodrag mt-1 w-full rounded bg-transparent text-center text-[11px] text-ink outline-none placeholder:text-fg-muted"
      />
      <Handle
        type="source"
        position={Position.Right}
        id="out"
        className="lcv-handle"
        style={{ background: "rgb(var(--accent))" }}
      />
    </div>
  );
}

export const LogoNode = memo(LogoNodeImpl);
