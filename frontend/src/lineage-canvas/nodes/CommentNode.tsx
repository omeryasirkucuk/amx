/**
 * CommentNode — resizable sticky-note annotation.
 *
 * The textarea uses ``nodrag`` so the canvas drag handler doesn't
 * steal pointerdown; the color swatch row carries
 * ``lcv-comment-grip`` (the node's drag handle, see registry.ts) so
 * the user can still drag the note from its header band. Every keystroke
 * commits back to canvas state via :func:`useReactFlow().setNodes` so
 * Save persists the latest text.
 */

import { memo, useState } from "react";
import { NodeProps, NodeResizer, useReactFlow } from "reactflow";
import clsx from "clsx";

import { COMMENT_COLORS } from "../constants";
import type { CommentNodeData } from "../types";

const PALETTE_ORDER: Array<keyof typeof COMMENT_COLORS> = [
  "amber",
  "rose",
  "emerald",
  "sky",
  "violet",
  "slate",
];

function CommentNodeImpl({ id, data, selected }: NodeProps<CommentNodeData>) {
  const rf = useReactFlow();
  const [paletteOpen, setPaletteOpen] = useState(false);
  const [text, setText] = useState<string>(data.text || "");
  const [color, setColor] = useState<keyof typeof COMMENT_COLORS>(
    (data.color as keyof typeof COMMENT_COLORS) || "amber",
  );
  const palette = COMMENT_COLORS[color] ?? COMMENT_COLORS.amber;

  function commitText(next: string) {
    setText(next);
    rf.setNodes((nodes) =>
      nodes.map((n) =>
        n.id === id ? { ...n, data: { ...n.data, text: next } } : n,
      ),
    );
  }

  function commitColor(next: keyof typeof COMMENT_COLORS) {
    setColor(next);
    setPaletteOpen(false);
    rf.setNodes((nodes) =>
      nodes.map((n) =>
        n.id === id ? { ...n, data: { ...n.data, color: next } } : n,
      ),
    );
  }

  return (
    <div
      className={clsx(
        "relative h-full w-full overflow-hidden rounded-lg shadow-xl",
        selected ? "ring-2 ring-accent-default" : "",
      )}
      style={{
        background: palette.bg,
        border: `2px solid ${palette.border}`,
      }}
    >
      <NodeResizer
        isVisible={selected}
        minWidth={140}
        minHeight={100}
        lineStyle={{ borderColor: palette.border }}
      />
      <div className="lcv-comment-grip flex h-6 items-center justify-between border-b border-white/10 bg-white/5 px-2 text-[10px] text-ink-muted">
        <span className="cursor-grab select-none uppercase tracking-wide">note</span>
        <button
          type="button"
          aria-label="Change color"
          className="nodrag h-3.5 w-3.5 rounded-full border border-white/30"
          style={{ background: palette.border }}
          onClick={(e) => {
            e.stopPropagation();
            setPaletteOpen((v) => !v);
          }}
        />
      </div>
      {paletteOpen && (
        <div
          className="nodrag absolute right-1 top-7 z-10 flex gap-1 rounded border border-surface-border bg-surface-raised p-1 shadow-lg"
          onClick={(e) => e.stopPropagation()}
        >
          {PALETTE_ORDER.map((key) => {
            const p = COMMENT_COLORS[key];
            return (
              <button
                key={key}
                type="button"
                title={key}
                onClick={() => commitColor(key)}
                className="h-4 w-4 rounded-full border border-white/30 transition hover:scale-110"
                style={{ background: p.border }}
              />
            );
          })}
        </div>
      )}
      <textarea
        value={text}
        onChange={(e) => commitText(e.target.value)}
        spellCheck={false}
        placeholder="Note…"
        className="nodrag nowheel absolute inset-x-0 bottom-0 top-6 w-full resize-none border-0 bg-transparent px-2 py-1.5 text-[12px] leading-snug text-ink outline-none placeholder:text-ink-dim"
      />
    </div>
  );
}

export const CommentNode = memo(CommentNodeImpl);
